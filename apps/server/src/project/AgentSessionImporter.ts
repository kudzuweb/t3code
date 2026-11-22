import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  isRuntimeModeAbove,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionNotFoundError,
  AgentSessionUnreadableError,
  AgentSessionScanError,
  AgentSessionUnresumableSessionError,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderThreadId,
  ThreadId,
  TurnItemId,
  type AgentSessionImportInput,
  type AgentSessionImportOneInput,
  type AgentSessionImportSource,
  type AgentSessionImportOneResult,
  type AgentSessionImportResult,
  type AgentSessionSource,
  type AgentSessionListInput,
  type AgentSessionListResult,
  type AgentSessionSummary,
  type ProviderInstanceId,
  type RuntimeMode,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const isProjectionStoreThreadNotFoundError = Schema.is(
  ProjectionStore.ProjectionStoreThreadNotFoundError,
);

const encodeStringList = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const isUnresumableSessionError = Schema.is(AgentSessionUnresumableSessionError);

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' already contains non-imported activity.`;
  }
}

// ACP imports read the history while holding the session-mutation lock, and an agent can
// leave session/resume or initialize unanswered. Longer than session/load's own deadline.
const NATIVE_HISTORY_READ_TIMEOUT = "2 minutes";

/**
 * Whether a thread bound to a native session still lacks its native history.
 * Once a turn has run, the native session also holds that turn's messages, so
 * its history can no longer be imported as run-less items.
 */
export function isNativeHistoryPending(input: {
  readonly historyOrigin: OrchestrationV2AppThread["historyOrigin"];
  readonly hasRun: boolean;
}): boolean {
  return input.historyOrigin !== "v1_import" && !input.hasRun;
}

type ImportableThread = Extract<
  AgentSessionScanner.AgentSessionRecentThread,
  { _tag: "Importable" }
>;

// Pi resumes by session file path; its session UUID only names the thread.
function nativeThreadIdOf(source: {
  readonly provider: AgentSessionSource;
  readonly providerSessionId: string;
  readonly filePath: string;
}): string {
  return source.provider === "pi" ? source.filePath : source.providerSessionId;
}

interface SessionKey {
  readonly providerInstanceId: ProviderInstanceId;
  readonly nativeThreadId: string;
  readonly importThreadId: ThreadId;
}

interface SessionOwner {
  readonly threadId: ThreadId;
  readonly projectId: string;
}

function importThreadIdOf(providerInstanceId: ProviderInstanceId, providerSessionId: string) {
  return ThreadId.make(`import:${providerInstanceId}:${providerSessionId}`);
}

function sessionKeyOf(source: AgentSessionImportSource): SessionKey {
  return {
    providerInstanceId: source.providerInstanceId,
    nativeThreadId: nativeThreadIdOf(source),
    importThreadId: importThreadIdOf(source.providerInstanceId, source.providerSessionId),
  };
}

const sessionKeyId = (key: SessionKey) => `${key.providerInstanceId}\0${key.nativeThreadId}`;

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

type HistoryMessage = Omit<AgentSessionScanner.AgentSessionThreadMessage, "role"> & {
  readonly role: AgentSessionScanner.AgentSessionThreadMessage["role"] | "reasoning";
};

// A Codex rollout names a turn, not a message, so its items carry that turn's id.
const nativeRefDriver = (driver: ProviderDriverKind) =>
  driver === "claudeAgent" || driver === "codex" || driver === "pi" ? driver : null;

function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly nativeDriver: ProviderDriverKind | null;
  readonly index: number;
  readonly message: HistoryMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = input.index + 1;
  const suffix = String(input.index).padStart(6, "0");
  const messageId = MessageId.make(`${input.threadId}:${suffix}`);
  const turnItemId = TurnItemId.make(
    `${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`,
  );
  const at = dateTime(input.message.createdAt);
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    // The next transcript sync reads from the last imported item's native record.
    nativeItemRef:
      input.nativeDriver === null || input.message.nativeId === undefined
        ? null
        : {
            driver: input.nativeDriver,
            nativeId: input.message.nativeId,
            strength: "strong" as const,
          },
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItemEvent = (payload: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
    id: EventId.make(turnItemId),
    type: "turn-item.updated",
    threadId: input.threadId,
    occurredAt: at,
    payload,
  });
  if (input.message.role === "reasoning") {
    return [
      turnItemEvent({ ...common, type: "reasoning", text: input.message.text, streaming: false }),
    ];
  }
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${input.threadId}:${suffix}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    turnItemEvent(turnItem),
  ];
}

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
  const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const sql = yield* SqlClient.SqlClient;
  const ceiling = settingsService.managedPolicy?.limits?.maxRuntimeMode;
  const loadProject = Effect.fn("AgentSessionImporter.loadProject")(function* (
    projectId: ProjectId,
  ) {
    return yield* projects.getById(projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );
  });

  // Imports run in full access, except under a policy ceiling: then in the project's default,
  // which the policy keeps under it, so imported threads never sit above the ceiling.
  const importRuntimeMode = Effect.fn("AgentSessionImporter.importRuntimeMode")(function* (
    projectId: ProjectId,
    project: Effect.Success<ReturnType<typeof loadProject>>,
  ) {
    if (ceiling === undefined) return DEFAULT_RUNTIME_MODE;
    return yield* settingsService.getSettings.pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-settings", cause })),
      Effect.map((settings): RuntimeMode => {
        const mode = resolveProjectSettings(settings, projectId, project).settings
          .defaultRuntimeMode;
        return isRuntimeModeAbove(mode, ceiling) ? ceiling : mode;
      }),
    );
  });

  // Resolves to "imported" for a new thread, "recorded" when the thread already held exactly the
  // transcript's messages, "stale" when it holds different ones, and "owned" when another thread
  // owns the native session.
  const importThread = Effect.fn("AgentSessionImporter.importThread")(function* (input: {
    readonly outcome: ImportableThread;
    readonly projectId: ProjectId;
    readonly workspaceRoot: string;
    readonly runtimeMode: RuntimeMode;
    // Set when the deterministic ids belong to a deleted thread.
    readonly ids?: { readonly threadId: ThreadId; readonly providerThreadId: ProviderThreadId };
  }) {
    const { outcome, projectId, workspaceRoot, runtimeMode } = input;
    const source = outcome.source;
    const threadId =
      input.ids?.threadId ?? importThreadIdOf(source.providerInstanceId, source.providerSessionId);
    const thread = outcome.thread;
    if (
      thread.source === "claudeAgent" &&
      !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
    ) {
      return yield* new AgentSessionUnresumableSessionError({
        source: thread.source,
        providerSessionId: thread.providerSessionId,
      });
    }
    const driver = ProviderDriverKind.make(thread.source);
    const nativeThreadId = nativeThreadIdOf(source);
    const persistRuntime = runtimes.upsert(
      {
        threadId,
        providerName: driver,
        providerInstanceId: thread.providerInstanceId,
        adapterKey: driver,
        runtimeMode,
        status: "stopped",
        lastSeenAt: thread.updatedAt,
        resumeCursor:
          thread.source === "claudeAgent"
            ? { threadId, resume: thread.providerSessionId }
            : { threadId: nativeThreadId },
        runtimePayload: { cwd: outcome.worktree?.path ?? workspaceRoot },
      },
      { onConflict: "ignore" },
    );
    const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, ["messages"]));
    if (Option.isSome(existing)) {
      if (existing.value.thread.projectId !== projectId) {
        return yield* new AgentSessionThreadProjectConflictError({
          threadId,
          expectedProjectId: projectId,
          actualProjectId: existing.value.thread.projectId,
        });
      }
      if (existing.value.thread.historyOrigin !== "v1_import") {
        return yield* new AgentSessionThreadModifiedError({ threadId });
      }
      // A transcript can change after the import, so record it only when the
      // thread already holds exactly its messages.
      const stored = existing.value.messages;
      if (
        stored.length !== thread.messages.length ||
        stored.some(
          (message, index) =>
            message.role !== thread.messages[index]?.role ||
            message.text !== thread.messages[index]?.text,
        )
      ) {
        return "stale" as const;
      }
      yield* persistRuntime;
      yield* runtimes.recordImportedTranscript({ threadId, source });
      return "recorded" as const;
    }

    const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL;
    const providerThreadId =
      input.ids?.providerThreadId ??
      idAllocator.derive.providerThread({
        driver,
        providerInstanceId: thread.providerInstanceId,
        nativeThreadId,
      });
    const createdAt = dateTime(thread.createdAt);
    const updatedAt = dateTime(thread.updatedAt);
    const appThread: OrchestrationV2AppThread = {
      createdBy: "system",
      creationSource: "server",
      id: threadId,
      projectId: projectId,
      title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
      providerInstanceId: thread.providerInstanceId,
      modelSelection: { instanceId: thread.providerInstanceId, model },
      runtimeMode,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: outcome.worktree?.branch ?? null,
      worktreePath: outcome.worktree?.path ?? null,
      linkedPullRequest: null,
      branchPullRequest: null,
      activeProviderThreadId: providerThreadId,
      historyOrigin: "v1_import",
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt,
      updatedAt,
      archivedAt: null,
      settledOverride: "settled",
      settledAt: updatedAt,
      unsettledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      pinnedAt: null,
      pinOrderKey: null,
      activeOrderKey: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      driver,
      providerInstanceId: thread.providerInstanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: {
        driver,
        nativeId: nativeThreadId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      createdAt,
      updatedAt,
    };

    const committed = yield* eventSink.write({
      unlessNativeThreadOwned: providerThread,
      events: [
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
          type: "thread.created",
          threadId,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: createdAt,
          payload: appThread,
        },
        ...(thread.omittedMessageCount > 0
          ? [
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`),
                type: "turn-item.updated" as const,
                threadId,
                occurredAt: createdAt,
                payload: {
                  id: TurnItemId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`),
                  threadId,
                  runId: null,
                  nodeId: null,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 0,
                  status: "completed" as const,
                  title: null,
                  startedAt: createdAt,
                  completedAt: createdAt,
                  updatedAt: createdAt,
                  type: "system_notice" as const,
                  message: `${thread.omittedMessageCount} earlier ${thread.omittedMessageCount === 1 ? "message was" : "messages were"} not imported. This thread keeps the first user message and the most recent history. The original session transcript is unchanged.`,
                },
              },
            ]
          : []),
        ...thread.messages.flatMap((message, index) =>
          messageEvents({ threadId, index, message, nativeDriver: nativeRefDriver(driver) }),
        ),
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${providerThreadId}`),
          type: "provider-thread.updated",
          threadId,
          driver,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: updatedAt,
          payload: providerThread,
        },
      ],
    });
    if (committed.length === 0) return "owned" as const;
    yield* persistRuntime;
    yield* runtimes.recordImportedTranscript({ threadId, source });
    return "imported" as const;
  });

  // The bulk import leaves a session alone when any thread, deleted or not, holds it.
  const ownedByAnotherThread = Effect.fn("AgentSessionImporter.ownedByAnotherThread")(function* (
    source: AgentSessionImportSource,
    threadId: ThreadId,
  ) {
    const owners = yield* sql`
      SELECT provider_thread_id
      FROM orchestration_v2_projection_provider_threads
      WHERE driver = ${source.provider}
        AND provider_instance_id = ${source.providerInstanceId}
        AND thread_id IS NOT ${threadId}
        AND json_extract(payload_json, '$.nativeThreadRef.nativeId') = ${nativeThreadIdOf(source)}
      LIMIT 1
    `.pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-owners", cause })),
    );
    return owners.length > 0;
  });

  const importRecentAgentThreads = Effect.fn("importRecentAgentThreadsV2")(function* (
    input: AgentSessionImportInput,
  ) {
    const project = yield* loadProject(input.projectId);
    if (
      input.expectedWorkspaceRoot !== undefined &&
      normalizeProjectPathForComparison(project.workspaceRoot) !==
        normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
    ) {
      return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
    }
    const runtimeMode = yield* importRuntimeMode(input.projectId, project);
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    // The thread's project owns its transcripts: a linked worktree import records the worktree as cwd.
    const completedSources = (yield* Effect.forEach(runtimeRows, (runtime) => {
      const imported = AgentSessionScanner.decodeImportedTranscriptPayload(
        runtime.runtimePayload,
      ).pipe(Option.flatMapNullishOr((payload) => payload.importedTranscripts));
      if (Option.isNone(imported) || imported.value.length === 0) return Effect.succeed([]);
      return orchestrator.getThreadRecords(runtime.threadId, []).pipe(
        Effect.map(({ thread }) => (thread.projectId === input.projectId ? imported.value : [])),
        Effect.catch((cause) =>
          isProjectionStoreThreadNotFoundError(cause.cause)
            ? Effect.succeed([])
            : Effect.fail(new AgentSessionScanError({ operation: "read-projects", cause })),
        ),
      );
    })).flat();
    const outcomes = scanner.recentThreads(project.workspaceRoot, completedSources);
    const importedThreadIds = new Set<ThreadId>();
    let importedCount = 0;
    let skippedCount = 0;

    yield* Stream.runForEach(outcomes, (outcome) =>
      Effect.gen(function* () {
        if (outcome._tag === "Skipped") {
          skippedCount += 1;
          return;
        }
        const source = outcome.source;
        const threadId = ThreadId.make(
          `import:${source.providerInstanceId}:${source.providerSessionId}`,
        );
        if (outcome._tag === "AlreadyImported") {
          importedThreadIds.add(threadId);
          return;
        }
        if (outcome._tag === "Duplicate") {
          if (importedThreadIds.has(threadId)) {
            yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.ignore);
          }
          return;
        }

        const imported = yield* Effect.gen(function* () {
          if (yield* ownedByAnotherThread(outcome.source, threadId)) return "owned" as const;
          return yield* importThread({
            outcome,
            projectId: input.projectId,
            workspaceRoot: project.workspaceRoot,
            runtimeMode,
          });
        }).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Could not import an agent session", {
              provider: outcome.thread.source,
              sessionId: outcome.thread.providerSessionId,
              cause,
            }).pipe(Effect.as("stale" as const)),
          ),
        );
        if (imported === "imported") {
          importedThreadIds.add(threadId);
          importedCount += 1;
        } else if (imported === "recorded") {
          importedThreadIds.add(threadId);
        } else if (imported === "stale") {
          skippedCount += 1;
        }
      }),
    );

    return { importedCount, skippedCount } satisfies AgentSessionImportResult;
  });

  // The thread holding a session is found by the native id its provider thread owns, the way
  // the event sink's ownership guard sees it, or by the id a bulk import gives it.
  const findOwners = Effect.fn("AgentSessionImporter.findOwners")(function* (
    projectId: ProjectId,
    keys: ReadonlyArray<SessionKey>,
  ) {
    const owners = new Map<string, SessionOwner>();
    if (keys.length === 0) return owners;
    const rows = yield* sql<{
      readonly thread_id: string;
      readonly project_id: string;
      readonly provider_instance_id: string | null;
      readonly native_id: string | null;
    }>`
      SELECT thread.thread_id AS thread_id,
             thread.project_id AS project_id,
             provider.provider_instance_id AS provider_instance_id,
             json_extract(provider.payload_json, '$.nativeThreadRef.nativeId') AS native_id
      FROM orchestration_v2_projection_threads AS thread
      LEFT JOIN orchestration_v2_projection_provider_threads AS provider
        ON provider.thread_id = thread.thread_id
      WHERE thread.deleted_at IS NULL
        AND (
          thread.thread_id IN (
            SELECT value FROM json_each(${encodeStringList(keys.map((key) => key.importThreadId))})
          )
          OR json_extract(provider.payload_json, '$.nativeThreadRef.nativeId') IN (
            SELECT value FROM json_each(${encodeStringList(keys.map((key) => key.nativeThreadId))})
          )
        )
      ORDER BY thread.updated_at DESC
    `.pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-owners", cause })),
    );
    for (const key of keys) {
      const holders = rows.filter(
        (row) =>
          row.thread_id === key.importThreadId ||
          (row.provider_instance_id === key.providerInstanceId &&
            row.native_id === key.nativeThreadId),
      );
      const holder = holders.find((row) => row.project_id === projectId) ?? holders[0];
      if (holder !== undefined) {
        owners.set(sessionKeyId(key), {
          threadId: ThreadId.make(holder.thread_id),
          projectId: holder.project_id,
        });
      }
    }
    return owners;
  });

  // A deleted thread keeps its rows, so a session whose only thread was deleted gets new ids.
  const importIds = Effect.fn("AgentSessionImporter.importIds")(function* (
    projectId: ProjectId,
    found: ImportableThread,
  ) {
    const { source, thread } = found;
    const threadId = importThreadIdOf(source.providerInstanceId, source.providerSessionId);
    const providerThreadId = idAllocator.derive.providerThread({
      driver: ProviderDriverKind.make(thread.source),
      providerInstanceId: thread.providerInstanceId,
      nativeThreadId: nativeThreadIdOf(source),
    });
    const taken = yield* sql<{ readonly taken: number }>`
      SELECT 1 AS taken
      WHERE EXISTS (
          SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
        )
        OR EXISTS (
          SELECT 1 FROM orchestration_v2_projection_provider_threads
          WHERE provider_thread_id = ${providerThreadId}
        )
    `.pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-owners", cause })),
    );
    if (taken.length === 0) return { threadId, providerThreadId };
    const freshThreadId = yield* idAllocator.allocate
      .thread({ projectId })
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "import-session", cause }),
        ),
      );
    return {
      threadId: freshThreadId,
      providerThreadId: ProviderThreadId.make(`${providerThreadId}:reimport:${freshThreadId}`),
    };
  });

  const listSessions = Effect.fn("AgentSessionImporter.listSessions")(function* (
    input: AgentSessionListInput,
  ) {
    const project = yield* loadProject(input.projectId);
    const listing = yield* scanner.listProjectSessions(project.workspaceRoot);
    const owners = yield* findOwners(
      input.projectId,
      listing.sessions.map(({ source }) => sessionKeyOf(source)),
    );
    const sessions = listing.sessions.map(
      ({ source, title, lastActiveAt, worktree }): AgentSessionSummary => {
        const owner = owners.get(sessionKeyId(sessionKeyOf(source)));
        return {
          provider: source.provider,
          providerInstanceId: source.providerInstanceId,
          providerSessionId: source.providerSessionId,
          title,
          lastActiveAt,
          worktree,
          threadId: owner?.threadId ?? null,
        };
      },
    );
    return { sessions, truncated: listing.truncated } satisfies AgentSessionListResult;
  });

  /**
   * Opens a session as a thread. A thread that already holds the session is returned as it
   * is, even when the transcript has grown since, because T3 may have continued that thread.
   */
  const importSession = Effect.fn("AgentSessionImporter.importSession")(function* (
    input: AgentSessionImportOneInput,
  ) {
    const project = yield* loadProject(input.projectId);
    const held = Effect.fn("AgentSessionImporter.importSession.held")(function* (
      nativeThreadId: string,
      // Before the scan confirms the session belongs to this project, only a thread in it counts.
      scope: "project" | "any",
    ) {
      const key: SessionKey = {
        providerInstanceId: input.providerInstanceId,
        nativeThreadId,
        importThreadId: importThreadIdOf(input.providerInstanceId, input.providerSessionId),
      };
      const owner = (yield* findOwners(input.projectId, [key])).get(sessionKeyId(key));
      if (owner === undefined) return null;
      if (scope === "project" && owner.projectId !== input.projectId) return null;
      return { threadId: owner.threadId, imported: false } satisfies AgentSessionImportOneResult;
    });
    const alreadyHeld = yield* held(input.providerSessionId, "project");
    if (alreadyHeld !== null) return alreadyHeld;

    const found = yield* scanner.findProjectSession({
      workspaceRoot: project.workspaceRoot,
      providerInstanceId: input.providerInstanceId,
      providerSessionId: input.providerSessionId,
    });
    if (found._tag !== "Importable") {
      return yield* found._tag === "Skipped" && found.filePath !== undefined
        ? new AgentSessionUnreadableError({
            projectId: input.projectId,
            providerSessionId: input.providerSessionId,
            filePath: found.filePath,
          })
        : new AgentSessionNotFoundError({
            projectId: input.projectId,
            providerSessionId: input.providerSessionId,
          });
    }
    const nativeThreadId = nativeThreadIdOf(found.source);
    const heldAfterScan =
      (yield* held(input.providerSessionId, "any")) ??
      (nativeThreadId === input.providerSessionId ? null : yield* held(nativeThreadId, "any"));
    if (heldAfterScan !== null) return heldAfterScan;

    const ids = yield* importIds(input.projectId, found);
    const settled = yield* importThread({
      outcome: found,
      projectId: input.projectId,
      workspaceRoot: project.workspaceRoot,
      runtimeMode: yield* importRuntimeMode(input.projectId, project),
      ids,
    }).pipe(
      Effect.map((outcome) =>
        outcome === "imported"
          ? ({
              threadId: ids.threadId,
              imported: true,
            } satisfies AgentSessionImportOneResult)
          : new AgentSessionUnresumableSessionError({
              source: found.source.provider,
              providerSessionId: found.source.providerSessionId,
            }),
      ),
      Effect.catch((cause) =>
        Effect.succeed(
          isUnresumableSessionError(cause)
            ? cause
            : new AgentSessionScanError({ operation: "import-session", cause }),
        ),
      ),
    );
    if ("imported" in settled) return settled;
    // Another import can claim the session between the first check and the write.
    return (yield* held(nativeThreadId, "any")) ?? (yield* settled);
  });

  /**
   * Loading a native session replays its history only into the adapter's own
   * snapshot, so copy that snapshot into the thread as run-less history. The
   * `v1_import` origin keeps it in forks and marks the import done, so a call
   * after a failed read repairs the thread and any later call does nothing.
   */
  const importNativeHistory = Effect.fn("importNativeHistoryV2")(function* (input: {
    readonly threadId: ThreadId;
    readonly workspaceRoot: string;
  }) {
    const pendingThread = Effect.map(
      orchestrator.getThreadRecords(input.threadId, ["providerThreads", "runs"]),
      (records) =>
        isNativeHistoryPending({
          historyOrigin: records.thread.historyOrigin,
          hasRun: records.runs.length > 0,
        })
          ? records
          : null,
    );
    // Thread commands write whole thread rows, so hold their lock from the
    // re-read through the write to keep both this marker and their changes.
    const commit = (messages: ReadonlyArray<HistoryMessage>) =>
      threadCommands.withLock(
        input.threadId,
        Effect.gen(function* () {
          const current = yield* pendingThread;
          if (current === null) return 0;
          yield* eventSink.write({
            events: [
              ...messages.flatMap((message, index) =>
                messageEvents({ threadId: input.threadId, index, message, nativeDriver: null }),
              ),
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${input.threadId}:native-history`),
                type: "thread.metadata-updated",
                threadId: input.threadId,
                providerInstanceId: current.thread.providerInstanceId,
                occurredAt: yield* DateTime.now,
                payload: { ...current.thread, historyOrigin: "v1_import" },
              },
            ],
          });
          return messages.length;
        }),
      );
    const records = yield* pendingThread;
    if (records === null) return 0;
    const { thread, providerThreads } = records;
    const providerThread = providerThreads.find(
      (candidate) => candidate.appThreadId === input.threadId && candidate.nativeThreadRef !== null,
    );
    const nativeThreadId = providerThread?.nativeThreadRef?.nativeId;
    if (providerThread === undefined || nativeThreadId == null) return 0;
    const providerSessionId = yield* idAllocator.allocate.providerSession({
      providerInstanceId: providerThread.providerInstanceId,
      threadId: input.threadId,
    });
    // The registry admits this open against sign-in changes, like any other open.
    const adapter = yield* registry.get(thread.modelSelection.instanceId);
    const snapshot = yield* adapter
      .openSession({
        threadId: input.threadId,
        providerSessionId,
        modelSelection: thread.modelSelection,
        runtimePolicy: {
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
          cwd: thread.worktreePath ?? input.workspaceRoot,
        },
        initialNativeThreadId: nativeThreadId,
        ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
          ? {}
          : {
              initialProviderItemIdentityVersion: providerThread.nativeMetadata.itemIdentityVersion,
            }),
      })
      .pipe(
        Effect.flatMap((session) => session.readThreadSnapshot({ providerThread })),
        Effect.timeout(NATIVE_HISTORY_READ_TIMEOUT),
        Effect.scoped,
      );
    const reasoningIds = new Set(snapshot.reasoningMessageIds);
    const messages = snapshot.messages.flatMap((message) =>
      (message.role === "user" || message.role === "assistant") && message.text.trim() !== ""
        ? [
            {
              role: reasoningIds.has(message.id) ? ("reasoning" as const) : message.role,
              text: message.text,
              createdAt: DateTime.formatIso(message.createdAt),
            },
          ]
        : [],
    );
    // Loading the session can take a while, and the thread is usable meanwhile.
    return yield* commit(messages);
  });

  return { importRecentAgentThreads, importNativeHistory, listSessions, importSession };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make);
