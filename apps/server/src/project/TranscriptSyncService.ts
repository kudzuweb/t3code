import {
  CommandId,
  OrchestrationV2SyncTranscriptTurnsError,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  type ProviderThreadId,
  type ThreadId,
} from "@t3tools/contracts";
import { PI_PROVIDER } from "@t3tools/provider-pi/server";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { CLAUDE_PROVIDER, claudeTurnCursor } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CODEX_DRIVER_KIND } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import type { ProjectionRecords } from "../orchestration-v2/ProjectionStore.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";
import { codexTurnCursor } from "./CodexTurns.ts";

/**
 * Brings a thread up to date with turns made in its native session outside T3. A turn T3 cannot
 * place with certainty is left for a later read: a wrong guess would duplicate or misplace
 * history, which is worse than a missing turn.
 */
export class TranscriptSyncService extends Context.Service<
  TranscriptSyncService,
  {
    /** Records the turns the thread's session gained after what T3 has shown; returns how many. */
    readonly sync: (
      threadId: ThreadId,
    ) => Effect.Effect<number, OrchestrationV2SyncTranscriptTurnsError>;
    /** Starts a sync and returns at once; a failure is logged, never raised. */
    readonly syncInBackground: (threadId: ThreadId) => Effect.Effect<void>;
  }
>()("t3/project/TranscriptSyncService") {}

type TranscriptRead = Effect.Success<
  ReturnType<AgentSessionScanner.AgentSessionScanner["Service"]["readTurnsAfter"]>
>;

const providerTurnEnded = (turn: OrchestrationV2ProviderTurn) =>
  turn.status === "completed" ||
  turn.status === "interrupted" ||
  turn.status === "failed" ||
  turn.status === "cancelled";

/**
 * Where an ended T3 turn left its provider's session, or null when that cannot be told. Claude
 * and Codex name it in the turn. Pi's turn names only the entry it started at, and the end of the
 * turn is the provider thread's head, which the same write set when it captured the turn's entries.
 */
const endedTurnCursor = (
  providerThread: OrchestrationV2ProviderThread,
  turn: OrchestrationV2ProviderTurn,
): string | null => {
  if (!providerTurnEnded(turn)) return null;
  if (providerThread.driver === CLAUDE_PROVIDER) return claudeTurnCursor(turn);
  if (providerThread.driver === CODEX_DRIVER_KIND) return codexTurnCursor(turn);
  if (providerThread.driver === PI_PROVIDER) {
    return turn.nativeTurnRef?.strength === "strong"
      ? (providerThread.nativeConversationHeadRef?.nativeId ?? null)
      : null;
  }
  return null;
};

const isTerminal = (run: OrchestrationV2Run) =>
  run.status !== "preparing" &&
  run.status !== "queued" &&
  run.status !== "starting" &&
  run.status !== "running" &&
  run.status !== "waiting";

const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const projects = yield* ProjectService.ProjectService;
  const locks = yield* KeyedLock.make<ThreadId>();
  // What the last read of each provider thread's transcript saw, so an unchanged file is not read again.
  const lastRead = new Map<
    ProviderThreadId,
    { readonly anchor: string; readonly signature: AgentSessionScanner.TranscriptSignature }
  >();

  const newestItemNativeId = (items: ReadonlyArray<OrchestrationV2TurnItem>): string | undefined =>
    items.findLast((item) => item.nativeItemRef?.nativeId != null)?.nativeItemRef?.nativeId ??
    undefined;

  const leftOutput = Effect.fn("TranscriptSyncService.leftOutput")(function* (
    threadId: ThreadId,
    run: OrchestrationV2Run,
  ) {
    const { turnItems } = yield* orchestrator.getThreadRecords(threadId, ["turnItems"], {
      turnItemRunIds: [run.id],
    });
    return turnItems.some((item) => item.type === "assistant_message" && item.text.trim() !== "");
  });

  /**
   * Where the transcript was last shown: the newest run that reached this provider thread decides.
   * Runs that never started wrote nothing to the transcript, so they are passed over. Null means
   * the transcript cannot be placed, so nothing is read.
   */
  const anchorFor = Effect.fn("TranscriptSyncService.anchorFor")(function* (
    threadId: ThreadId,
    records: ProjectionRecords<"runs" | "attempts" | "providerThreads" | "providerTurns">,
    providerThread: OrchestrationV2ProviderThread,
  ) {
    // A switch of provider instance continues the same native session on a new provider thread.
    const sessionThreadIds = new Set(
      records.providerThreads
        .filter(
          (candidate) =>
            candidate.driver === providerThread.driver &&
            candidate.nativeThreadRef?.nativeId === providerThread.nativeThreadRef?.nativeId,
        )
        .map((candidate) => candidate.id),
    );
    const newestFirst = records.runs.toSorted((left, right) => right.ordinal - left.ordinal);
    for (const run of newestFirst) {
      // A run a rollback removed no longer shows where the session stands.
      if (run.status === "rolled_back") continue;
      if (run.providerThreadId === null || !sessionThreadIds.has(run.providerThreadId)) continue;
      if (run.recordedFrom === "transcript") {
        const { turnItems } = yield* orchestrator.getThreadRecords(threadId, ["turnItems"], {
          turnItemRunIds: [run.id],
        });
        return newestItemNativeId(turnItems) ?? null;
      }
      const providerTurn = Orchestrator.providerTurnForRun(records, run);
      if (providerTurn === undefined) continue;
      const cursor = endedTurnCursor(providerThread, providerTurn);
      if (cursor !== null) return cursor;
      // A turn with no native ref that wrote nothing never reached the provider's session, as
      // with a Codex goal control turn, so the anchor stays where the turn before it left it. One
      // that wrote something may sit beside output of its own, so nothing after it can be told apart.
      if (
        providerTurn.nativeTurnRef === null &&
        providerTurnEnded(providerTurn) &&
        !(yield* leftOutput(threadId, run))
      ) {
        continue;
      }
      return null;
    }
    const { turnItems } = yield* orchestrator.getThreadRecords(threadId, ["turnItems"], {
      turnItemRunIds: [null],
    });
    return newestItemNativeId(turnItems) ?? null;
  });

  const syncOnce = Effect.fn("TranscriptSyncService.sync")(function* (threadId: ThreadId) {
    yield* Effect.annotateCurrentSpan({ "orchestration_v2.thread_id": threadId });
    const records = yield* orchestrator.getThreadRecords(threadId, [
      "runs",
      "attempts",
      "providerThreads",
      "providerTurns",
    ]);
    const { thread, runs } = records;
    if (thread.deletedAt !== null || !runs.every(isTerminal)) return 0;
    const providerThread = records.providerThreads.find(
      (candidate) => candidate.id === thread.activeProviderThreadId,
    );
    const nativeThread = providerThread?.nativeThreadRef;
    if (
      providerThread === undefined ||
      nativeThread == null ||
      nativeThread.strength !== "strong"
    ) {
      return 0;
    }
    // A rollback resumes the next T3 turn at its head, which leaves out what the terminal added.
    // Pi keeps its head there after every turn, so only the other providers read it as a rollback.
    if (
      providerThread.driver !== PI_PROVIDER &&
      providerThread.nativeConversationHeadRef !== null
    ) {
      return 0;
    }

    const anchorNativeId = yield* anchorFor(threadId, records, providerThread);
    if (anchorNativeId === null) {
      yield* Effect.logDebug("Transcript sync has no anchor", { threadId });
      return 0;
    }

    const previous = lastRead.get(providerThread.id);
    // A Codex rollout is found through the directory the thread works in.
    const workspaceRoot =
      providerThread.driver === CODEX_DRIVER_KIND
        ? (thread.worktreePath ??
          Option.getOrUndefined(yield* projects.getById(thread.projectId))?.workspaceRoot)
        : undefined;
    const read: TranscriptRead = yield* scanner.readTurnsAfter({
      driver: providerThread.driver,
      providerInstanceId: providerThread.providerInstanceId,
      providerSessionId: nativeThread.nativeId,
      anchorNativeId,
      ...(previous?.anchor === anchorNativeId ? { unchangedSince: previous.signature } : {}),
      ...(workspaceRoot === undefined ? {} : { workspaceRoot }),
    });
    if (read._tag === "Unchanged") return 0;
    // Only a read that leaves nothing to retry is remembered; a refused command reads again.
    const remember = (anchor: string) => {
      if (read.signature !== undefined) {
        lastRead.set(providerThread.id, { anchor, signature: read.signature });
      }
    };
    if (read._tag === "AnchorNotFound") {
      remember(anchorNativeId);
      yield* Effect.logWarning("Transcript sync could not find its anchor in the session", {
        threadId,
        anchorNativeId,
      });
      return 0;
    }
    if (read._tag !== "Turns") return 0;
    if (read.turns.length === 0) {
      remember(anchorNativeId);
      return 0;
    }

    const latestRunOrdinal = runs.reduce((max, run) => Math.max(max, run.ordinal), 0);
    const recorded = yield* orchestrator
      .dispatch({
        type: "transcript_turns.record",
        // The expected ordinal is part of the id because a refused command may be sent again.
        commandId: CommandId.make(
          `transcript-sync:${providerThread.id}:${read.throughNativeId}:${latestRunOrdinal}`,
        ),
        threadId,
        providerThreadId: providerThread.id,
        expectedLatestRunOrdinal: latestRunOrdinal,
        turns: read.turns,
      })
      .pipe(
        Effect.as(read.turns.length),
        // The thread changed since it was read; the next trigger reads it again.
        Effect.catchTags({ OrchestratorCommandRejectedError: () => Effect.succeed(0) }),
      );
    if (recorded > 0) remember(read.throughNativeId);
    return recorded;
  });

  // A trigger during a sync waits for it and then finds the transcript unchanged.
  const sync: TranscriptSyncService["Service"]["sync"] = (threadId) =>
    locks
      .withLock(threadId, syncOnce(threadId))
      .pipe(
        Effect.mapError(
          (cause) => new OrchestrationV2SyncTranscriptTurnsError({ threadId, cause }),
        ),
      );

  const syncInBackground: TranscriptSyncService["Service"]["syncInBackground"] = (threadId) =>
    sync(threadId).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Transcript sync failed", { threadId, cause: error.cause }),
      ),
      Effect.asVoid,
      Effect.forkDetach({ startImmediately: true }),
      Effect.asVoid,
    );

  return TranscriptSyncService.of({ sync, syncInBackground });
});

export const layer = Layer.effect(TranscriptSyncService, make);
