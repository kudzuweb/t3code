import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2TranscriptTurn,
} from "@t3tools/contracts";
import { PI_PROVIDER } from "@t3tools/provider-pi/server";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as TranscriptSyncService from "./TranscriptSyncService.ts";

const instanceId = ProviderInstanceId.make("claudeAgent");
const driver = ProviderDriverKind.make("claudeAgent");
const modelSelection = { instanceId, model: "claude-haiku-5-5" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("A transcript sync needs no provider process"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerStores = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "transcript-sync" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

const threadId = ThreadId.make("thread:sync");
const providerThreadId = ProviderThreadId.make("provider-thread:sync");
const earlierProviderThreadId = ProviderThreadId.make("provider-thread:earlier");

const terminalTurn: OrchestrationV2TranscriptTurn = {
  userNativeId: "u-terminal",
  userText: "From the terminal",
  userAt: "2026-11-20T10:01:00.000Z",
  assistantNativeId: "a-terminal",
  assistantText: "Answered in the terminal",
  assistantAt: "2026-11-20T10:01:30.000Z",
};

type SeedRun = {
  readonly status: "completed" | "running" | "failed" | "cancelled" | "rolled_back";
  /**
   * What the run left in the provider: its native turn id, `"no-native-id"` for a turn without
   * one, or `"never-started"` for a run that never reached the provider.
   */
  readonly turn: string;
  /** How the adapter left the provider turn; it ends as completed, but a late write can undo that. */
  readonly turnStatus?: "running" | "completed" | "failed";
  /** The adapter writes its `turn:<attempt>` placeholder strong while a turn runs, weak otherwise. */
  readonly turnStrength?: "strong" | "weak";
  /** A provider thread other than the active one, which holds the same native session. */
  readonly onEarlierProviderThread?: true;
  /** The run left an assistant message in the thread. */
  readonly output?: true;
};

const seed = Effect.fn("TranscriptSyncService.test.seed")(function* (input: {
  readonly runs: ReadonlyArray<SeedRun>;
  /** Native ids of imported history items, which have no run. */
  readonly importedItems?: ReadonlyArray<string>;
  readonly nativeConversationHeadRef?: boolean;
  /** The driver of the native session; Claude unless the test says otherwise. */
  readonly provider?: "claudeAgent" | "codex" | "pi";
  /** The native record a provider that keeps its position there, such as Pi, left after its last turn. */
  readonly head?: string;
}) {
  const kind = ProviderDriverKind.make(input.provider ?? "claudeAgent");
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create-sync-thread"),
    threadId,
    projectId: ProjectId.make("project:sync"),
    title: "Sync",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  // The thread's active provider thread is the one written last.
  if (input.runs.some((run) => run.onEarlierProviderThread === true)) {
    yield* projections.apply({
      id: EventId.make("sync:earlier-provider-thread"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: { ...providerThreadAt(now, false, kind), id: earlierProviderThreadId },
    });
  }
  yield* projections.apply({
    id: EventId.make("sync:provider-thread"),
    type: "provider-thread.updated",
    threadId,
    occurredAt: now,
    payload: providerThreadAt(now, input.nativeConversationHeadRef === true, kind, input.head),
  });
  for (const [index, nativeId] of (input.importedItems ?? []).entries()) {
    yield* projections.apply({
      id: EventId.make(`sync:imported:${index}`),
      type: "turn-item.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: TurnItemId.make(`imported:${index}`),
        threadId,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: { driver: kind, nativeId, strength: "strong" },
        parentItemId: null,
        ordinal: index + 1,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "assistant_message",
        messageId: MessageId.make(`imported-message:${index}`),
        text: "imported",
        streaming: false,
      },
    });
  }
  for (const [index, run] of input.runs.entries()) {
    const ordinal = index + 1;
    const attemptId = RunAttemptId.make(`attempt:${ordinal}`);
    const reachedProvider = run.turn !== "never-started";
    yield* projections.apply({
      id: EventId.make(`sync:run:${ordinal}`),
      type: "run.created",
      threadId,
      occurredAt: now,
      payload: {
        id: RunId.make(`run:${ordinal}`),
        threadId,
        ordinal,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId:
          run.onEarlierProviderThread === true ? earlierProviderThreadId : providerThreadId,
        userMessageId: MessageId.make(`message:${ordinal}`),
        rootNodeId: null,
        activeAttemptId: reachedProvider ? attemptId : null,
        status: run.status,
        requestedAt: now,
        startedAt: reachedProvider ? now : null,
        completedAt: run.status === "running" ? null : now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    if (reachedProvider) {
      yield* projections.apply({
        id: EventId.make(`sync:turn:${ordinal}`),
        type: "provider-turn.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: ProviderTurnId.make(`provider-turn:${ordinal}`),
          providerThreadId:
            run.onEarlierProviderThread === true ? earlierProviderThreadId : providerThreadId,
          nodeId: NodeId.make(`node:${ordinal}`),
          runAttemptId: attemptId,
          nativeTurnRef:
            run.turn === "no-native-id"
              ? null
              : { driver: kind, nativeId: run.turn, strength: run.turnStrength ?? "weak" },
          ordinal,
          status: run.turnStatus ?? "completed",
          startedAt: now,
          completedAt: run.turnStatus === "running" ? null : now,
        },
      });
    }
    if (run.output === true) {
      yield* projections.apply({
        id: EventId.make(`sync:output:${ordinal}`),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: TurnItemId.make(`output:${ordinal}`),
          threadId,
          runId: RunId.make(`run:${ordinal}`),
          nodeId: null,
          providerThreadId,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 0,
          status: "completed",
          title: null,
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          type: "assistant_message",
          messageId: MessageId.make(`output-message:${ordinal}`),
          text: "It wrote something before it lost its place.",
          streaming: false,
        },
      });
    }
  }
  return projections;
});

const providerThreadAt = (
  now: DateTime.Utc,
  pinnedHead: boolean,
  kind: ProviderDriverKind = driver,
  head?: string,
) => ({
  id: providerThreadId,
  driver: kind,
  providerInstanceId: instanceId,
  providerSessionId: null,
  appThreadId: threadId,
  ownerNodeId: null,
  nativeThreadRef: { driver: kind, nativeId: "native-session", strength: "strong" as const },
  nativeConversationHeadRef:
    head !== undefined
      ? { driver: kind, nativeId: head, strength: "strong" as const }
      : pinnedHead
        ? { driver: kind, nativeId: "rolled-back-head", strength: "weak" as const }
        : null,
  status: "idle" as const,
  firstRunOrdinal: null,
  lastRunOrdinal: null,
  handoffIds: [],
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
});

const signature = { size: 100, mtimeMs: 5 };

/** A sync whose reader answers with `reads[anchor]` and remembers what it was asked. */
const makeService = Effect.fn("TranscriptSyncService.test.makeService")(function* (
  reads: Record<string, AgentSessionScanner.TurnsAfterAnchor>,
) {
  const asked: Array<{
    readonly anchor: string;
    readonly unchangedSince: AgentSessionScanner.TranscriptSignature | undefined;
  }> = [];
  const polls: Array<boolean | undefined> = [];
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const scanner = Layer.mock(AgentSessionScanner.AgentSessionScanner)({
    readTurnsAfter: ({ anchorNativeId, unchangedSince, poll }) =>
      Effect.sync(() => {
        asked.push({ anchor: anchorNativeId, unchangedSince });
        polls.push(poll);
        return unchangedSince !== undefined
          ? ({ _tag: "Unchanged" } as const)
          : { ...(reads[anchorNativeId] ?? { _tag: "AnchorNotFound" as const }), signature };
      }),
  });
  const service = yield* TranscriptSyncService.TranscriptSyncService.pipe(
    Effect.provide(
      TranscriptSyncService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(scanner, Layer.succeed(Orchestrator.OrchestratorV2, orchestrator)),
        ),
      ),
    ),
  );
  return {
    service,
    asked,
    polls,
    get anchors() {
      return asked.map((entry) => entry.anchor);
    },
  };
});

const terminal = (name: string): OrchestrationV2TranscriptTurn => ({
  ...terminalTurn,
  userNativeId: `u-${name}`,
  assistantNativeId: `a-${name}`,
});
const turnsOf = (...names: ReadonlyArray<string>): AgentSessionScanner.TurnsAfterAnchor => ({
  _tag: "Turns",
  turns: names.map(terminal),
  throughNativeId: `a-${names.at(-1)}`,
});
it.effect.each([
  {
    name: "a T3 run that reached the provider anchors on its provider turn",
    seeded: { runs: [{ status: "completed", turn: "a-t3" }] },
    anchor: "a-t3",
  },
  {
    name: "a removed queued run that never started is passed over",
    seeded: {
      runs: [
        { status: "completed", turn: "a-t3" },
        { status: "cancelled", turn: "never-started" },
      ],
    },
    anchor: "a-t3",
  },
  {
    name: "a run on an earlier provider thread of the same native session still anchors",
    seeded: {
      runs: [
        { status: "completed", turn: "a-t3", onEarlierProviderThread: true },
        { status: "failed", turn: "never-started" },
      ],
      importedItems: ["a-imported"],
    },
    anchor: "a-t3",
  },
  {
    name: "imported history anchors on its last item when no run reached the provider",
    seeded: { runs: [], importedItems: ["u-imported", "a-imported"] },
    anchor: "a-imported",
  },
] as const)("$name", ({ seeded, anchor }) =>
  Effect.gen(function* () {
    const projections = yield* seed(seeded);
    const sync = yield* makeService({ [anchor]: turnsOf("one") });

    assert.equal(yield* sync.service.sync(threadId), 1);

    assert.deepEqual(sync.anchors, [anchor]);
    const after = yield* projections.getThreadProjection(threadId);
    assert.equal(after.runs.at(-1)?.recordedFrom, "transcript");
  }).pipe(Effect.provide(layerStores)),
);

it.effect("anchors on the newest recorded turn through rounds of T3 and terminal turns", () =>
  Effect.gen(function* () {
    const projections = yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const sync = yield* makeService({ "a-t3": turnsOf("one"), "a-t3-again": turnsOf("two") });

    assert.equal(yield* sync.service.sync(threadId), 1);
    // A second T3 turn follows the first terminal turn.
    const now = yield* DateTime.now;
    yield* projections.apply({
      id: EventId.make("sync:run:again"),
      type: "run.created",
      threadId,
      occurredAt: now,
      payload: {
        id: RunId.make("run:again"),
        threadId,
        ordinal: 3,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make("message:again"),
        rootNodeId: null,
        activeAttemptId: RunAttemptId.make("attempt:again"),
        status: "completed",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("sync:turn:again"),
      type: "provider-turn.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: ProviderTurnId.make("provider-turn:again"),
        providerThreadId,
        nodeId: NodeId.make("node:again"),
        runAttemptId: RunAttemptId.make("attempt:again"),
        nativeTurnRef: { driver, nativeId: "a-t3-again", strength: "weak" },
        ordinal: 2,
        status: "completed",
        startedAt: now,
        completedAt: now,
      },
    });
    assert.equal(yield* sync.service.sync(threadId), 1);
    // Nothing new after the second terminal turn: the anchor is its last item.
    assert.equal(yield* sync.service.sync(threadId), 0);

    assert.deepEqual(sync.anchors, ["a-t3", "a-t3-again", "a-two"]);
    assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 4);
  }).pipe(Effect.provide(layerStores)),
);

it.effect("is not moved by the adapter re-emitting a stale provider thread", () =>
  Effect.gen(function* () {
    const projections = yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const sync = yield* makeService({ "a-t3": turnsOf("one") });
    assert.equal(yield* sync.service.sync(threadId), 1);

    yield* projections.apply({
      id: EventId.make("sync:stale-provider-thread"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: yield* DateTime.now,
      payload: providerThreadAt(yield* DateTime.now, false),
    });
    assert.equal(yield* sync.service.sync(threadId), 0);

    assert.deepEqual(sync.anchors, ["a-t3", "a-one"]);
  }).pipe(Effect.provide(layerStores)),
);

it.effect.each([
  { name: "a run is still active", seeded: { runs: [{ status: "running", turn: "a-t3" }] } },
  {
    name: "the latest run reached the provider but has no native turn id",
    seeded: { runs: [{ status: "failed", turn: "no-native-id" }] },
  },
  {
    // Claude writes a weak ref holding `turn:<attempt>` for a turn that never produced a record.
    name: "the latest run ended with a turn id that names no transcript record",
    seeded: { runs: [{ status: "failed", turn: "turn:attempt-1" }] },
  },
  {
    name: "the provider turn never ended, though it holds a real cursor",
    seeded: { runs: [{ status: "completed", turn: "a-t3", turnStatus: "running" }] },
  },
  {
    name: "a late usage write left the provider turn running with its placeholder",
    seeded: {
      runs: [
        {
          status: "completed",
          turn: "turn:attempt-1",
          turnStatus: "running",
          turnStrength: "strong",
        },
      ],
    },
  },
  { name: "nothing has anchored the thread yet", seeded: { runs: [] } },
  {
    name: "a rollback pins the native conversation head",
    seeded: {
      runs: [{ status: "completed", turn: "a-t3" }],
      nativeConversationHeadRef: true,
    },
  },
] as const)("skips the sync when $name", ({ seeded }) =>
  Effect.gen(function* () {
    const projections = yield* seed(seeded);
    const sync = yield* makeService({ "a-t3": turnsOf("one") });

    assert.equal(yield* sync.service.sync(threadId), 0);

    assert.deepEqual(sync.anchors, []);
    assert.equal(
      (yield* projections.getThreadProjection(threadId)).runs.length,
      seeded.runs.length,
    );
  }).pipe(Effect.provide(layerStores)),
);

it.effect("writes nothing when the anchor is not on the session's chain", () =>
  Effect.gen(function* () {
    const projections = yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const sync = yield* makeService({});

    assert.equal(yield* sync.service.sync(threadId), 0);

    assert.deepEqual(sync.anchors, ["a-t3"]);
    assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 1);
  }).pipe(Effect.provide(layerStores)),
);

it.effect("does not read a transcript that has not changed since the last sync", () =>
  Effect.gen(function* () {
    yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const sync = yield* makeService({
      "a-t3": { _tag: "Turns", turns: [], throughNativeId: "a-t3" },
    });

    assert.equal(yield* sync.service.sync(threadId), 0);
    assert.equal(yield* sync.service.sync(threadId), 0);

    assert.deepEqual(sync.asked, [
      { anchor: "a-t3", unchangedSince: undefined },
      { anchor: "a-t3", unchangedSince: signature },
    ]);
  }).pipe(Effect.provide(layerStores)),
);

it.effect("serializes triggers for a thread, and the later one finds the file unchanged", () =>
  Effect.gen(function* () {
    yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const asked: Array<AgentSessionScanner.TranscriptSignature | undefined> = [];
    const service = yield* TranscriptSyncService.TranscriptSyncService.pipe(
      Effect.provide(
        TranscriptSyncService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.mock(AgentSessionScanner.AgentSessionScanner)({
                readTurnsAfter: ({ unchangedSince }) =>
                  Effect.gen(function* () {
                    asked.push(unchangedSince);
                    if (unchangedSince !== undefined) return { _tag: "Unchanged" as const };
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                    return { ...turnsOf("one"), signature };
                  }),
              }),
              Layer.succeed(Orchestrator.OrchestratorV2, orchestrator),
            ),
          ),
        ),
      ),
    );

    const first = yield* Effect.forkChild(service.sync(threadId));
    yield* Deferred.await(entered);
    const second = yield* Effect.forkChild(service.sync(threadId), { startImmediately: true });
    yield* Deferred.succeed(release, undefined);

    assert.deepEqual([yield* Fiber.join(first), yield* Fiber.join(second)], [1, 0]);
    assert.deepEqual(asked, [undefined, signature]);
  }).pipe(Effect.provide(layerStores)),
);

it.effect("starts a sync in the background without waiting for it", () =>
  Effect.gen(function* () {
    yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const read = yield* Deferred.make<string>();
    const service = yield* TranscriptSyncService.TranscriptSyncService.pipe(
      Effect.provide(
        TranscriptSyncService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.mock(AgentSessionScanner.AgentSessionScanner)({
                readTurnsAfter: ({ anchorNativeId }) =>
                  Deferred.succeed(read, anchorNativeId).pipe(
                    Effect.as({ _tag: "NoTranscript" as const }),
                  ),
              }),
              Layer.succeed(Orchestrator.OrchestratorV2, orchestrator),
            ),
          ),
        ),
      ),
    );

    yield* service.syncInBackground(threadId);

    assert.equal(yield* Deferred.await(read), "a-t3");
  }).pipe(Effect.provide(layerStores)),
);

it.effect("reads again after a dispatch the thread refused, though the file is unchanged", () =>
  Effect.gen(function* () {
    const projections = yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const asked: Array<AgentSessionScanner.TranscriptSignature | undefined> = [];
    let reads = 0;
    const service = yield* TranscriptSyncService.TranscriptSyncService.pipe(
      Effect.provide(
        TranscriptSyncService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.mock(AgentSessionScanner.AgentSessionScanner)({
                readTurnsAfter: ({ unchangedSince }) =>
                  Effect.gen(function* () {
                    asked.push(unchangedSince);
                    if (unchangedSince !== undefined) return { _tag: "Unchanged" as const };
                    reads += 1;
                    if (reads === 1) {
                      // A run appears between the read and the command, so the command is refused.
                      const now = yield* DateTime.now;
                      yield* projections.apply({
                        id: EventId.make("sync:late-run"),
                        type: "run.created",
                        threadId,
                        occurredAt: now,
                        payload: {
                          id: RunId.make("run:late"),
                          threadId,
                          ordinal: 2,
                          providerInstanceId: instanceId,
                          modelSelection,
                          providerThreadId,
                          userMessageId: MessageId.make("message:late"),
                          rootNodeId: null,
                          activeAttemptId: null,
                          status: "cancelled",
                          requestedAt: now,
                          startedAt: null,
                          completedAt: now,
                          checkpointId: null,
                          contextHandoffId: null,
                        },
                      });
                    }
                    return { ...turnsOf("one"), signature };
                  }),
              }),
              Layer.succeed(Orchestrator.OrchestratorV2, orchestrator),
            ),
          ),
        ),
      ),
    );

    assert.equal(yield* service.sync(threadId), 0);
    assert.equal(yield* service.sync(threadId), 1);

    assert.deepEqual(asked, [undefined, undefined]);
  }).pipe(Effect.provide(layerStores)),
);

describe("Codex sessions", () => {
  it.effect.each([
    {
      name: "a T3 run anchors on the turn id its provider turn ended with",
      seeded: {
        provider: "codex",
        runs: [{ status: "completed", turn: "turn-t3", turnStrength: "strong" }],
      },
      anchor: "turn-t3",
    },
    {
      name: "imported history anchors on the turn id of its last item",
      seeded: { provider: "codex", runs: [], importedItems: ["turn-a", "turn-b"] },
      anchor: "turn-b",
    },
  ] as const)("reads the rollout when $name", ({ seeded, anchor }) =>
    Effect.gen(function* () {
      const projections = yield* seed(seeded);
      const sync = yield* makeService({ [anchor]: turnsOf("one") });

      assert.equal(yield* sync.service.sync(threadId), 1);

      assert.deepEqual(sync.anchors, [anchor]);
      assert.deepEqual(sync.polls, [undefined]);
      assert.equal(
        (yield* projections.getThreadProjection(threadId)).runs.at(-1)?.recordedFrom,
        "transcript",
      );
    }).pipe(Effect.provide(layerStores)),
  );

  it.effect.each([
    {
      name: "the latest run's provider turn has a weak Codex turn id",
      seeded: { provider: "codex", runs: [{ status: "completed", turn: "turn-t3" }] },
    },
    {
      name: "the provider turn never ended",
      seeded: {
        provider: "codex",
        runs: [
          { status: "completed", turn: "turn-t3", turnStrength: "strong", turnStatus: "running" },
        ],
      },
    },
    {
      name: "a rollback pinned the native conversation head",
      seeded: {
        provider: "codex",
        runs: [{ status: "completed", turn: "turn-t3", turnStrength: "strong" }],
        nativeConversationHeadRef: true,
      },
    },
  ] as const)("does not read the rollout when $name", ({ seeded }) =>
    Effect.gen(function* () {
      yield* seed(seeded);
      const sync = yield* makeService({ "turn-t3": turnsOf("one") });

      assert.equal(yield* sync.service.sync(threadId), 0);

      assert.deepEqual(sync.anchors, []);
    }).pipe(Effect.provide(layerStores)),
  );

  it.effect(
    "tells the reader a poll from an open, so a poll can leave a missing rollout alone",
    () =>
      Effect.gen(function* () {
        yield* seed({
          provider: "codex",
          runs: [{ status: "completed", turn: "turn-t3", turnStrength: "strong" }],
        });
        const sync = yield* makeService({});

        yield* sync.service.sync(threadId, { poll: true });
        yield* sync.service.sync(threadId);

        assert.deepEqual(sync.polls, [true, undefined]);
      }).pipe(Effect.provide(layerStores)),
  );

  it.effect("moves its anchor to the turn it recorded, which names the turn and not an item", () =>
    Effect.gen(function* () {
      const projections = yield* seed({
        provider: "codex",
        runs: [{ status: "completed", turn: "turn-t3", turnStrength: "strong" }],
      });
      const recorded: AgentSessionScanner.TurnsAfterAnchor = {
        _tag: "Turns",
        turns: [{ ...terminal("one"), nativeRefId: "turn-one" }],
        throughNativeId: "turn-one",
      };
      const sync = yield* makeService({
        "turn-t3": recorded,
        "turn-one": { _tag: "Turns", turns: [], throughNativeId: "turn-one" },
      });

      assert.equal(yield* sync.service.sync(threadId), 1);
      assert.equal(yield* sync.service.sync(threadId), 0);

      assert.deepEqual(sync.anchors, ["turn-t3", "turn-one"]);
      assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 2);
    }).pipe(Effect.provide(layerStores)),
  );
});

describe("Pi sessions", () => {
  it.effect("anchors on the leaf entry its last T3 turn ended at, though the head is set", () =>
    Effect.gen(function* () {
      const projections = yield* seed({
        provider: "pi",
        head: "leaf-1",
        runs: [{ status: "completed", turn: "user-entry-1", turnStrength: "strong" }],
      });
      const sync = yield* makeService({ "leaf-1": turnsOf("one") });

      assert.equal(yield* sync.service.sync(threadId), 1);

      assert.deepEqual(sync.anchors, ["leaf-1"]);
      assert.equal(
        (yield* projections.getThreadProjection(threadId)).runs.at(-1)?.recordedFrom,
        "transcript",
      );
    }).pipe(Effect.provide(layerStores)),
  );

  it.effect("anchors on the last imported entry when no T3 turn has run", () =>
    Effect.gen(function* () {
      yield* seed({ provider: "pi", runs: [], importedItems: ["entry-1", "entry-2"] });
      const sync = yield* makeService({ "entry-2": turnsOf("one") });

      assert.equal(yield* sync.service.sync(threadId), 1);

      assert.deepEqual(sync.anchors, ["entry-2"]);
    }).pipe(Effect.provide(layerStores)),
  );

  it.effect("follows a T3 turn after a terminal turn to the head that turn left", () =>
    Effect.gen(function* () {
      const projections = yield* seed({
        provider: "pi",
        head: "leaf-1",
        runs: [{ status: "completed", turn: "user-entry-1", turnStrength: "strong" }],
      });
      const sync = yield* makeService({ "leaf-1": turnsOf("one"), "leaf-2": turnsOf("two") });
      assert.equal(yield* sync.service.sync(threadId), 1);

      // A second T3 turn follows the terminal turn and moves the head.
      const now = yield* DateTime.now;
      yield* projections.apply({
        id: EventId.make("sync:run:pi-again"),
        type: "run.created",
        threadId,
        occurredAt: now,
        payload: {
          id: RunId.make("run:pi-again"),
          threadId,
          ordinal: 3,
          providerInstanceId: instanceId,
          modelSelection,
          providerThreadId,
          userMessageId: MessageId.make("message:pi-again"),
          rootNodeId: null,
          activeAttemptId: RunAttemptId.make("attempt:pi-again"),
          status: "completed",
          requestedAt: now,
          startedAt: now,
          completedAt: now,
          checkpointId: null,
          contextHandoffId: null,
        },
      });
      yield* projections.apply({
        id: EventId.make("sync:turn:pi-again"),
        type: "provider-turn.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: ProviderTurnId.make("provider-turn:pi-again"),
          providerThreadId,
          nodeId: NodeId.make("node:pi-again"),
          runAttemptId: RunAttemptId.make("attempt:pi-again"),
          nativeTurnRef: { driver: PI_PROVIDER, nativeId: "user-entry-2", strength: "strong" },
          ordinal: 2,
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
      yield* projections.apply({
        id: EventId.make("sync:provider-thread:pi-again"),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: providerThreadAt(now, false, PI_PROVIDER, "leaf-2"),
      });
      assert.equal(yield* sync.service.sync(threadId), 1);

      assert.deepEqual(sync.anchors, ["leaf-1", "leaf-2"]);
      assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 4);
    }).pipe(Effect.provide(layerStores)),
  );

  it.effect(
    "writes nothing when the user rewound the session, so the head is not on its branch",
    () =>
      Effect.gen(function* () {
        const projections = yield* seed({
          provider: "pi",
          head: "leaf-1",
          runs: [{ status: "completed", turn: "user-entry-1", turnStrength: "strong" }],
        });
        const sync = yield* makeService({});

        assert.equal(yield* sync.service.sync(threadId), 0);

        assert.deepEqual(sync.anchors, ["leaf-1"]);
        assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 1);
      }).pipe(Effect.provide(layerStores)),
  );

  it.effect("anchors on the head a rollback left, passing over the runs it removed", () =>
    Effect.gen(function* () {
      yield* seed({
        provider: "pi",
        head: "fork-leaf",
        runs: [
          { status: "completed", turn: "user-entry-1", turnStrength: "strong" },
          { status: "rolled_back", turn: "no-native-id" },
        ],
      });
      const sync = yield* makeService({ "fork-leaf": turnsOf("one") });

      assert.equal(yield* sync.service.sync(threadId), 1);

      assert.deepEqual(sync.anchors, ["fork-leaf"]);
    }).pipe(Effect.provide(layerStores)),
  );

  it.effect.each([
    {
      name: "the latest run's provider turn holds a weak synthetic ref, as after a stale cursor",
      seeded: {
        provider: "pi",
        head: "leaf-1",
        runs: [{ status: "completed", turn: "turn:synthetic" }],
      },
    },
    {
      name: "the latest run's provider turn left no entry ref",
      seeded: {
        provider: "pi",
        head: "leaf-1",
        runs: [{ status: "failed", turn: "no-native-id" }],
      },
    },
    {
      name: "the provider thread has no head",
      seeded: {
        provider: "pi",
        runs: [{ status: "completed", turn: "user-entry-1", turnStrength: "strong" }],
      },
    },
  ] as const)("does not read the Pi session when $name", ({ seeded }) =>
    Effect.gen(function* () {
      yield* seed(seeded);
      const sync = yield* makeService({ "leaf-1": turnsOf("one") });

      assert.equal(yield* sync.service.sync(threadId), 0);

      assert.deepEqual(sync.anchors, []);
    }).pipe(Effect.provide(layerStores)),
  );
});

describe("a newest turn with no native ref", () => {
  const providers = [
    { provider: "claudeAgent", anchor: "a-t3", head: undefined, turn: "a-t3" },
    { provider: "codex", anchor: "turn-t3", head: undefined, turn: "turn-t3" },
    { provider: "pi", anchor: "leaf-1", head: "leaf-1", turn: "user-entry-1" },
  ] as const;

  it.effect.each(providers)(
    "is passed over for the turn before it when it wrote nothing: $provider",
    ({ provider, anchor, head, turn }) =>
      Effect.gen(function* () {
        yield* seed({
          provider,
          ...(head === undefined ? {} : { head }),
          runs: [
            { status: "completed", turn, turnStrength: "strong" },
            // A Codex /goal control turn, or a Pi turn that wrote no entries.
            { status: "completed", turn: "no-native-id" },
          ],
        });
        const sync = yield* makeService({ [anchor]: turnsOf("one") });

        assert.equal(yield* sync.service.sync(threadId), 1);

        assert.deepEqual(sync.anchors, [anchor]);
      }).pipe(Effect.provide(layerStores)),
  );

  it.effect.each(providers)(
    "stops the sync when it left output, since nothing after it can be told apart: $provider",
    ({ provider, head, turn }) =>
      Effect.gen(function* () {
        yield* seed({
          provider,
          ...(head === undefined ? {} : { head }),
          runs: [
            { status: "completed", turn, turnStrength: "strong" },
            { status: "failed", turn: "no-native-id", output: true },
          ],
        });
        const sync = yield* makeService({});

        assert.equal(yield* sync.service.sync(threadId), 0);

        assert.deepEqual(sync.anchors, []);
      }).pipe(Effect.provide(layerStores)),
  );
});
