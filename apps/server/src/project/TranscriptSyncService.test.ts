import { assert, it } from "@effect/vitest";
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

const terminalTurn: OrchestrationV2TranscriptTurn = {
  userNativeId: "u-terminal",
  userText: "From the terminal",
  userAt: "2026-11-20T10:01:00.000Z",
  assistantNativeId: "a-terminal",
  assistantText: "Answered in the terminal",
  assistantAt: "2026-11-20T10:01:30.000Z",
};

type SeedRun = {
  readonly status: "completed" | "running" | "failed" | "cancelled";
  /**
   * What the run left in the provider: its native turn id, `"no-native-id"` for a turn without
   * one, or `"never-started"` for a run that never reached the provider.
   */
  readonly turn: string;
};

const seed = Effect.fn("TranscriptSyncService.test.seed")(function* (input: {
  readonly runs: ReadonlyArray<SeedRun>;
  /** Native ids of imported history items, which have no run. */
  readonly importedItems?: ReadonlyArray<string>;
  readonly nativeConversationHeadRef?: boolean;
}) {
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
  yield* projections.apply({
    id: EventId.make("sync:provider-thread"),
    type: "provider-thread.updated",
    threadId,
    occurredAt: now,
    payload: providerThreadAt(now, input.nativeConversationHeadRef === true),
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
        nativeItemRef: { driver, nativeId, strength: "strong" },
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
        providerThreadId,
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
          providerThreadId,
          nodeId: NodeId.make(`node:${ordinal}`),
          runAttemptId: attemptId,
          nativeTurnRef:
            run.turn === "no-native-id" ? null : { driver, nativeId: run.turn, strength: "weak" },
          ordinal,
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
    }
  }
  return projections;
});

const providerThreadAt = (now: DateTime.Utc, pinnedHead: boolean) => ({
  id: providerThreadId,
  driver,
  providerInstanceId: instanceId,
  providerSessionId: null,
  appThreadId: threadId,
  ownerNodeId: null,
  nativeThreadRef: { driver, nativeId: "native-session", strength: "strong" as const },
  nativeConversationHeadRef: pinnedHead
    ? { driver, nativeId: "rolled-back-head", strength: "weak" as const }
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
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const scanner = Layer.mock(AgentSessionScanner.AgentSessionScanner)({
    readTurnsAfter: ({ anchorNativeId, unchangedSince }) =>
      Effect.sync(() => {
        asked.push({ anchor: anchorNativeId, unchangedSince });
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

it.effect("lets a trigger that arrives during a sync join it", () =>
  Effect.gen(function* () {
    yield* seed({ runs: [{ status: "completed", turn: "a-t3" }] });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let reads = 0;
    const service = yield* TranscriptSyncService.TranscriptSyncService.pipe(
      Effect.provide(
        TranscriptSyncService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.mock(AgentSessionScanner.AgentSessionScanner)({
                readTurnsAfter: () =>
                  Effect.gen(function* () {
                    reads += 1;
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                    return turnsOf("one");
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

    assert.deepEqual([yield* Fiber.join(first), yield* Fiber.join(second)], [1, 1]);
    assert.equal(reads, 1);
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
