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
  type OrchestrationV2TranscriptTurn,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
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
  readonly status: "completed" | "running" | "failed";
  /** The native id the run's provider turn stored; null leaves the run without a provider turn. */
  readonly nativeTurnId: string | null;
};

const seed = Effect.fn("TranscriptSyncService.test.seed")(function* (input: {
  readonly runs: ReadonlyArray<SeedRun>;
  readonly transcriptSync?: { throughNativeId: string; throughRunOrdinal: number };
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
    payload: {
      id: providerThreadId,
      driver,
      providerInstanceId: instanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: "native-session", strength: "strong" },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      nativeMetadata:
        input.transcriptSync === undefined ? null : { transcriptSync: input.transcriptSync },
      createdAt: now,
      updatedAt: now,
    },
  });
  for (const [index, run] of input.runs.entries()) {
    const ordinal = index + 1;
    const attemptId = RunAttemptId.make(`attempt:${ordinal}`);
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
        activeAttemptId: run.nativeTurnId === null ? null : attemptId,
        status: run.status,
        requestedAt: now,
        startedAt: now,
        completedAt: run.status === "running" ? null : now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    if (run.nativeTurnId !== null) {
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
          nativeTurnRef: { driver, nativeId: run.nativeTurnId, strength: "weak" },
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

/** A sync whose reader answers with `reads[anchor]` and remembers every anchor it was asked for. */
const makeService = Effect.fn("TranscriptSyncService.test.makeService")(function* (
  reads: Record<string, AgentSessionScanner.TurnsAfterAnchor>,
) {
  const anchors: Array<string> = [];
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const scanner = Layer.mock(AgentSessionScanner.AgentSessionScanner)({
    readTurnsAfter: ({ anchorNativeId }) =>
      Effect.sync(() => {
        anchors.push(anchorNativeId);
        return reads[anchorNativeId] ?? { _tag: "AnchorNotFound" as const };
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
  return { service, anchors };
});
const turns: AgentSessionScanner.TurnsAfterAnchor = {
  _tag: "Turns",
  turns: [terminalTurn],
  throughNativeId: "a-terminal",
};

it.effect.each([
  {
    name: "a T3 run newer than the last sync anchors on its provider turn",
    runs: [{ status: "completed", nativeTurnId: "a-t3" }],
    anchor: "a-t3",
  },
  {
    name: "a last sync newer than the T3 runs anchors on where it stopped",
    runs: [{ status: "completed", nativeTurnId: "a-t3" }],
    transcriptSync: { throughNativeId: "a-synced", throughRunOrdinal: 1 },
    anchor: "a-synced",
  },
] as const)("$name", ({ runs, anchor, ...rest }) =>
  Effect.gen(function* () {
    const projections = yield* seed({
      runs,
      ...("transcriptSync" in rest ? { transcriptSync: rest.transcriptSync } : {}),
    });
    const { service, anchors } = yield* makeService({ [anchor]: turns });

    assert.equal(yield* service.sync(threadId), 1);

    assert.deepEqual(anchors, [anchor]);
    const after = yield* projections.getThreadProjection(threadId);
    assert.equal(after.runs.at(-1)?.recordedFrom, "transcript");
    assert.equal(after.runs.length, runs.length + 1);
    // The recorded turn moves the anchor, so a second sync reads from after it.
    assert.equal(yield* service.sync(threadId), 0);
    assert.deepEqual(anchors.at(-1), "a-terminal");
  }).pipe(Effect.provide(layerStores)),
);

it.effect.each([
  { name: "a run is still active", runs: [{ status: "running", nativeTurnId: "a-t3" }] },
  {
    name: "the latest T3 run ended without a native turn id",
    runs: [{ status: "failed", nativeTurnId: null }],
  },
  { name: "nothing has anchored the thread yet", runs: [] },
] as const)("skips the sync when $name", ({ runs }) =>
  Effect.gen(function* () {
    const projections = yield* seed({ runs });
    const { service, anchors } = yield* makeService({ "a-t3": turns });

    assert.equal(yield* service.sync(threadId), 0);

    assert.deepEqual(anchors, []);
    assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, runs.length);
  }).pipe(Effect.provide(layerStores)),
);

it.effect("writes nothing when the anchor is not on the session's chain", () =>
  Effect.gen(function* () {
    const projections = yield* seed({ runs: [{ status: "completed", nativeTurnId: "a-t3" }] });
    const { service, anchors } = yield* makeService({});

    assert.equal(yield* service.sync(threadId), 0);

    assert.deepEqual(anchors, ["a-t3"]);
    assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 1);
  }).pipe(Effect.provide(layerStores)),
);

it.effect("starts a sync in the background without waiting for it", () =>
  Effect.gen(function* () {
    yield* seed({ runs: [{ status: "completed", nativeTurnId: "a-t3" }] });
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
