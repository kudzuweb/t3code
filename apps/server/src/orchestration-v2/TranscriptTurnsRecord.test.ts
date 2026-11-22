import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2TranscriptTurn,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("claudeAgent");
const driver = ProviderDriverKind.make("claudeAgent");
const modelSelection = { instanceId, model: "claude-haiku-5-5" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("A transcript record needs no provider process"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
  EventStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "transcript-turns" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

const threadId = ThreadId.make("thread:transcript");
const providerThreadId = ProviderThreadId.make("provider-thread:transcript");

const turn = (name: string, minute: number): OrchestrationV2TranscriptTurn => ({
  userNativeId: `${name}-user`,
  userText: `Prompt ${name}`,
  userAt: `2026-11-20T10:0${minute}:00.000Z`,
  assistantNativeId: `${name}-assistant`,
  assistantText: `Reply ${name}`,
  assistantAt: `2026-11-20T10:0${minute}:30.000Z`,
});

const setup = Effect.fn("TranscriptTurnsRecord.setup")(function* (
  latestRunStatus: "completed" | "running" | null,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create-transcript-thread"),
    threadId,
    projectId: ProjectId.make("project:transcript"),
    title: "Transcript",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* projections.apply({
    id: EventId.make("transcript:provider-thread"),
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
      createdAt: now,
      updatedAt: now,
    },
  });
  if (latestRunStatus !== null) {
    yield* projections.apply({
      id: EventId.make("transcript:t3-run"),
      type: "run.created",
      threadId,
      occurredAt: now,
      payload: {
        id: RunId.make("run:t3"),
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make("message:t3"),
        rootNodeId: null,
        activeAttemptId: null,
        status: latestRunStatus,
        requestedAt: now,
        startedAt: now,
        completedAt: latestRunStatus === "completed" ? now : null,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
  }
  return { orchestrator, projections };
});

const record = (
  commandId: string,
  expectedLatestRunOrdinal: number,
  turns: ReadonlyArray<OrchestrationV2TranscriptTurn>,
) =>
  ({
    type: "transcript_turns.record",
    commandId: CommandId.make(commandId),
    threadId,
    providerThreadId,
    expectedLatestRunOrdinal,
    turns,
  }) as const;

it.effect(
  "records terminal turns as completed runs after the latest run, once, without compaction or effects",
  () =>
    Effect.gen(function* () {
      const { orchestrator, projections } = yield* setup("completed");
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const events = yield* EventStore.EventStoreV2;
      const turns = [turn("a", 1), turn("b", 2)];
      const command = record("record-transcript-turns", 1, turns);

      const first = yield* orchestrator.dispatch(command);
      const after = yield* projections.getThreadProjection(threadId);
      const recorded = after.runs.filter((run) => run.recordedFrom === "transcript");
      assert.deepEqual(
        recorded.map((run) => [run.ordinal, run.status, run.providerThreadId, run.id]),
        [
          [2, "completed", providerThreadId, "transcript-run:native-session:a-user"],
          [3, "completed", providerThreadId, "transcript-run:native-session:b-user"],
        ],
      );
      assert.deepEqual(
        recorded.map((run) => [run.requestedAt, run.completedAt].map(DateTime.formatIso)),
        [
          ["2026-11-20T10:01:00.000Z", "2026-11-20T10:01:30.000Z"],
          ["2026-11-20T10:02:00.000Z", "2026-11-20T10:02:30.000Z"],
        ],
      );
      for (const run of recorded) {
        const items = after.turnItems.filter((item) => item.runId === run.id);
        assert.deepEqual(items.map((item) => item.type).toSorted(), [
          "assistant_message",
          "user_message",
        ]);
        assert.isTrue(
          after.messages.some((message) => message.id === run.userMessageId),
          "the run's user message exists",
        );
      }
      // The native ids on the last items are what the next sync anchors on.
      assert.deepEqual(
        recorded.map((run) =>
          after.turnItems
            .filter((item) => item.runId === run.id)
            .map((item) => [item.type, item.nativeItemRef?.nativeId, item.nativeItemRef?.strength]),
        ),
        [
          [
            ["user_message", "a-user", "strong"],
            ["assistant_message", "a-assistant", "strong"],
          ],
          [
            ["user_message", "b-user", "strong"],
            ["assistant_message", "b-assistant", "strong"],
          ],
        ],
      );
      assert.isNull(after.providerThreads[0]?.nativeMetadata ?? null);

      const stored = yield* events.read({ threadId }).pipe(Stream.runCollect);
      assert.notInclude(
        stored.map(({ event }) => event.type),
        "run.updated",
      );
      assert.deepEqual(yield* outbox.listByCommandId(command.commandId), []);

      const again = yield* orchestrator.dispatch(command);
      assert.equal(again.sequence, first.sequence);
      assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 3);
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each([
  { name: "a run is still active", status: "running", expected: 1 },
  { name: "a newer run exists than the reader saw", status: "completed", expected: 0 },
] as const)("refuses to record when $name and writes nothing", ({ status, expected }) =>
  Effect.gen(function* () {
    const { orchestrator, projections } = yield* setup(status);
    const before = yield* projections.getThreadProjection(threadId);

    const outcome = yield* orchestrator
      .dispatch(record("record-refused", expected, [turn("a", 1)]))
      .pipe(Effect.result);

    assert.isTrue(Result.isFailure(outcome));
    assert.instanceOf(
      Result.isFailure(outcome) ? outcome.failure : undefined,
      Orchestrator.OrchestratorCommandRejectedError,
    );
    assert.deepEqual(yield* projections.getThreadProjection(threadId), before);

    // A refusal is not recorded, so the same command id succeeds once the thread matches.
    const settled = yield* projections.getThreadProjection(threadId);
    if (status === "running") return;
    yield* orchestrator.dispatch(record("record-refused", settled.runs.length, [turn("a", 1)]));
    assert.equal((yield* projections.getThreadProjection(threadId)).runs.length, 2);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("refuses a fork whose source is a recorded terminal turn", () =>
  Effect.gen(function* () {
    const { orchestrator, projections } = yield* setup("completed");
    yield* orchestrator.dispatch(record("record-before-fork", 1, [turn("a", 1)]));
    const recorded = (yield* projections.getThreadProjection(threadId)).runs.find(
      (run) => run.recordedFrom === "transcript",
    )!;
    const targetThreadId = ThreadId.make("thread:fork-target");

    const refused = yield* orchestrator
      .dispatch({
        type: "thread.fork",
        commandId: CommandId.make("fork-terminal-turn"),
        sourceThreadId: threadId,
        targetThreadId,
        sourcePoint: { type: "run", runId: recorded.id },
        createdBy: "user",
        creationSource: "web",
      })
      .pipe(Effect.flip);

    assert.include(String(Reflect.get(refused, "cause")), "made outside T3");
    assert.isNull(yield* projections.getThreadShell(targetThreadId));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("writes nothing for turns it already recorded, even under a new command id", () =>
  Effect.gen(function* () {
    const { orchestrator, projections } = yield* setup("completed");
    yield* orchestrator.dispatch(record("first-read", 1, [turn("a", 1)]));
    const before = yield* projections.getThreadProjection(threadId);

    // A second read of the same turns arrives after the first was recorded.
    yield* orchestrator.dispatch(record("second-read", 2, [turn("a", 1)]));
    assert.deepEqual(yield* projections.getThreadProjection(threadId), before);

    // New turns beside an already recorded one are recorded after it.
    yield* orchestrator.dispatch(record("third-read", 2, [turn("a", 1), turn("b", 2)]));
    const after = yield* projections.getThreadProjection(threadId);
    assert.deepEqual(
      after.runs.map((run) => run.ordinal),
      [1, 2, 3],
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect("skips turns recorded under an earlier provider thread of the same native session", () =>
  Effect.gen(function* () {
    const { orchestrator, projections } = yield* setup("completed");
    yield* orchestrator.dispatch(record("under-first", 1, [turn("a", 1)]));
    const before = yield* projections.getThreadProjection(threadId);
    const now = yield* DateTime.now;

    // An instance switch moves the same native session to a new provider thread.
    const switchedTo = ProviderThreadId.make("provider-thread:switched");
    yield* projections.apply({
      id: EventId.make("transcript:switched-provider-thread"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: { ...before.providerThreads[0]!, id: switchedTo },
    });
    yield* orchestrator.dispatch({
      ...record("under-second", 2, [turn("a", 1)]),
      providerThreadId: switchedTo,
    });

    assert.deepEqual(
      (yield* projections.getThreadProjection(threadId)).runs.map((run) => run.ordinal),
      [1, 2],
    );
  }).pipe(Effect.provide(layerTest)),
);
