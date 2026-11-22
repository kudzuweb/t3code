import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as TranscriptSyncService from "./TranscriptSyncService.ts";
import * as TranscriptSyncWorker from "./TranscriptSyncWorker.ts";

const claude = ProviderDriverKind.make("claudeAgent");
const other = ProviderDriverKind.make("someOtherDriver");

/** A worker over fakes that record what the projection was asked for and what was synced. */
const startWorker = Effect.fn("TranscriptSyncWorker.test.startWorker")(function* (input: {
  readonly candidates: () => ReturnType<
    ProjectionStore.ProjectionStoreV2Shape["listTranscriptSyncCandidates"]
  >;
  readonly pollMany?: () => Effect.Effect<number>;
}) {
  const queries: Array<DateTime.Utc> = [];
  const synced: Array<ReadonlyArray<ThreadId>> = [];
  yield* Layer.build(
    TranscriptSyncWorker.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            listTranscriptSyncCandidates: ({ updatedSince }) =>
              Effect.sync(() => queries.push(updatedSince)).pipe(
                Effect.andThen(input.candidates()),
              ),
          }),
          Layer.mock(AgentSessionScanner.AgentSessionScanner)({
            supportsTranscriptSync: (driver) => driver === claude,
          }),
          Layer.mock(TranscriptSyncService.TranscriptSyncService)({
            pollMany: (candidates) =>
              Effect.sync(() =>
                synced.push(candidates.map((candidate) => candidate.threadId)),
              ).pipe(Effect.andThen(input.pollMany?.() ?? Effect.succeed(0))),
          }),
        ),
      ),
    ),
  );
  return { queries, synced };
});

const candidate = (threadId: ThreadId, driver: ProviderDriverKind) => ({
  threadId,
  driver,
  providerThreadId: ProviderThreadId.make(`provider-thread:${threadId}`),
  providerInstanceId: ProviderInstanceId.make("claudeAgent"),
  nativeId: `native:${threadId}`,
});

const a = ThreadId.make("thread:a");
const b = ThreadId.make("thread:b");

it.effect("syncs the supported candidates once at start and again every interval", () =>
  Effect.gen(function* () {
    const worker = yield* startWorker({
      candidates: () =>
        Effect.succeed([
          candidate(a, claude),
          candidate(ThreadId.make("thread:unsupported"), other),
          candidate(b, claude),
        ]),
    });
    yield* Effect.yieldNow;
    assert.deepEqual(worker.synced, [[a, b]]);

    yield* TestClock.adjust(TranscriptSyncWorker.TRANSCRIPT_SYNC_INTERVAL);
    assert.equal(worker.synced.length, 2);

    yield* TestClock.adjust(
      Duration.millis(Duration.toMillis(TranscriptSyncWorker.TRANSCRIPT_SYNC_INTERVAL) - 1),
    );
    assert.equal(worker.synced.length, 2);
  }).pipe(Effect.scoped),
);

it.effect("asks only for threads active within the activity window", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse("2026-11-22T12:00:00.000Z"));
    const worker = yield* startWorker({ candidates: () => Effect.succeed([]) });
    yield* Effect.yieldNow;

    assert.equal(
      DateTime.toEpochMillis(worker.queries[0]!),
      Date.parse("2026-11-22T12:00:00.000Z") -
        Duration.toMillis(TranscriptSyncWorker.TRANSCRIPT_SYNC_ACTIVITY_WINDOW),
    );
    assert.deepEqual(worker.synced, []);
  }).pipe(Effect.scoped),
);

it.effect("does not start a tick while the previous one is still running", () =>
  Effect.gen(function* () {
    const release = yield* Deferred.make<void>();
    const worker = yield* startWorker({
      candidates: () => Effect.succeed([candidate(a, claude)]),
      pollMany: () => Deferred.await(release).pipe(Effect.as(0)),
    });
    yield* Effect.yieldNow;

    yield* TestClock.adjust(Duration.times(TranscriptSyncWorker.TRANSCRIPT_SYNC_INTERVAL, 3));
    assert.equal(worker.synced.length, 1);

    yield* Deferred.succeed(release, undefined);
    yield* TestClock.adjust(TranscriptSyncWorker.TRANSCRIPT_SYNC_INTERVAL);
    assert.equal(worker.synced.length, 2);
  }).pipe(Effect.scoped),
);

it.effect("keeps running after a tick fails", () =>
  Effect.gen(function* () {
    let attempts = 0;
    const worker = yield* startWorker({
      candidates: () =>
        Effect.suspend(() =>
          ++attempts === 1
            ? Effect.die("the projection read failed")
            : Effect.succeed([candidate(a, claude)]),
        ),
    });
    yield* Effect.yieldNow;
    assert.deepEqual(worker.synced, []);

    yield* TestClock.adjust(TranscriptSyncWorker.TRANSCRIPT_SYNC_INTERVAL);
    assert.deepEqual(worker.synced, [[a]]);
  }).pipe(Effect.scoped),
);
