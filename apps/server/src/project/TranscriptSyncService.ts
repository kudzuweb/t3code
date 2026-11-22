import {
  CommandId,
  OrchestrationV2SyncTranscriptTurnsError,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type ThreadId,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";

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

const isTerminal = (run: OrchestrationV2Run) =>
  run.status !== "preparing" &&
  run.status !== "queued" &&
  run.status !== "starting" &&
  run.status !== "running" &&
  run.status !== "waiting";

// Claude Code names a turn that is still running `turn:<attempt>`; only a finished turn has a uuid.
const isNativeRecordId = (nativeId: string) => !nativeId.startsWith("turn:");

const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const locks = yield* KeyedLock.make<ThreadId>();

  const anchorFor = (input: {
    readonly latestT3Run: OrchestrationV2Run | undefined;
    readonly providerThread: OrchestrationV2ProviderThread;
    readonly nativeTurnId: string | undefined;
  }): string | undefined => {
    const synced = input.providerThread.nativeMetadata?.transcriptSync;
    const t3RunSinceSync =
      input.latestT3Run !== undefined &&
      input.latestT3Run.ordinal > (synced?.throughRunOrdinal ?? 0);
    if (!t3RunSinceSync) return synced?.throughNativeId;
    // A turn that ended without a native id, such as a failed or interrupted one, may sit beside
    // output of its own in the transcript, so nothing after it can be told apart.
    return input.nativeTurnId !== undefined && isNativeRecordId(input.nativeTurnId)
      ? input.nativeTurnId
      : undefined;
  };

  const syncLocked = Effect.fn("TranscriptSyncService.sync")(function* (threadId: ThreadId) {
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

    const latestT3Run = runs
      .filter((run) => run.recordedFrom === undefined)
      .toSorted((left, right) => right.ordinal - left.ordinal)[0];
    const latestT3Turn =
      latestT3Run?.providerThreadId === providerThread.id
        ? Orchestrator.providerTurnForRun(records, latestT3Run)
        : undefined;
    const anchorNativeId = anchorFor({
      latestT3Run,
      providerThread,
      nativeTurnId: latestT3Turn?.nativeTurnRef?.nativeId,
    });
    if (anchorNativeId === undefined) {
      yield* Effect.logDebug("Transcript sync has no anchor", { threadId });
      return 0;
    }

    const read = yield* scanner.readTurnsAfter({
      driver: providerThread.driver,
      providerInstanceId: providerThread.providerInstanceId,
      providerSessionId: nativeThread.nativeId,
      anchorNativeId,
    });
    if (read._tag === "AnchorNotFound") {
      yield* Effect.logWarning("Transcript sync could not find its anchor in the session", {
        threadId,
        anchorNativeId,
      });
      return 0;
    }
    if (read._tag !== "Turns" || read.turns.length === 0) return 0;

    const latestRunOrdinal = runs.reduce((max, run) => Math.max(max, run.ordinal), 0);
    return yield* orchestrator
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
        throughNativeId: read.throughNativeId,
      })
      .pipe(
        Effect.as(read.turns.length),
        // The thread changed since it was read; the next trigger reads it again.
        Effect.catchTags({ OrchestratorCommandRejectedError: () => Effect.succeed(0) }),
      );
  });

  const sync: TranscriptSyncService["Service"]["sync"] = (threadId) =>
    locks
      .withLock(threadId, syncLocked(threadId))
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
