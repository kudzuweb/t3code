import {
  CommandId,
  OrchestrationV2SyncTranscriptTurnsError,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderThreadId,
  type ThreadId,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { claudeTurnCursor } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import type { ProjectionRecords } from "../orchestration-v2/ProjectionStore.ts";
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
    /**
     * Syncs each thread, a few at a time, and returns the total recorded. A thread that fails is
     * logged and the rest still sync.
     */
    readonly syncMany: (threadIds: ReadonlyArray<ThreadId>) => Effect.Effect<number>;
    /**
     * One tick of the poll: syncs the candidates whose session file changed, and a limited number
     * the server has not read yet. A candidate whose file is unchanged costs one stat and no
     * projection read. Returns the total recorded; a failure is logged per thread.
     */
    readonly pollMany: (
      candidates: ReadonlyArray<TranscriptSyncPollCandidate>,
    ) => Effect.Effect<number>;
    /** Starts a sync and returns at once; a failure is logged, never raised. */
    readonly syncInBackground: (threadId: ThreadId) => Effect.Effect<void>;
  }
>()("t3/project/TranscriptSyncService") {}

/** A thread the poll may sync, with what the file check needs to find its session. */
export interface TranscriptSyncPollCandidate {
  readonly threadId: ThreadId;
  readonly driver: ProviderDriverKind;
  readonly providerThreadId: ProviderThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly nativeId: string;
}

const SYNC_MANY_CONCURRENCY = 4;

/** How many sessions a tick may read for the first time, so a server start spreads its reads out. */
export const TRANSCRIPT_SYNC_COLD_READS_PER_TICK = 10;

/** How long the poll leaves a session it could not find alone. Opening and focus look at once. */
export const TRANSCRIPT_SYNC_NO_TRANSCRIPT_RECHECK = Duration.minutes(5);

const providerTurnEnded = (turn: OrchestrationV2ProviderTurn) =>
  turn.status === "completed" ||
  turn.status === "interrupted" ||
  turn.status === "failed" ||
  turn.status === "cancelled";

const isTerminal = (run: OrchestrationV2Run) =>
  run.status !== "preparing" &&
  run.status !== "queued" &&
  run.status !== "starting" &&
  run.status !== "running" &&
  run.status !== "waiting";

const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const locks = yield* KeyedLock.make<ThreadId>();
  // What the last read of each provider thread's transcript saw, so an unchanged file is not read again.
  // When a provider thread last had nothing to read (no file, or no anchor), so the poll does not
  // look at it every tick.
  const quietSince = new Map<ProviderThreadId, number>();
  const lastRead = new Map<
    ProviderThreadId,
    { readonly anchor: string | null; readonly signature: AgentSessionScanner.TranscriptSignature }
  >();

  const newestItemNativeId = (items: ReadonlyArray<OrchestrationV2TurnItem>): string | undefined =>
    items.findLast((item) => item.nativeItemRef?.nativeId != null)?.nativeItemRef?.nativeId ??
    undefined;

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
      if (run.providerThreadId === null || !sessionThreadIds.has(run.providerThreadId)) continue;
      if (run.recordedFrom === "transcript") {
        const { turnItems } = yield* orchestrator.getThreadRecords(threadId, ["turnItems"], {
          turnItemRunIds: [run.id],
        });
        return newestItemNativeId(turnItems) ?? null;
      }
      const providerTurn = Orchestrator.providerTurnForRun(records, run);
      if (providerTurn === undefined) continue;
      // A turn that never ended, or ended with no record uuid (it failed before Claude wrote
      // anything), may sit beside output of its own, so nothing after it can be told apart.
      return providerTurnEnded(providerTurn) ? claudeTurnCursor(providerTurn) : null;
    }
    const { turnItems } = yield* orchestrator.getThreadRecords(threadId, ["turnItems"], {
      turnItemRunIds: [null],
    });
    return newestItemNativeId(turnItems) ?? null;
  });

  // A sync that stops before reading still notes the file, so the poll leaves the thread alone
  // until the file changes and it takes no cold-read slot meanwhile.
  const rememberFile = Effect.fn("TranscriptSyncService.rememberFile")(function* (
    providerThread: OrchestrationV2ProviderThread,
    providerSessionId: string,
    pollingRememberedPath: boolean,
  ) {
    const found = yield* scanner.locateTranscript({
      driver: providerThread.driver,
      providerInstanceId: providerThread.providerInstanceId,
      providerSessionId,
      ...(pollingRememberedPath ? { useRememberedPath: true } : {}),
    });
    if (Option.isSome(found)) {
      lastRead.set(providerThread.id, { anchor: null, signature: found.value });
    }
  });

  const syncOnce = Effect.fn("TranscriptSyncService.sync")(function* (
    threadId: ThreadId,
    pollingRememberedPath: boolean,
    coldBudget?: { remaining: number },
  ) {
    yield* Effect.annotateCurrentSpan({ "orchestration_v2.thread_id": threadId });
    // Provider threads are few, so they are read first to rule out a thread with nothing to read
    // before loading its run history.
    const head = yield* orchestrator.getThreadRecords(threadId, ["providerThreads"]);
    const headProviderThread = head.providerThreads.find(
      (candidate) => candidate.id === head.thread.activeProviderThreadId,
    );
    if (
      head.thread.deletedAt !== null ||
      headProviderThread === undefined ||
      headProviderThread.nativeThreadRef?.strength !== "strong" ||
      headProviderThread.nativeThreadRef.nativeId === null ||
      !scanner.supportsTranscriptSync(headProviderThread.driver)
    ) {
      return 0;
    }
    // A batch reads only so many sessions it has not read before; the poll picks up the rest.
    if (coldBudget !== undefined && !lastRead.has(headProviderThread.id)) {
      if (coldBudget.remaining <= 0) return 0;
      coldBudget.remaining -= 1;
    }
    const records = yield* orchestrator.getThreadRecords(threadId, [
      "runs",
      "attempts",
      "providerThreads",
      "providerTurns",
    ]);
    const { thread, runs } = records;
    const providerThread = records.providerThreads.find(
      (candidate) => candidate.id === thread.activeProviderThreadId,
    );
    const nativeSessionId = providerThread?.nativeThreadRef?.nativeId;
    if (providerThread === undefined || nativeSessionId == null) return 0;
    if (!runs.every(isTerminal)) {
      yield* rememberFile(providerThread, nativeSessionId, pollingRememberedPath);
      return 0;
    }
    // A rollback resumes the next T3 turn at its head, which leaves out what the terminal added.
    if (providerThread.nativeConversationHeadRef !== null) {
      yield* rememberFile(providerThread, nativeSessionId, pollingRememberedPath);
      quietSince.set(providerThread.id, yield* Clock.currentTimeMillis);
      return 0;
    }

    const anchorNativeId = yield* anchorFor(threadId, records, providerThread);
    if (anchorNativeId === null) {
      yield* Effect.logDebug("Transcript sync has no anchor", { threadId });
      quietSince.set(providerThread.id, yield* Clock.currentTimeMillis);
      return 0;
    }

    const previous = lastRead.get(providerThread.id);
    const read = yield* scanner.readTurnsAfter({
      driver: providerThread.driver,
      providerInstanceId: providerThread.providerInstanceId,
      providerSessionId: nativeSessionId,
      anchorNativeId,
      ...(previous?.anchor === anchorNativeId ? { unchangedSince: previous.signature } : {}),
      ...(pollingRememberedPath ? { useRememberedPath: true } : {}),
    });
    if (read._tag === "NoTranscript") {
      quietSince.set(providerThread.id, yield* Clock.currentTimeMillis);
    } else {
      quietSince.delete(providerThread.id);
    }
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
  const syncLocked = (
    threadId: ThreadId,
    pollingRememberedPath: boolean,
    coldBudget?: { remaining: number },
  ) =>
    locks
      .withLock(threadId, syncOnce(threadId, pollingRememberedPath, coldBudget))
      .pipe(
        Effect.mapError(
          (cause) => new OrchestrationV2SyncTranscriptTurnsError({ threadId, cause }),
        ),
      );

  const sync: TranscriptSyncService["Service"]["sync"] = (threadId) => syncLocked(threadId, false);

  const syncInBackground: TranscriptSyncService["Service"]["syncInBackground"] = (threadId) =>
    sync(threadId).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Transcript sync failed", { threadId, cause: error.cause }),
      ),
      Effect.asVoid,
      Effect.forkDetach({ startImmediately: true }),
      Effect.asVoid,
    );

  const syncMany: TranscriptSyncService["Service"]["syncMany"] = (threadIds) => {
    const coldBudget = { remaining: TRANSCRIPT_SYNC_COLD_READS_PER_TICK };
    return Effect.forEach(
      new Set(threadIds),
      (threadId) =>
        syncLocked(threadId, false, coldBudget).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Transcript sync failed", { threadId, cause: error.cause }).pipe(
              Effect.as(0),
            ),
          ),
        ),
      { concurrency: SYNC_MANY_CONCURRENCY },
    ).pipe(Effect.map((counts) => counts.reduce((total, count) => total + count, 0)));
  };

  type PollVerdict = "skip" | "changed" | "cold";

  // Looks at the file only; the projection is not read for a session that has not changed.
  const pollVerdict = Effect.fn("TranscriptSyncService.pollVerdict")(function* (
    candidate: TranscriptSyncPollCandidate,
  ) {
    const missingSince = quietSince.get(candidate.providerThreadId);
    if (
      missingSince !== undefined &&
      (yield* Clock.currentTimeMillis) - missingSince <
        Duration.toMillis(TRANSCRIPT_SYNC_NO_TRANSCRIPT_RECHECK)
    ) {
      return "skip" as PollVerdict;
    }
    const previous = lastRead.get(candidate.providerThreadId);
    if (previous === undefined) return "cold" as PollVerdict;
    const current = yield* scanner.peekTranscriptSignature({
      driver: candidate.driver,
      providerInstanceId: candidate.providerInstanceId,
      providerSessionId: candidate.nativeId,
    });
    if (Option.isNone(current)) return "cold" as PollVerdict;
    return (
      current.value.size === previous.signature.size &&
      current.value.mtimeMs === previous.signature.mtimeMs
        ? "skip"
        : "changed"
    ) as PollVerdict;
  });

  const pollMany: TranscriptSyncService["Service"]["pollMany"] = Effect.fn(
    "TranscriptSyncService.pollMany",
  )(function* (candidates) {
    const verdicts = yield* Effect.forEach(
      candidates,
      (candidate) =>
        pollVerdict(candidate).pipe(
          Effect.orElseSucceed(() => "cold" as PollVerdict),
          Effect.map((verdict) => ({ candidate, verdict })),
        ),
      { concurrency: SYNC_MANY_CONCURRENCY },
    );
    let coldReads = 0;
    const due = verdicts.filter(({ verdict }) => {
      if (verdict === "changed") return true;
      return verdict === "cold" && coldReads++ < TRANSCRIPT_SYNC_COLD_READS_PER_TICK;
    });
    const counts = yield* Effect.forEach(
      due,
      ({ candidate }) =>
        syncLocked(candidate.threadId, true).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Transcript sync failed", {
              threadId: candidate.threadId,
              cause: error.cause,
            }).pipe(Effect.as(0)),
          ),
        ),
      { concurrency: SYNC_MANY_CONCURRENCY },
    );
    return counts.reduce((total, count) => total + count, 0);
  });

  return TranscriptSyncService.of({ sync, syncMany, pollMany, syncInBackground });
});

export const layer = Layer.effect(TranscriptSyncService, make);
