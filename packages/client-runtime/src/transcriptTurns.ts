import {
  SYNC_TRANSCRIPT_TURNS_MAX_THREADS,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";

/** A run T3 recorded from a provider's transcript, for a turn made outside T3. */
export function isTerminalRun(
  run: { readonly recordedFrom?: "transcript" | undefined } | undefined,
): boolean {
  return run?.recordedFrom === "transcript";
}

/** The run a message belongs to, so a row can ask whether it was made outside T3. */
export function findTerminalRun<
  Run extends { readonly id: string; readonly recordedFrom?: "transcript" | undefined },
>(runs: ReadonlyArray<Run>, runId: string | null): Run | undefined {
  return runId === null ? undefined : runs.find((run) => run.id === runId);
}

export interface TranscriptSyncThread {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
  readonly archivedAt: string | null;
  readonly activeProviderThreadId: string | null;
}

export interface TranscriptSyncBatch {
  readonly environmentId: EnvironmentId;
  readonly threadIds: ReadonlyArray<ThreadId>;
}

/**
 * The calls that bring a list of threads up to date with their sessions: each environment's
 * threads that are not archived and have a provider session, in list order, within the server's
 * limit per call. The server decides which of them it can read.
 */
export function planTranscriptSyncBatches(
  threads: ReadonlyArray<TranscriptSyncThread>,
): ReadonlyArray<TranscriptSyncBatch> {
  const idsByEnvironment = new Map<EnvironmentId, Set<ThreadId>>();
  for (const thread of threads) {
    if (thread.archivedAt !== null || thread.activeProviderThreadId === null) continue;
    const ids = idsByEnvironment.get(thread.environmentId) ?? new Set<ThreadId>();
    ids.add(thread.id);
    idsByEnvironment.set(thread.environmentId, ids);
  }
  return [...idsByEnvironment].flatMap(([environmentId, ids]) => {
    const threadIds = [...ids];
    return Array.from(
      { length: Math.ceil(threadIds.length / SYNC_TRANSCRIPT_TURNS_MAX_THREADS) },
      (_, index) => ({
        environmentId,
        threadIds: threadIds.slice(
          index * SYNC_TRANSCRIPT_TURNS_MAX_THREADS,
          (index + 1) * SYNC_TRANSCRIPT_TURNS_MAX_THREADS,
        ),
      }),
    );
  });
}

/** Sends each environment's threads to be synced, skipping environments the user cannot operate. */
export function syncTranscriptTurnsOnFocus(input: {
  readonly threads: ReadonlyArray<TranscriptSyncThread>;
  readonly canSync: (environmentId: EnvironmentId) => boolean;
  readonly send: (batch: TranscriptSyncBatch) => void;
}): void {
  for (const batch of planTranscriptSyncBatches(input.threads)) {
    if (input.canSync(batch.environmentId)) input.send(batch);
  }
}
