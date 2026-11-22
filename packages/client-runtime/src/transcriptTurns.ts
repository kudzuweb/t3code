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

/** Whether the thread's session can gain turns in a terminal that T3 knows how to read. */
export function shouldSyncTranscriptTurns(input: {
  readonly activeProviderThreadId: string | null;
  readonly providerThreads: ReadonlyArray<{ readonly id: string; readonly driver: string }>;
}): boolean {
  return (
    input.providerThreads.find((thread) => thread.id === input.activeProviderThreadId)?.driver ===
    "claudeAgent"
  );
}
