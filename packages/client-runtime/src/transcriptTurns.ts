import type { OrchestrationV2ProviderThread, OrchestrationV2Run } from "@t3tools/contracts";

/** A run T3 recorded from a provider's transcript, for a turn made outside T3. */
export function isTerminalRun(run: Pick<OrchestrationV2Run, "recordedFrom"> | undefined): boolean {
  return run?.recordedFrom === "transcript";
}

/** The run a message belongs to, so a row can ask whether it was made outside T3. */
export function findTerminalRun<Run extends Pick<OrchestrationV2Run, "id" | "recordedFrom">>(
  runs: ReadonlyArray<Run>,
  runId: string | null,
): Run | undefined {
  return runId === null ? undefined : runs.find((run) => run.id === runId);
}

/** Whether the thread's session can gain turns in a terminal that T3 knows how to read. */
export function shouldSyncTranscriptTurns(input: {
  readonly activeProviderThreadId: string | null;
  readonly providerThreads: ReadonlyArray<Pick<OrchestrationV2ProviderThread, "id" | "driver">>;
}): boolean {
  return (
    input.providerThreads.find((thread) => thread.id === input.activeProviderThreadId)?.driver ===
    "claudeAgent"
  );
}
