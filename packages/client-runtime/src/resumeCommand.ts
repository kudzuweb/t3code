import type { OrchestrationV2ProviderThread } from "@t3tools/contracts";

const RESUME_COMMAND_BY_DRIVER: Readonly<Record<string, (nativeId: string) => string>> = {
  claudeAgent: (nativeId) => `claude --resume ${nativeId}`,
  codex: (nativeId) => `codex resume ${nativeId}`,
};

/** Wraps a value in single quotes for a POSIX shell, escaping embedded single quotes. */
function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

/**
 * The shell command that continues a provider thread's native session in a terminal, or null when
 * the driver has no known resume command or the thread has no native session id.
 */
export function buildResumeCommand(input: {
  readonly driver: string;
  readonly nativeId: string | null;
  readonly directory: string;
}): string | null {
  const resume = RESUME_COMMAND_BY_DRIVER[input.driver];
  if (resume === undefined || input.nativeId === null || input.directory === "") return null;
  return `cd ${shellQuote(input.directory)} && ${resume(input.nativeId)}`;
}

/** The resume command for the thread's active provider thread. */
export function resolveThreadResumeCommand(input: {
  readonly activeProviderThreadId: string | null;
  readonly providerThreads: ReadonlyArray<
    Pick<OrchestrationV2ProviderThread, "id" | "nativeThreadRef">
  >;
  readonly directory: string | null;
}): string | null {
  const nativeThreadRef = input.providerThreads.find(
    (thread) => thread.id === input.activeProviderThreadId,
  )?.nativeThreadRef;
  if (!nativeThreadRef || nativeThreadRef.strength === "none" || input.directory === null) {
    return null;
  }
  return buildResumeCommand({
    driver: nativeThreadRef.driver,
    nativeId: nativeThreadRef.nativeId,
    directory: input.directory,
  });
}
