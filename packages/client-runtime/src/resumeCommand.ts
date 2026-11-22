import type { OrchestrationV2ProviderThread } from "@t3tools/contracts";

/** What a provider instance is configured with, as far as its home directory goes. */
export interface ResumeCommandProviderInstance {
  readonly config?: unknown;
  readonly environment?: ReadonlyArray<{
    readonly name: string;
    readonly value: unknown;
    readonly sensitive: boolean;
  }>;
}

const RESUME_COMMAND_BY_DRIVER: Readonly<
  Record<string, { readonly homeVariable: string; readonly command: string }>
> = {
  claudeAgent: { homeVariable: "CLAUDE_CONFIG_DIR", command: "claude --resume" },
  codex: { homeVariable: "CODEX_HOME", command: "codex resume" },
};

/** Wraps a value in single quotes for a POSIX shell, escaping embedded single quotes. */
function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

/** Quotes a path but leaves a leading `~/` unquoted, since the server expands it and a shell would not. */
function shellQuotePath(value: string): string {
  if (value === "~") return "~";
  return value.startsWith("~/") ? `~/${shellQuote(value.slice(2))}` : shellQuote(value);
}

/**
 * The home directory an instance overrides, null when it uses the provider's default, or
 * "unknown" when the override is a secret the client cannot read.
 */
function resolveInstanceHome(
  homeVariable: string,
  instance: ResumeCommandProviderInstance | undefined,
): string | null | "unknown" {
  const configured =
    typeof instance?.config === "object" && instance.config !== null
      ? (instance.config as { homePath?: unknown }).homePath
      : undefined;
  if (typeof configured === "string" && configured.trim() !== "") return configured.trim();
  const variable = instance?.environment?.find((entry) => entry.name === homeVariable);
  if (variable === undefined) return null;
  if (variable.sensitive || typeof variable.value !== "string") return "unknown";
  return variable.value.trim() === "" ? null : variable.value.trim();
}

/**
 * The shell command that continues a provider thread's native session in a terminal, or null when
 * the driver has no known resume command or the thread has no native session id.
 */
export function buildResumeCommand(input: {
  readonly driver: string;
  readonly nativeId: string | null;
  readonly directory: string;
  /** The instance's custom home directory, or null for the provider's default. */
  readonly homePath?: string | null;
}): string | null {
  const resume = RESUME_COMMAND_BY_DRIVER[input.driver];
  if (resume === undefined || input.nativeId === null || input.directory === "") return null;
  const homePrefix = input.homePath
    ? `${resume.homeVariable}=${shellQuotePath(input.homePath)} `
    : "";
  return `cd ${shellQuote(input.directory)} && ${homePrefix}${resume.command} ${shellQuote(input.nativeId)}`;
}

/** The resume command for the thread's active provider thread. */
export function resolveThreadResumeCommand(input: {
  readonly activeProviderThreadId: string | null;
  readonly providerThreads: ReadonlyArray<
    Pick<OrchestrationV2ProviderThread, "id" | "providerInstanceId" | "nativeThreadRef">
  >;
  readonly providerInstances: Readonly<Record<string, ResumeCommandProviderInstance>>;
  readonly directory: string | null;
}): string | null {
  const providerThread = input.providerThreads.find(
    (thread) => thread.id === input.activeProviderThreadId,
  );
  const nativeThreadRef = providerThread?.nativeThreadRef;
  if (!providerThread || !nativeThreadRef || nativeThreadRef.strength !== "strong") return null;
  if (input.directory === null) return null;
  const homeVariable = RESUME_COMMAND_BY_DRIVER[nativeThreadRef.driver]?.homeVariable;
  if (homeVariable === undefined) return null;
  const instance = Object.hasOwn(input.providerInstances, providerThread.providerInstanceId)
    ? input.providerInstances[providerThread.providerInstanceId]
    : undefined;
  const homePath = resolveInstanceHome(homeVariable, instance);
  if (homePath === "unknown") return null;
  return buildResumeCommand({
    driver: nativeThreadRef.driver,
    nativeId: nativeThreadRef.nativeId,
    directory: input.directory,
    homePath,
  });
}
