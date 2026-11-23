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
  Record<
    string,
    {
      readonly homeVariable: string;
      readonly command: string;
      /** The provider allows one writer per session, so T3 must let go before a terminal can resume. */
      readonly singleWriter: boolean;
    }
  >
> = {
  claudeAgent: {
    homeVariable: "CLAUDE_CONFIG_DIR",
    command: "claude --resume",
    singleWriter: false,
  },
  codex: { homeVariable: "CODEX_HOME", command: "codex resume", singleWriter: true },
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

/**
 * Runs a release for a key unless one is already in flight for it, so repeated clicks do not
 * dispatch repeated detaches. Resolves to undefined when skipped.
 */
export function createReleaseGuard() {
  const inFlight = new Set<string>();
  return async <T>(key: string, release: () => Promise<T>): Promise<T | undefined> => {
    if (inFlight.has(key)) return undefined;
    inFlight.add(key);
    try {
      return await release();
    } finally {
      inFlight.delete(key);
    }
  };
}

/** What a release attempt did: `detached` counts the detaches dispatched, `failure` is a refusal. */
export type ResumeReleaseAttempt = { readonly detached: number } | { readonly failure: unknown };

export function classifyResumeRelease(
  attempt: ResumeReleaseAttempt,
): "released" | "nothing" | "refused" {
  if ("failure" in attempt) return "refused";
  return attempt.detached > 0 ? "released" : "nothing";
}

export interface ThreadResume {
  readonly command: string;
  /** T3 holds the session and should be told to let go before the command is used. */
  readonly releaseFirst: boolean;
}

/** The resume command for the thread's active provider thread, and whether T3 must release it first. */
export function resolveThreadResume(input: {
  readonly activeProviderThreadId: string | null;
  readonly providerThreads: ReadonlyArray<
    Pick<OrchestrationV2ProviderThread, "id" | "providerInstanceId" | "nativeThreadRef">
  >;
  /** The thread's live provider sessions; a thread with none holds nothing. */
  readonly providerSessions: ReadonlyArray<unknown>;
  readonly providerInstances: Readonly<Record<string, ResumeCommandProviderInstance>>;
  readonly directory: string | null;
}): ThreadResume | null {
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
  const command = buildResumeCommand({
    driver: nativeThreadRef.driver,
    nativeId: nativeThreadRef.nativeId,
    directory: input.directory,
    homePath,
  });
  if (command === null) return null;
  return {
    command,
    releaseFirst:
      RESUME_COMMAND_BY_DRIVER[nativeThreadRef.driver]?.singleWriter === true &&
      input.providerSessions.length > 0,
  };
}
