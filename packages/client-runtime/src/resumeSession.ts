import type { AgentSessionSource, AgentSessionSummary, ThreadId } from "@t3tools/contracts";
import { mFallback, msg } from "gt-i18n";
import type { MFunctionType } from "gt-i18n/types";

import { thrownErrorMessage } from "./errors/translatable.ts";

/** What choosing the row does: import it, or open the thread already holding it. */
export type ResumeSessionRowState = "resume" | "open";

export interface ResumeSessionRow {
  readonly key: string;
  readonly session: AgentSessionSummary;
  readonly title: string;
  readonly providerLabel: string;
  readonly lastActiveAt: string | null;
  readonly branch: string | null;
  readonly state: ResumeSessionRowState;
  /** The thread an `open` row leads to. */
  readonly threadId: ThreadId | null;
}

const PROVIDER_LABELS: Readonly<Record<AgentSessionSource, string>> = {
  claudeAgent: "Claude Code",
  codex: "Codex",
  pi: "Pi",
};

function rowState(session: AgentSessionSummary): ResumeSessionRowState {
  return session.threadId === null ? "resume" : "open";
}

/** Rows keep the server's order, which is newest first. */
export function buildResumeSessionRows(
  sessions: ReadonlyArray<AgentSessionSummary>,
): ReadonlyArray<ResumeSessionRow> {
  return sessions.map((session) => ({
    key: `${session.providerInstanceId}:${session.providerSessionId}`,
    session,
    title: session.title,
    providerLabel: PROVIDER_LABELS[session.provider],
    lastActiveAt: session.lastActiveAt,
    branch: session.worktree?.branch ?? null,
    state: rowState(session),
    threadId: session.threadId,
  }));
}

/** Every word of the search must appear in the title, provider label or branch, in any case. */
export function filterResumeSessionRows(
  rows: ReadonlyArray<ResumeSessionRow>,
  search: string,
): ReadonlyArray<ResumeSessionRow> {
  const words = search.toLowerCase().split(/\s+/).filter(Boolean);
  if (words.length === 0) return rows;
  return rows.filter((row) => {
    const haystack = `${row.title} ${row.providerLabel} ${row.branch ?? ""}`.toLowerCase();
    return words.every((word) => haystack.includes(word));
  });
}

/** A short reason for a failed `agentSessions.importOne`, in the active language. */
export function resumeSessionErrorMessage(
  error: unknown,
  resolve: MFunctionType = mFallback,
): string {
  switch ((error as { readonly _tag?: string } | null)?._tag) {
    case "AgentSessionNotFoundError":
      return resolve(msg("This session is no longer in the project."));
    case "AgentSessionUnreadableError":
      return resolve(msg("This session's history could not be read."));
    case "AgentSessionUnresumableSessionError":
      return resolve(msg("This session cannot be resumed."));
    default:
      return thrownErrorMessage(error, resolve) ?? resolve(msg("Could not resume the session."));
  }
}
