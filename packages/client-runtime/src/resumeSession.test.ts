import { ProviderInstanceId, ThreadId, type AgentSessionSummary } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  buildResumeSessionRows,
  filterResumeSessionRows,
  resumeSessionErrorMessage,
} from "./resumeSession.ts";

const session = (
  overrides: Partial<AgentSessionSummary> & { id: string },
): AgentSessionSummary => ({
  provider: "claudeAgent",
  providerInstanceId: ProviderInstanceId.make("claudeAgent"),
  providerSessionId: overrides.id,
  title: `Session ${overrides.id}`,
  lastActiveAt: null,
  worktree: null,
  threadId: null,
  ...overrides,
});

describe("buildResumeSessionRows", () => {
  it("keeps the server order and labels each row's state", () => {
    const rows = buildResumeSessionRows([
      session({ id: "new" }),
      session({ id: "held", threadId: ThreadId.make("thread-1") }),
    ]);
    expect(rows.map((row) => [row.session.providerSessionId, row.state])).toEqual([
      ["new", "resume"],
      ["held", "open"],
    ]);
    expect(rows[1]?.threadId).toBe("thread-1");
  });

  it("takes the provider label and branch from the session", () => {
    const [row] = buildResumeSessionRows([
      session({
        id: "a",
        provider: "codex",
        worktree: { path: "/work/a", branch: "feature/login" },
      }),
    ]);
    expect(row).toMatchObject({ providerLabel: "Codex", branch: "feature/login" });
  });
});

describe("filterResumeSessionRows", () => {
  const rows = buildResumeSessionRows([
    session({ id: "1", title: "Fix the Login bug", worktree: { path: "/w", branch: "main" } }),
    session({ id: "2", title: "Write docs", provider: "pi" }),
  ]);

  it("matches title, provider label and branch regardless of case, in server order", () => {
    expect(filterResumeSessionRows(rows, "login").map((row) => row.key)).toHaveLength(1);
    expect(filterResumeSessionRows(rows, "PI")[0]?.session.providerSessionId).toBe("2");
    expect(filterResumeSessionRows(rows, "main")[0]?.session.providerSessionId).toBe("1");
  });

  it("requires every word and returns everything for a blank search", () => {
    expect(filterResumeSessionRows(rows, "fix docs")).toEqual([]);
    expect(filterResumeSessionRows(rows, "  ")).toBe(rows);
  });
});

describe("resumeSessionErrorMessage", () => {
  it("names an unresumable session and falls back for unknown failures", () => {
    expect(resumeSessionErrorMessage({ _tag: "AgentSessionUnresumableSessionError" })).toBe(
      "This session cannot be resumed.",
    );
    expect(resumeSessionErrorMessage("boom")).toBe("Could not resume the session.");
  });
});
