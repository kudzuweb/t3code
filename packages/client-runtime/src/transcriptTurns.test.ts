import { describe, expect, it } from "vite-plus/test";

import { findTerminalRun, isTerminalRun, shouldSyncTranscriptTurns } from "./transcriptTurns.ts";

describe("isTerminalRun", () => {
  it("is true for a run recorded from a transcript", () => {
    expect(isTerminalRun({ recordedFrom: "transcript" })).toBe(true);
  });

  it("is false for a run T3 made, and for no run", () => {
    expect(isTerminalRun({})).toBe(false);
    expect(isTerminalRun(undefined)).toBe(false);
  });
});

describe("findTerminalRun", () => {
  const runs = [{ id: "run-1" }, { id: "run-2", recordedFrom: "transcript" as const }];

  it("returns the run a message belongs to", () => {
    expect(findTerminalRun(runs, "run-2")).toBe(runs[1]);
  });

  it("returns undefined for a message with no run or an unknown run", () => {
    expect(findTerminalRun(runs, null)).toBeUndefined();
    expect(findTerminalRun(runs, "run-9")).toBeUndefined();
  });
});

describe("shouldSyncTranscriptTurns", () => {
  const providerThreads = [
    { id: "pt-claude", driver: "claudeAgent" },
    { id: "pt-codex", driver: "codex" },
  ];

  it("syncs a thread whose active provider thread is Claude", () => {
    expect(
      shouldSyncTranscriptTurns({ activeProviderThreadId: "pt-claude", providerThreads }),
    ).toBe(true);
  });

  it("skips a thread whose active provider thread is another driver", () => {
    expect(shouldSyncTranscriptTurns({ activeProviderThreadId: "pt-codex", providerThreads })).toBe(
      false,
    );
  });

  it("skips a thread with no active provider thread", () => {
    expect(shouldSyncTranscriptTurns({ activeProviderThreadId: null, providerThreads })).toBe(
      false,
    );
    expect(shouldSyncTranscriptTurns({ activeProviderThreadId: "pt-gone", providerThreads })).toBe(
      false,
    );
  });
});
