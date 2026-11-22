import { describe, expect, it, vi } from "vite-plus/test";

import { EnvironmentId, SYNC_TRANSCRIPT_TURNS_MAX_THREADS, ThreadId } from "@t3tools/contracts";

import {
  findTerminalRun,
  isTerminalRun,
  planTranscriptSyncBatches,
  syncTranscriptTurnsOnFocus,
} from "./transcriptTurns.ts";

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

const local = EnvironmentId.make("env-local");
const remote = EnvironmentId.make("env-remote");
const thread = (
  environmentId: EnvironmentId,
  id: string,
  overrides: { archivedAt?: string | null; activeProviderThreadId?: string | null } = {},
) => ({
  environmentId,
  id: ThreadId.make(id),
  archivedAt: null,
  activeProviderThreadId: "provider-thread",
  ...overrides,
});

describe("planTranscriptSyncBatches", () => {
  it("groups the threads of each environment, keeping the list's order", () => {
    expect(
      planTranscriptSyncBatches([thread(local, "a"), thread(remote, "b"), thread(local, "c")]),
    ).toEqual([
      { environmentId: local, threadIds: ["a", "c"] },
      { environmentId: remote, threadIds: ["b"] },
    ]);
  });

  it("leaves out archived threads, threads with no provider session, and repeats", () => {
    expect(
      planTranscriptSyncBatches([
        thread(local, "a"),
        thread(local, "archived", { archivedAt: "2026-11-01T00:00:00.000Z" }),
        thread(local, "no-session", { activeProviderThreadId: null }),
        thread(local, "a"),
      ]),
    ).toEqual([{ environmentId: local, threadIds: ["a"] }]);
  });

  it("splits an environment's threads into calls the server accepts", () => {
    const threads = Array.from({ length: SYNC_TRANSCRIPT_TURNS_MAX_THREADS + 1 }, (_, index) =>
      thread(local, `t-${index}`),
    );
    const batches = planTranscriptSyncBatches(threads);
    expect(batches.map((batch) => batch.threadIds.length)).toEqual([
      SYNC_TRANSCRIPT_TURNS_MAX_THREADS,
      1,
    ]);
  });

  it("plans nothing for an empty list", () => {
    expect(planTranscriptSyncBatches([])).toEqual([]);
  });
});

describe("syncTranscriptTurnsOnFocus", () => {
  it("sends each environment's sidebar threads", async () => {
    const send = vi.fn(async () => {});
    await syncTranscriptTurnsOnFocus({
      threads: [thread(local, "a"), thread(remote, "b")],
      canSync: () => true,
      send,
    });
    expect(send.mock.calls).toEqual([
      [{ environmentId: local, threadIds: ["a"] }],
      [{ environmentId: remote, threadIds: ["b"] }],
    ]);
  });

  it("sends nothing to an environment the user cannot operate", async () => {
    const send = vi.fn(async () => {});
    await syncTranscriptTurnsOnFocus({
      threads: [thread(local, "a"), thread(remote, "b")],
      canSync: (environmentId) => environmentId === remote,
      send,
    });
    expect(send.mock.calls).toEqual([[{ environmentId: remote, threadIds: ["b"] }]]);
  });

  it("sends an environment's next chunk only after the previous one is answered", async () => {
    const threads = Array.from({ length: SYNC_TRANSCRIPT_TURNS_MAX_THREADS + 1 }, (_, index) =>
      thread(local, `t-${index}`),
    );
    const answers: Array<() => void> = [];
    const send = vi.fn(() => new Promise<void>((resolve) => answers.push(resolve)));

    const done = syncTranscriptTurnsOnFocus({ threads, canSync: () => true, send });
    expect(send).toHaveBeenCalledTimes(1);

    answers[0]!();
    await vi.waitFor(() => expect(send).toHaveBeenCalledTimes(2));
    answers[1]!();
    await done;
  });
});
