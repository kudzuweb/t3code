import { describe, expect, it } from "vite-plus/test";

import { buildResumeCommand, resolveThreadResumeCommand } from "./resumeCommand.ts";

describe("buildResumeCommand", () => {
  it.each([
    ["claudeAgent", "cd '/work/app' && claude --resume abc-123"],
    ["codex", "cd '/work/app' && codex resume abc-123"],
  ])("builds the %s command", (driver, expected) => {
    expect(buildResumeCommand({ driver, nativeId: "abc-123", directory: "/work/app" })).toBe(
      expected,
    );
  });

  it("escapes single quotes in the directory", () => {
    expect(
      buildResumeCommand({
        driver: "claudeAgent",
        nativeId: "id",
        directory: "/Users/o'brien/app",
      }),
    ).toBe(`cd '/Users/o'\\''brien/app' && claude --resume id`);
  });

  it("returns null for other drivers and for a missing native id", () => {
    expect(buildResumeCommand({ driver: "cursor", nativeId: "id", directory: "/a" })).toBeNull();
    expect(
      buildResumeCommand({ driver: "claudeAgent", nativeId: null, directory: "/a" }),
    ).toBeNull();
  });
});

describe("resolveThreadResumeCommand", () => {
  const providerThreads = [
    { id: "pt-1", nativeThreadRef: { driver: "claudeAgent", nativeId: "s-1", strength: "strong" } },
    { id: "pt-2", nativeThreadRef: { driver: "claudeAgent", nativeId: "s-2", strength: "none" } },
  ] as never;

  it("uses the active provider thread", () => {
    expect(
      resolveThreadResumeCommand({
        activeProviderThreadId: "pt-1",
        providerThreads,
        directory: "/a",
      }),
    ).toBe("cd '/a' && claude --resume s-1");
  });

  it("returns null without an active thread, a usable ref, or a directory", () => {
    expect(
      resolveThreadResumeCommand({
        activeProviderThreadId: null,
        providerThreads,
        directory: "/a",
      }),
    ).toBeNull();
    expect(
      resolveThreadResumeCommand({
        activeProviderThreadId: "pt-2",
        providerThreads,
        directory: "/a",
      }),
    ).toBeNull();
    expect(
      resolveThreadResumeCommand({
        activeProviderThreadId: "pt-1",
        providerThreads,
        directory: null,
      }),
    ).toBeNull();
  });
});
