import { describe, expect, it } from "vite-plus/test";

import { buildResumeCommand, resolveThreadResume } from "./resumeCommand.ts";

describe("buildResumeCommand", () => {
  it.each([
    ["claudeAgent", "cd '/work/app' && claude --resume 'abc-123'"],
    ["codex", "cd '/work/app' && codex resume 'abc-123'"],
  ])("builds the %s command", (driver, expected) => {
    expect(buildResumeCommand({ driver, nativeId: "abc-123", directory: "/work/app" })).toBe(
      expected,
    );
  });

  it.each([
    [
      "claudeAgent",
      "/homes/work claude",
      "cd '/a' && CLAUDE_CONFIG_DIR='/homes/work claude' claude --resume 'id'",
    ],
    ["codex", "/homes/o'x", "cd '/a' && CODEX_HOME='/homes/o'\\''x' codex resume 'id'"],
    ["codex", "~/.codex-work", "cd '/a' && CODEX_HOME=~/'.codex-work' codex resume 'id'"],
  ])("prefixes a custom %s home", (driver, homePath, expected) => {
    expect(buildResumeCommand({ driver, nativeId: "id", directory: "/a", homePath })).toBe(
      expected,
    );
  });

  it("escapes single quotes in the directory and the id", () => {
    expect(
      buildResumeCommand({
        driver: "claudeAgent",
        nativeId: "i'd",
        directory: "/Users/o'brien/app",
      }),
    ).toBe(`cd '/Users/o'\\''brien/app' && claude --resume 'i'\\''d'`);
  });

  it("returns null for other drivers and for a missing native id", () => {
    expect(buildResumeCommand({ driver: "cursor", nativeId: "id", directory: "/a" })).toBeNull();
    expect(
      buildResumeCommand({ driver: "claudeAgent", nativeId: null, directory: "/a" }),
    ).toBeNull();
  });
});

describe("resolveThreadResume", () => {
  const ref = (driver: string, nativeId: string, strength: string) => ({
    driver,
    nativeId,
    strength,
  });
  const providerThreads = [
    {
      id: "pt-1",
      providerInstanceId: "claudeAgent",
      nativeThreadRef: ref("claudeAgent", "s-1", "strong"),
    },
    {
      id: "pt-2",
      providerInstanceId: "claudeAgent",
      nativeThreadRef: ref("claudeAgent", "s-2", "weak"),
    },
    {
      id: "pt-3",
      providerInstanceId: "codex_work",
      nativeThreadRef: ref("codex", "s-3", "strong"),
    },
    {
      id: "pt-4",
      providerInstanceId: "claude_work",
      nativeThreadRef: ref("claudeAgent", "s-4", "strong"),
    },
  ] as never;
  const resolveWithSessions = (
    activeProviderThreadId: string | null,
    providerSessions: ReadonlyArray<unknown>,
    providerInstances = {},
    directory: string | null = "/a",
  ) =>
    resolveThreadResume({
      activeProviderThreadId,
      providerThreads,
      providerSessions,
      providerInstances,
      directory,
    });
  const resolve = (
    activeProviderThreadId: string | null,
    providerInstances = {},
    directory: string | null = "/a",
  ) =>
    resolveWithSessions(activeProviderThreadId, [], providerInstances, directory)?.command ?? null;

  it("uses the active provider thread with the default home", () => {
    expect(resolve("pt-1")).toBe("cd '/a' && claude --resume 's-1'");
  });

  it("prefixes the instance's configured home", () => {
    expect(resolve("pt-3", { codex_work: { config: { homePath: "/h/codex" } } })).toBe(
      "cd '/a' && CODEX_HOME='/h/codex' codex resume 's-3'",
    );
    expect(
      resolve("pt-4", {
        claude_work: {
          environment: [{ name: "CLAUDE_CONFIG_DIR", value: "/h/c", sensitive: false }],
        },
      }),
    ).toBe("cd '/a' && CLAUDE_CONFIG_DIR='/h/c' claude --resume 's-4'");
  });

  it("returns null when the instance's home is a secret", () => {
    expect(
      resolve("pt-4", {
        claude_work: { environment: [{ name: "CLAUDE_CONFIG_DIR", value: {}, sensitive: true }] },
      }),
    ).toBeNull();
  });

  it("returns null without an active thread, a strong ref, or a directory", () => {
    expect(resolve(null)).toBeNull();
    expect(resolve("pt-2")).toBeNull();
    expect(resolve("pt-1", {}, null)).toBeNull();
  });

  it("asks to release T3's hold only for a Codex thread with a live session", () => {
    const liveSession = [{ id: "ps-1" }];
    expect(resolveWithSessions("pt-3", liveSession)?.releaseFirst).toBe(true);
    expect(resolveWithSessions("pt-3", [])?.releaseFirst).toBe(false);
    expect(resolveWithSessions("pt-1", liveSession)?.releaseFirst).toBe(false);
  });
});
