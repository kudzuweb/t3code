import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import type { LidSleepGuardMode, LidSleepGuardStatus } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProcessRunner from "../processRunner.ts";
import * as LidSleepGuard from "./LidSleepGuard.ts";

const SET_ON = "/usr/bin/sudo -n /usr/bin/pmset -a disablesleep 1";
const SET_OFF = "/usr/bin/sudo -n /usr/bin/pmset -a disablesleep 0";
const PMSET_READ = "/usr/bin/pmset -g";

interface FakeMac {
  sleepDisabled: boolean;
  sudo: "allowed" | "no-rule";
  failRelease: boolean;
  /** The next `pmset -g` fails once; armed by a successful set. */
  failReadAfterSet: boolean;
  failNextRead: boolean;
  /** Another tool turns the block on while T3's own `disablesleep 1` fails. */
  externalTakesOverOnFailedSet: boolean;
  readonly commands: Array<string>;
  readonly log: Queue.Queue<string>;
}

const output = (code: number, stdout = "", stderr = "") => ({
  stdout,
  stderr,
  code: ChildProcessSpawner.ExitCode(code),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

const fakeRunner = (mac: FakeMac) =>
  ProcessRunner.ProcessRunner.of({
    run: ({ command, args }) =>
      Effect.sync(() => {
        const line = [command, ...args].join(" ");
        mac.commands.push(line);
        Queue.offerUnsafe(mac.log, line);
        if (line === "/usr/bin/pmset -g") {
          if (mac.failNextRead) {
            mac.failNextRead = false;
            return output(1, "", "pmset: read failed\n");
          }
          return output(0, ` SleepDisabled\t\t${mac.sleepDisabled ? 1 : 0}\n sleep 1\n`);
        }
        if (line === SET_ON || line === SET_OFF) {
          if (mac.sudo === "no-rule") return output(1, "", "sudo: a password is required\n");
          if (line === SET_ON && mac.externalTakesOverOnFailedSet) {
            mac.sleepDisabled = true;
            return output(1, "", "pmset: timed out\n");
          }
          if (line === SET_OFF && mac.failRelease) return output(1, "", "pmset: failed\n");
          mac.sleepDisabled = line === SET_ON;
          if (line === SET_ON && mac.failReadAfterSet) mac.failNextRead = true;
          return output(0);
        }
        return output(127, "", `unexpected command: ${line}`);
      }),
  });

const STALE_LEASE = JSON.stringify({ pid: 999, token: "old-token", home: "/home/t3" });
const SERVER_PID = 4242;
const START_TOKEN = "token-a";
const HOME = "/home/t3";

interface Spawned {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}

const harness = (options: {
  readonly initialMode?: LidSleepGuardMode;
  readonly sleepDisabled?: boolean;
  readonly staleLease?: boolean;
  readonly platform?: NodeJS.Platform;
  readonly initialDemand?: boolean;
  readonly sudo?: FakeMac["sudo"];
  readonly production?: boolean;
  readonly keepAwakeEnabled?: boolean;
  readonly failReadAfterSet?: boolean;
  readonly externalTakesOverOnFailedSet?: boolean;
}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-lid-sleep-guard-" });
    const leasePath = `${directory}/lid-sleep-guard.lease`;
    if (options.staleLease) yield* fileSystem.writeFileString(leasePath, STALE_LEASE);
    const mac: FakeMac = {
      sleepDisabled: options.sleepDisabled ?? false,
      sudo: options.sudo ?? "allowed",
      failRelease: false,
      failReadAfterSet: options.failReadAfterSet ?? false,
      failNextRead: false,
      externalTakesOverOnFailedSet: options.externalTakesOverOnFailedSet ?? false,
      commands: [],
      log: yield* Queue.unbounded<string>(),
    };
    const watchers: Array<Spawned> = [];
    const mode = yield* Queue.unbounded<LidSleepGuardMode>();
    const demand = yield* Queue.unbounded<boolean>();
    const keepAwakeEnabled = yield* Queue.unbounded<boolean>();
    if (options.initialMode !== undefined) yield* Queue.offer(mode, options.initialMode);
    if (options.initialDemand !== undefined) yield* Queue.offer(demand, options.initialDemand);
    yield* Queue.offer(keepAwakeEnabled, options.keepAwakeEnabled ?? true);
    const guardScope = yield* Scope.make();
    const guard = yield* LidSleepGuard.make({
      platform: options.platform ?? "darwin",
      production: options.production ?? true,
      mode: Stream.fromQueue(mode),
      demand: Stream.fromQueue(demand),
      keepAwakeEnabled: Stream.fromQueue(keepAwakeEnabled),
      leasePath,
      serverPid: SERVER_PID,
      startToken: START_TOKEN,
      home: HOME,
      spawnWatcher: (watcher) => Effect.sync(() => void watchers.push(watcher)),
    }).pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, fakeRunner(mac)),
      Scope.provide(guardScope),
    );
    const awaitStatus = (predicate: (status: LidSleepGuardStatus) => boolean) =>
      guard.status.pipe(Stream.filter(predicate), Stream.runHead, Effect.map(Option.getOrThrow));
    return {
      mac,
      mode,
      demand,
      keepAwakeEnabled,
      watchers,
      leasePath,
      awaitStatus,
      shutDown: Scope.close(guardScope, Exit.void),
      hasLease: fileSystem.exists(leasePath),
      readLease: fileSystem.readFileString(leasePath).pipe(Effect.map((text) => JSON.parse(text))),
    };
  }).pipe(Effect.provide(NodeServices.layer));

const sudoCommands = (mac: FakeMac) => mac.commands.filter((line) => line.includes("sudo"));
const countOf = (mac: FakeMac, line: string) =>
  mac.commands.filter((entry) => entry === line).length;

/** Waits on the command log, so a test never polls or sleeps on the real clock. */
const awaitCommand = (mac: FakeMac, line: string, count: number) =>
  Effect.gen(function* () {
    while (countOf(mac, line) < count) yield* Queue.take(mac.log);
  });

describe("LidSleepGuard", () => {
  it.effect("auto acquires when work starts", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "auto", initialDemand: false });
      yield* Queue.offer(t.demand, true);
      yield* t.awaitStatus((status) => status.owned);
      expect(sudoCommands(t.mac)).toEqual([SET_ON]);
      expect(yield* t.hasLease).toBe(true);
    }),
  );

  it.effect("auto releases after KeepAwake's grace when work ends", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "auto", initialDemand: true });
      yield* t.awaitStatus((status) => status.owned);
      yield* Queue.offer(t.demand, false);
      yield* TestClock.adjust("59 seconds");
      expect(sudoCommands(t.mac)).toEqual([SET_ON]);
      yield* TestClock.adjust("1 second");
      yield* t.awaitStatus((status) => !status.owned);
      expect(sudoCommands(t.mac)).toEqual([SET_ON, SET_OFF]);
      expect(yield* t.hasLease).toBe(false);
    }),
  );

  it.effect("leaves an external block alone and never owns it", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "on", sleepDisabled: true });
      yield* t.awaitStatus((status) => status.blocking && !status.owned);
      expect(yield* t.hasLease).toBe(false);
      expect(sudoCommands(t.mac)).toEqual([]);
    }),
  );

  it.effect("does nothing on release when it does not own the block", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "on", sleepDisabled: true });
      yield* t.awaitStatus((status) => status.blocking);
      yield* Queue.offer(t.mode, "off");
      // Reads from the startup check, the acquire attempt and the release attempt; none runs sudo.
      yield* awaitCommand(t.mac, PMSET_READ, 3);
      expect(sudoCommands(t.mac)).toEqual([]);
      expect(t.mac.sleepDisabled).toBe(true);
    }),
  );

  it.effect("keeps the lease when the release fails", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "on" });
      yield* t.awaitStatus((status) => status.owned);
      t.mac.failRelease = true;
      yield* Queue.offer(t.mode, "off");
      yield* t.awaitStatus((status) => status.error === "failed");
      expect(yield* t.hasLease).toBe(true);
      expect(t.mac.sleepDisabled).toBe(true);
    }),
  );

  it.effect("releases a stale lease at startup when the mode is off", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "off", sleepDisabled: true, staleLease: true });
      // The read after the release comes from the off mode, so the lease is already gone.
      yield* awaitCommand(t.mac, PMSET_READ, 2);
      expect(sudoCommands(t.mac)).toEqual([SET_OFF]);
      expect(yield* t.hasLease).toBe(false);
    }),
  );

  it.effect("releases a stale lease at startup when auto has no work", () =>
    Effect.gen(function* () {
      const t = yield* harness({
        initialMode: "auto",
        initialDemand: false,
        sleepDisabled: true,
        staleLease: true,
      });
      yield* awaitCommand(t.mac, SET_OFF, 1);
      expect(sudoCommands(t.mac)).toEqual([SET_OFF]);
    }),
  );

  it.effect("reports a missing sudoers rule and keeps running", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "on", sudo: "no-rule" });
      yield* t.awaitStatus((status) => status.error === "needs-sudoers-rule");
      expect(yield* t.hasLease).toBe(false);
      t.mac.sudo = "allowed";
      yield* TestClock.adjust("5 seconds");
      yield* t.awaitStatus((status) => status.owned && status.error === undefined);
    }),
  );

  it.effect("holds in mode on and releases when switched to off", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "on" });
      yield* t.awaitStatus((status) => status.owned);
      expect(t.mac.sleepDisabled).toBe(true);
      yield* Queue.offer(t.mode, "off");
      yield* t.awaitStatus((status) => !status.owned && !status.blocking);
      expect(t.mac.sleepDisabled).toBe(false);
      expect(yield* t.hasLease).toBe(false);
    }),
  );

  it.effect("does nothing off macOS", () =>
    Effect.gen(function* () {
      const t = yield* harness({ initialMode: "on", platform: "linux" });
      yield* TestClock.adjust("10 minutes");
      expect(t.mac.commands).toEqual([]);
      expect(yield* t.hasLease).toBe(false);
    }),
  );

  describe("release watcher", () => {
    it("builds a shell command that waits for the server and releases only a matching lease", () => {
      const watcher = LidSleepGuard.releaseWatcher({
        serverPid: 4242,
        leasePath: "/tmp/t3 home/lid-sleep-guard.lease",
        startToken: "token-a",
      });
      expect(watcher.command).toBe("sh");
      expect(watcher.args[0]).toBe("-c");
      expect(watcher.args.slice(2)).toEqual([
        "_",
        "4242",
        "/tmp/t3 home/lid-sleep-guard.lease",
        "token-a",
      ]);
      const script = watcher.args[1] ?? "";
      expect(script).toContain('/usr/bin/caffeinate -w "$1"');
      expect(script).toContain('grep -q "\\"pid\\":$1," "$2"');
      expect(script).toContain('grep -q "\\"token\\":\\"$3\\"" "$2"');
      expect(script).toContain("/usr/bin/sudo -n /usr/bin/pmset -a disablesleep 0");
      expect(script).toContain('rm -f "$2"');
    });

    it.effect("starts when T3 acquires a block it owns, once per server", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "auto", initialDemand: true });
        yield* t.awaitStatus((status) => status.owned);
        expect(t.watchers).toEqual([
          LidSleepGuard.releaseWatcher({
            serverPid: SERVER_PID,
            leasePath: t.leasePath,
            startToken: START_TOKEN,
          }),
        ]);
        yield* Queue.offer(t.demand, false);
        yield* TestClock.adjust("60 seconds");
        yield* t.awaitStatus((status) => !status.owned);
        yield* Queue.offer(t.demand, true);
        yield* t.awaitStatus((status) => status.owned);
        expect(countOf(t.mac, SET_ON)).toBe(2);
        expect(t.watchers).toHaveLength(1);
      }),
    );

    it.effect("does not start when T3 adopts a block it did not set", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on", sleepDisabled: true });
        yield* t.awaitStatus((status) => status.blocking && !status.owned);
        expect(t.watchers).toEqual([]);
      }),
    );
  });

  describe("release retry", () => {
    it.effect("retries a failed release with exponential delays capped at 5 minutes", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on" });
        yield* t.awaitStatus((status) => status.owned);
        t.mac.failRelease = true;
        yield* Queue.offer(t.mode, "off");
        // The shutdown-style release and the retry loop each try once right away.
        yield* awaitCommand(t.mac, SET_OFF, 2);
        const delays = [
          "5 seconds",
          "10 seconds",
          "20 seconds",
          "40 seconds",
          "80 seconds",
          "160 seconds",
        ] as const;
        for (const [index, delay] of delays.entries()) {
          yield* TestClock.adjust(delay);
          yield* awaitCommand(t.mac, SET_OFF, index + 3);
        }
        // The next delay would be 320 seconds uncapped, so 300 seconds proves the cap.
        yield* TestClock.adjust("300 seconds");
        yield* awaitCommand(t.mac, SET_OFF, 9);
        yield* TestClock.adjust("300 seconds");
        yield* awaitCommand(t.mac, SET_OFF, 10);
      }),
    );

    it.effect("stops retrying once the release succeeds", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on" });
        yield* t.awaitStatus((status) => status.owned);
        t.mac.failRelease = true;
        yield* Queue.offer(t.mode, "off");
        yield* awaitCommand(t.mac, SET_OFF, 2);
        t.mac.failRelease = false;
        yield* TestClock.adjust("5 seconds");
        yield* t.awaitStatus((status) => !status.owned && !status.blocking);
        expect(yield* t.hasLease).toBe(false);
        yield* TestClock.adjust("10 minutes");
        expect(countOf(t.mac, SET_OFF)).toBe(3);
      }),
    );

    it.effect("retries a failed release after auto's work ends, until work returns", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "auto", initialDemand: true });
        yield* t.awaitStatus((status) => status.owned);
        t.mac.failRelease = true;
        yield* Queue.offer(t.demand, false);
        yield* TestClock.adjust("60 seconds");
        yield* awaitCommand(t.mac, SET_OFF, 2);
        yield* TestClock.adjust("5 seconds");
        yield* awaitCommand(t.mac, SET_OFF, 3);
        yield* Queue.offer(t.demand, true);
        yield* t.awaitStatus((status) => status.owned && status.error === undefined);
        yield* TestClock.adjust("10 minutes");
        expect(countOf(t.mac, SET_OFF)).toBe(3);
      }),
    );

    it.effect("stops retrying when the mode switches back to on during a failing release", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on" });
        yield* t.awaitStatus((status) => status.owned);
        t.mac.failRelease = true;
        yield* Queue.offer(t.mode, "off");
        yield* awaitCommand(t.mac, SET_OFF, 2);
        yield* Queue.offer(t.mode, "on");
        yield* t.awaitStatus((status) => status.owned && status.error === undefined);
        yield* TestClock.adjust("10 minutes");
        expect(countOf(t.mac, SET_OFF)).toBe(2);
        expect(yield* t.hasLease).toBe(true);
      }),
    );
  });

  describe("production only", () => {
    it.effect("reports disabled in development builds and never runs pmset or sudo", () =>
      Effect.gen(function* () {
        const t = yield* harness({
          initialMode: "on",
          production: false,
          sleepDisabled: true,
          staleLease: true,
        });
        yield* t.awaitStatus((status) => status.inactive === "development-build");
        yield* TestClock.adjust("10 minutes");
        expect(t.mac.commands).toEqual([]);
        expect(t.watchers).toEqual([]);
      }),
    );
  });

  describe("adopted block", () => {
    it.effect("acquires when the block it adopted from another tool drops", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on", sleepDisabled: true });
        yield* t.awaitStatus((status) => status.blocking && !status.owned);
        yield* TestClock.adjust("29 seconds");
        expect(sudoCommands(t.mac)).toEqual([]);
        t.mac.sleepDisabled = false;
        yield* TestClock.adjust("1 second");
        yield* t.awaitStatus((status) => status.owned);
        expect(sudoCommands(t.mac)).toEqual([SET_ON]);
        expect(t.watchers).toHaveLength(1);
      }),
    );

    it.effect("keeps leaving the block alone while the other tool still holds it", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on", sleepDisabled: true });
        yield* t.awaitStatus((status) => status.blocking && !status.owned);
        yield* TestClock.adjust("5 minutes");
        expect(sudoCommands(t.mac)).toEqual([]);
        expect(yield* t.hasLease).toBe(false);
      }),
    );
  });

  describe("lease identity", () => {
    it.effect("records the server pid, start token and T3 home", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on" });
        yield* t.awaitStatus((status) => status.owned);
        expect(yield* t.readLease).toMatchObject({
          pid: SERVER_PID,
          token: START_TOKEN,
          home: HOME,
        });
      }),
    );

    it.effect("drops a stale lease without sudo when nothing is blocked", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "off", staleLease: true });
        // The second read comes from the off mode, after the stale lease was settled.
        yield* awaitCommand(t.mac, PMSET_READ, 2);
        yield* awaitCommand(t.mac, PMSET_READ, 1);
        expect(sudoCommands(t.mac)).toEqual([]);
        expect(yield* t.hasLease).toBe(false);
      }),
    );

    it.effect("releases a stale lease before acquiring a new one of its own", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on", sleepDisabled: true, staleLease: true });
        yield* t.awaitStatus((status) => status.owned);
        expect(sudoCommands(t.mac)).toEqual([SET_OFF, SET_ON]);
        expect(yield* t.readLease).toMatchObject({ pid: SERVER_PID, token: START_TOKEN });
      }),
    );

    it.effect("leaves no lease when its own set fails, even if another tool then blocks", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on", externalTakesOverOnFailedSet: true });
        yield* t.awaitStatus((status) => status.error === "failed");
        expect(yield* t.hasLease).toBe(false);
        yield* TestClock.adjust("5 seconds");
        yield* t.awaitStatus((status) => status.blocking && !status.owned);
        expect(sudoCommands(t.mac)).toEqual([SET_ON]);
        expect(yield* t.hasLease).toBe(false);
      }),
    );
  });

  describe("status", () => {
    it.effect("starts from pmset and not from the lease file", () =>
      Effect.gen(function* () {
        const stale = yield* harness({ staleLease: true });
        yield* stale.awaitStatus((status) => !status.blocking && !status.owned);
        const external = yield* harness({ sleepDisabled: true });
        yield* external.awaitStatus((status) => status.blocking && !status.owned);
      }),
    );

    it.effect("says auto is inactive while keep awake is off", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "auto", keepAwakeEnabled: false });
        yield* t.awaitStatus((status) => status.inactive === "keep-awake-off");
        yield* Queue.offer(t.keepAwakeEnabled, true);
        yield* t.awaitStatus((status) => status.inactive === undefined);
        yield* Queue.offer(t.keepAwakeEnabled, false);
        yield* t.awaitStatus((status) => status.inactive === "keep-awake-off");
        yield* Queue.offer(t.mode, "on");
        yield* t.awaitStatus((status) => status.inactive === undefined);
      }),
    );
  });

  describe("coverage the review found missing", () => {
    it.effect("releases an owned block on normal shutdown", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on" });
        yield* t.awaitStatus((status) => status.owned);
        yield* t.shutDown;
        expect(sudoCommands(t.mac)).toEqual([SET_ON, SET_OFF]);
        expect(t.mac.sleepDisabled).toBe(false);
        expect(yield* t.hasLease).toBe(false);
      }),
    );

    it.effect("recovers when a set succeeds and the read after it fails", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on", failReadAfterSet: true });
        yield* t.awaitStatus((status) => status.error === "failed");
        expect(yield* t.hasLease).toBe(true);
        yield* TestClock.adjust("5 seconds");
        yield* t.awaitStatus((status) => status.owned && status.error === undefined);
        expect(countOf(t.mac, SET_ON)).toBe(1);
        expect(t.watchers).toHaveLength(1);
        yield* t.shutDown;
        expect(t.mac.sleepDisabled).toBe(false);
        expect(yield* t.hasLease).toBe(false);
      }),
    );

    it.effect("switches mode while a release is in flight and ends owning the block", () =>
      Effect.gen(function* () {
        const t = yield* harness({ initialMode: "on" });
        yield* t.awaitStatus((status) => status.owned);
        yield* Queue.offer(t.mode, "off");
        yield* Queue.offer(t.mode, "on");
        yield* t.awaitStatus((status) => status.owned && status.error === undefined);
        yield* TestClock.adjust("1 minute");
        expect(t.mac.sleepDisabled).toBe(true);
        expect(yield* t.hasLease).toBe(true);
      }),
    );
  });
});
