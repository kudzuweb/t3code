import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import type { LidSleepGuardMode, LidSleepGuardStatus } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProcessRunner from "../processRunner.ts";
import * as LidSleepGuard from "./LidSleepGuard.ts";

const SET_ON = "/usr/bin/sudo -n /usr/bin/pmset -a disablesleep 1";
const SET_OFF = "/usr/bin/sudo -n /usr/bin/pmset -a disablesleep 0";

interface FakeMac {
  sleepDisabled: boolean;
  sudo: "allowed" | "no-rule";
  failRelease: boolean;
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
          return output(0, ` SleepDisabled\t\t${mac.sleepDisabled ? 1 : 0}\n sleep 1\n`);
        }
        if (line === SET_ON || line === SET_OFF) {
          if (mac.sudo === "no-rule") return output(1, "", "sudo: a password is required\n");
          if (line === SET_OFF && mac.failRelease) return output(1, "", "pmset: failed\n");
          mac.sleepDisabled = line === SET_ON;
          return output(0);
        }
        return output(127, "", `unexpected command: ${line}`);
      }),
  });

const harness = (options: {
  readonly initialMode: LidSleepGuardMode;
  readonly sleepDisabled?: boolean;
  readonly staleLease?: boolean;
  readonly platform?: NodeJS.Platform;
  readonly initialDemand?: boolean;
  readonly sudo?: FakeMac["sudo"];
}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-lid-sleep-guard-" });
    const leasePath = `${directory}/lid-sleep-guard.lease`;
    if (options.staleLease) yield* fileSystem.writeFileString(leasePath, "{}");
    const mac: FakeMac = {
      sleepDisabled: options.sleepDisabled ?? false,
      sudo: options.sudo ?? "allowed",
      failRelease: false,
      commands: [],
      log: yield* Queue.unbounded<string>(),
    };
    const mode = yield* Queue.unbounded<LidSleepGuardMode>();
    const demand = yield* Queue.unbounded<boolean>();
    yield* Queue.offer(mode, options.initialMode);
    if (options.initialDemand !== undefined) yield* Queue.offer(demand, options.initialDemand);
    const guard = yield* LidSleepGuard.make({
      platform: options.platform ?? "darwin",
      mode: Stream.fromQueue(mode),
      demand: Stream.fromQueue(demand),
      leasePath,
    }).pipe(Effect.provideService(ProcessRunner.ProcessRunner, fakeRunner(mac)));
    const awaitStatus = (predicate: (status: LidSleepGuardStatus) => boolean) =>
      guard.status.pipe(Stream.filter(predicate), Stream.runHead, Effect.map(Option.getOrThrow));
    return {
      mac,
      mode,
      demand,
      awaitStatus,
      hasLease: fileSystem.exists(leasePath),
    };
  }).pipe(Effect.provide(NodeServices.layer));

const sudoCommands = (mac: FakeMac) => mac.commands.filter((line) => line.includes("sudo"));

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
      // One read from the acquire attempt and one from the release attempt; neither runs sudo.
      expect([yield* Queue.take(t.mac.log), yield* Queue.take(t.mac.log)]).toEqual([
        "/usr/bin/pmset -g",
        "/usr/bin/pmset -g",
      ]);
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
      yield* t.awaitStatus((status) => !status.owned && !status.blocking);
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
      yield* t.awaitStatus((status) => !status.owned && !status.blocking);
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
});
