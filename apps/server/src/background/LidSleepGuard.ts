import type { LidSleepGuardMode, LidSleepGuardStatus } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberHandle from "effect/FiberHandle";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as SubscriptionRef from "effect/SubscriptionRef";

import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import { forkParked } from "../serverActivation.ts";
import * as Settings from "../serverSettings.ts";
import * as KeepAwake from "./KeepAwake.ts";

const LEASE_FILE_NAME = "lid-sleep-guard.lease";

const SUDO = "/usr/bin/sudo";
const PMSET = "/usr/bin/pmset";
const COMMAND_TIMEOUT = "10 seconds";
// The sudoers rule matches exactly these arguments, so the strings must not vary.
const setSleepDisabledArgs = (disabled: boolean) => [
  "-n",
  PMSET,
  "-a",
  "disablesleep",
  disabled ? "1" : "0",
];

const STATUS_RECHECK_INTERVAL = "30 seconds";
const releaseBackoff = Schedule.exponential("5 seconds").pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, Duration.minutes(5))),
  ),
);

export interface ReleaseWatcher {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}

/**
 * Outlives the server. Polls with `kill -0`, not `caffeinate -w`, which would hold an idle-sleep
 * assertion. Clears `disablesleep` only if the lease still names this server's pid and start token.
 * $1 is the server pid, $2 the lease path and $3 the start token.
 */
export const releaseWatcher = (input: {
  readonly serverPid: number;
  readonly leasePath: string;
  readonly startToken: string;
}): ReleaseWatcher => ({
  command: "sh",
  args: [
    "-c",
    [
      'while kill -0 "$1" 2>/dev/null; do sleep 5; done',
      'grep -q "\\"pid\\":$1," "$2" && grep -q "\\"token\\":\\"$3\\"" "$2"' +
        ` && ${SUDO} ${setSleepDisabledArgs(false).join(" ")} && rm -f "$2"`,
    ].join("; "),
    "_",
    String(input.serverPid),
    input.leasePath,
    input.startToken,
  ],
});

const LeaseContents = Schema.Struct({
  pid: Schema.Number,
  token: Schema.String,
  home: Schema.String,
});
const decodeLease = Schema.decodeUnknownOption(Schema.fromJsonString(LeaseContents));

export class LidSleepGuard extends Context.Service<
  LidSleepGuard,
  {
    /** Emits the current status first, then every change. */
    readonly status: Stream.Stream<LidSleepGuardStatus>;
  }
>()("t3/background/LidSleepGuard") {}

class LidSleepGuardFailure extends Schema.TaggedError<LidSleepGuardFailure>()(
  "LidSleepGuardFailure",
  { reason: Schema.Literals(["needs-sudoers-rule", "failed"]), detail: Schema.String },
) {
  override get message(): string {
    return `lid sleep guard failed (${this.reason}): ${this.detail}`;
  }
}

const NEEDS_RULE_STDERR = /password is required|not allowed|may not run sudo|no tty present/i;

export const make = Effect.fn("LidSleepGuard.make")(function* <E>(input: {
  readonly platform: NodeJS.Platform;
  readonly mode: Stream.Stream<LidSleepGuardMode>;
  /** True while the keep-awake setting is on and a run is active; the guard adds the release grace. */
  readonly demand: Stream.Stream<boolean, E>;
  /** The "Keep awake while agents work" setting; Auto never blocks while it is off. */
  readonly keepAwakeEnabled: Stream.Stream<boolean>;
  /** Only a server started without a Vite dev URL (packaged app or `t3` CLI) may change `pmset`. */
  readonly production: boolean;
  readonly leasePath: string;
  readonly serverPid: number;
  /** Identifies this server start, so a reused pid is never mistaken for it. */
  readonly startToken: string;
  readonly home: string;
  readonly spawnWatcher: (
    watcher: ReleaseWatcher,
  ) => Effect.Effect<void, PlatformError.PlatformError>;
  readonly grace?: KeepAwake.Grace;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const statusRef = yield* SubscriptionRef.make<LidSleepGuardStatus>({
    blocking: false,
    owned: false,
    ...(input.platform === "darwin" && !input.production
      ? { inactive: "development-build" as const }
      : {}),
  });
  const service = LidSleepGuard.of({ status: SubscriptionRef.changes(statusRef) });
  if (input.platform !== "darwin" || !input.production) return service;

  const runner = yield* ProcessRunner.ProcessRunner;

  const runCommand = (command: string, args: ReadonlyArray<string>) =>
    runner.run({ command, args, timeout: COMMAND_TIMEOUT }).pipe(
      Effect.mapError(
        (cause) => new LidSleepGuardFailure({ reason: "failed", detail: cause.message }),
      ),
      Effect.flatMap((result) =>
        result.code === 0
          ? Effect.succeed(result.stdout)
          : Effect.fail(
              new LidSleepGuardFailure({
                reason: NEEDS_RULE_STDERR.test(result.stderr) ? "needs-sudoers-rule" : "failed",
                detail: result.stderr.trim() || `exit code ${result.code}`,
              }),
            ),
      ),
    );
  const readBlocking = runCommand(PMSET, ["-g"]).pipe(
    Effect.map((stdout) => /^\s*SleepDisabled\s+1\b/m.test(stdout)),
  );
  const setBlocking = (blocking: boolean) =>
    runCommand(SUDO, setSleepDisabledArgs(blocking)).pipe(Effect.asVoid);
  const writeLease = DateTime.now.pipe(
    Effect.flatMap((now) =>
      fileSystem.writeFileString(
        input.leasePath,
        // The watcher greps for the pid and token in this order.
        JSON.stringify({
          pid: input.serverPid,
          token: input.startToken,
          home: input.home,
          acquiredAt: DateTime.formatIso(now),
        }),
      ),
    ),
    Effect.mapError(
      (cause) => new LidSleepGuardFailure({ reason: "failed", detail: cause.message }),
    ),
  );
  const hasLease = fileSystem.exists(input.leasePath).pipe(Effect.orElseSucceed(() => false));
  /** True only when the lease names this server start. */
  const ownsLease = fileSystem.readFileString(input.leasePath).pipe(
    Effect.map((text) =>
      Option.exists(decodeLease(text), (lease) => lease.token === input.startToken),
    ),
    Effect.orElseSucceed(() => false),
  );
  const removeLease = fileSystem
    .remove(input.leasePath, { force: true })
    .pipe(
      Effect.mapError(
        (cause) => new LidSleepGuardFailure({ reason: "failed", detail: cause.message }),
      ),
    );

  const publish = (status: LidSleepGuardStatus) => SubscriptionRef.set(statusRef, status);
  const publishFailure = (failure: LidSleepGuardFailure) =>
    Effect.logWarning("lid sleep guard could not change the sleep setting", {
      reason: failure.reason,
      detail: failure.detail,
    }).pipe(
      Effect.andThen(
        SubscriptionRef.update(statusRef, (status) => ({ ...status, error: failure.reason })),
      ),
    );

  // Serialized because a mode switch can start one hold while another is releasing.
  const serialized = Semaphore.makeUnsafe(1).withPermits(1);

  // The watcher is a backstop for a server that dies without releasing, and one per server
  // lifetime covers every later acquire, because it checks the lease only when the server exits.
  const watcherStarted = yield* Ref.make(false);
  const ensureWatcher = Effect.gen(function* () {
    if (yield* Ref.get(watcherStarted)) return;
    yield* input
      .spawnWatcher(
        releaseWatcher({
          serverPid: input.serverPid,
          leasePath: input.leasePath,
          startToken: input.startToken,
        }),
      )
      .pipe(Effect.andThen(Ref.set(watcherStarted, true)));
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("lid sleep guard could not start its release watcher", { cause }),
    ),
  );

  const acquire = serialized(
    Effect.gen(function* () {
      const owned = yield* ownsLease;
      const blocking = yield* readBlocking;
      if (blocking) {
        // An existing block is adopted only when this server start's lease says it set it.
        if (owned) yield* ensureWatcher;
        yield* publish({ blocking: true, owned });
        return;
      }
      yield* writeLease;
      // A failed set leaves no lease: losing a block is acceptable, a Mac stuck awake is not.
      yield* setBlocking(true).pipe(Effect.tapError(() => Effect.ignore(removeLease)));
      if (!(yield* readBlocking)) {
        return yield* new LidSleepGuardFailure({
          reason: "failed",
          detail: "sleep stayed enabled after disablesleep 1",
        });
      }
      yield* ensureWatcher;
      yield* publish({ blocking: true, owned: true });
    }).pipe(Effect.tapError(publishFailure)),
  );

  // Fails when the block could not be cleared, and keeps the lease so a retry still owns it.
  const releaseOnce = serialized(
    Effect.gen(function* () {
      if (!(yield* ownsLease)) {
        return yield* publish({ blocking: yield* readBlocking, owned: false });
      }
      yield* setBlocking(false);
      if (yield* readBlocking) {
        return yield* new LidSleepGuardFailure({
          reason: "failed",
          detail: "sleep stayed disabled after disablesleep 0",
        });
      }
      yield* removeLease;
      yield* publish({ blocking: false, owned: false });
    }).pipe(Effect.tapError(publishFailure)),
  );
  const releaseUntilDone = releaseOnce.pipe(Effect.retry(releaseBackoff), Effect.ignore);

  // A release that failed in a finalizer keeps retrying here until it succeeds or a new acquire
  // starts, because a finalizer must not wait for it.
  const pendingRelease = yield* FiberHandle.make();
  const release = releaseOnce.pipe(
    Effect.catch(() => FiberHandle.run(pendingRelease, releaseUntilDone, { onlyIfMissing: true })),
    Effect.asVoid,
  );

  // While another tool holds the block, T3 holds nothing itself, so it re-reads the block and
  // takes over when the other tool lets go.
  const takeOverWhenReleased = Effect.gen(function* () {
    if (yield* ownsLease) return;
    if (yield* readBlocking) return;
    yield* acquire;
  }).pipe(
    Effect.ignore,
    Effect.schedule(Schedule.spaced(STATUS_RECHECK_INTERVAL)),
    Effect.andThen(Effect.never),
  );

  const hold = Effect.acquireRelease(
    FiberHandle.clear(pendingRelease).pipe(Effect.andThen(acquire)),
    () => release,
  ).pipe(
    Effect.andThen(takeOverWhenReleased),
    Effect.scoped,
    KeepAwake.retryWithBackoff("lid sleep guard could not acquire the sleep block"),
    Effect.ignore,
  );

  // A lease at startup belongs to a server that is gone, because one server per home runs at a
  // time. Clearing it is safe only while `disablesleep` is set; if another tool set it after a
  // crash, this releases that block too, which only loses blocking.
  const settleStaleLease = Effect.gen(function* () {
    if (!(yield* hasLease)) {
      const blocking = yield* readBlocking.pipe(Effect.orElseSucceed(() => false));
      return yield* publish({ blocking, owned: false });
    }
    if (yield* readBlocking) yield* setBlocking(false);
    yield* removeLease;
    yield* publish({ blocking: false, owned: false });
  }).pipe(serialized, Effect.tapError(publishFailure), Effect.retry(releaseBackoff), Effect.ignore);

  const holdFor = (mode: LidSleepGuardMode) => {
    switch (mode) {
      case "off":
        return FiberHandle.run(pendingRelease, releaseUntilDone, { onlyIfMissing: true }).pipe(
          Effect.asVoid,
        );
      case "on":
        return hold;
      case "auto":
        return KeepAwake.holdWhileWanted(input.demand, hold, input.grace);
    }
  };

  const setInactive = (inactive: LidSleepGuardStatus["inactive"]) =>
    SubscriptionRef.update(statusRef, ({ inactive: _previous, ...status }) =>
      inactive === undefined ? status : { ...status, inactive },
    );

  yield* forkParked(
    settleStaleLease.pipe(
      Effect.andThen(
        Stream.zipLatestWith(
          input.mode,
          input.keepAwakeEnabled,
          (mode, keepAwake) => [mode, keepAwake] as const,
        ).pipe(
          Stream.tap(([mode, keepAwake]) =>
            setInactive(mode === "auto" && !keepAwake ? "keep-awake-off" : undefined),
          ),
          Stream.map(([mode]) => mode),
          Stream.changes,
          Stream.switchMap((mode) => Stream.fromEffect(holdFor(mode))),
          Stream.runDrain,
        ),
      ),
      Effect.catchCause((cause) => Effect.logWarning("lid sleep guard stopped", { cause })),
    ),
  );
  return service;
});

// Detached into its own process group with its stdio closed, and unreferenced so that closing
// the scope does not kill it: it has to outlive the server.
const spawnDetached = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  watcher: ReleaseWatcher,
) =>
  spawner
    .spawn(
      ChildProcess.make(watcher.command, watcher.args, {
        detached: true,
        shell: false,
        stdin: "ignore",
        stdout: "ignore",
        stderr: "ignore",
      }),
    )
    .pipe(
      Effect.flatMap((child) => child.unref),
      Effect.scoped,
      Effect.asVoid,
    );

export const layer = Layer.effect(
  LidSleepGuard,
  Effect.gen(function* () {
    const platform = yield* HostProcess.Platform;
    const config = yield* ServerConfig.ServerConfig;
    const path = yield* Path.Path;
    const settings = yield* Settings.ServerSettingsService;
    const demand = yield* KeepAwake.demand;
    const keepAwakeEnabled = yield* KeepAwake.keepAwakeEnabled;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const random = yield* SecureRandom.SecureRandom;
    const startToken = yield* random.uuidV4();
    const mode = Stream.unwrap(
      Effect.gen(function* () {
        const changes = yield* settings.subscribeChanges;
        const current = yield* settings.getSettings.pipe(
          Effect.map((value) => value.lidSleepGuard),
          Effect.orElseSucceed(() => "off" as const),
        );
        return Stream.concat(
          Stream.succeed(current),
          Stream.map(changes, (value) => value.lidSleepGuard),
        );
      }),
    );
    return yield* make({
      platform,
      production: config.devUrl === undefined,
      mode,
      demand,
      keepAwakeEnabled,
      leasePath: path.join(config.stateDir, LEASE_FILE_NAME),
      serverPid: process.pid,
      startToken,
      home: config.baseDir,
      spawnWatcher: (watcher) => spawnDetached(spawner, watcher),
    });
  }),
);
