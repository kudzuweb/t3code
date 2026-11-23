import type { LidSleepGuardMode, LidSleepGuardStatus } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
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
  /** True while agents are working, already including KeepAwake's release grace. */
  readonly demand: Stream.Stream<boolean, E>;
  readonly leasePath: string;
  readonly grace?: KeepAwake.Grace;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const hasLease = fileSystem.exists(input.leasePath).pipe(Effect.orElseSucceed(() => false));
  const initiallyOwned = input.platform === "darwin" && (yield* hasLease);
  const statusRef = yield* SubscriptionRef.make<LidSleepGuardStatus>({
    blocking: initiallyOwned,
    owned: initiallyOwned,
  });
  const service = LidSleepGuard.of({ status: SubscriptionRef.changes(statusRef) });
  if (input.platform !== "darwin") return service;

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
        JSON.stringify({ acquiredAt: DateTime.formatIso(now) }),
      ),
    ),
    Effect.mapError(
      (cause) => new LidSleepGuardFailure({ reason: "failed", detail: cause.message }),
    ),
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

  const acquire = serialized(
    Effect.gen(function* () {
      const owned = yield* hasLease;
      const blocking = yield* readBlocking;
      if (blocking) {
        // An existing block is adopted only when this server's lease says it set it.
        return yield* publish({ blocking: true, owned });
      }
      yield* writeLease;
      yield* setBlocking(true).pipe(
        Effect.tapError(() =>
          // Nothing applied means nothing to release later.
          readBlocking.pipe(
            Effect.flatMap((applied) => (applied ? Effect.void : removeLease)),
            Effect.ignore,
          ),
        ),
      );
      if (!(yield* readBlocking)) {
        return yield* new LidSleepGuardFailure({
          reason: "failed",
          detail: "sleep stayed enabled after disablesleep 1",
        });
      }
      yield* publish({ blocking: true, owned: true });
    }).pipe(Effect.tapError(publishFailure)),
  );

  // Never fails: it runs in finalizers, and a failure leaves the lease for a later retry.
  const release = serialized(
    Effect.gen(function* () {
      if (!(yield* hasLease)) {
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
    }).pipe(Effect.tapError(publishFailure), Effect.ignore),
  );

  const hold = Effect.acquireRelease(acquire, () => release).pipe(
    Effect.andThen(Effect.never),
    Effect.scoped,
    KeepAwake.retryWithBackoff("lid sleep guard could not acquire the sleep block"),
    Effect.ignore,
  );

  // The first reading settles a lease left by a crash: no work now means releasing it, work now
  // means the next acquire adopts it.
  const demandWithStartupCleanup = input.demand.pipe(
    Stream.zipWithIndex,
    Stream.tap(([wanted, index]) => (index === 0 && !wanted ? release : Effect.void)),
    Stream.map(([wanted]) => wanted),
  );

  const holdFor = (mode: LidSleepGuardMode) => {
    switch (mode) {
      case "off":
        return release;
      case "on":
        return hold;
      case "auto":
        return KeepAwake.holdWhileWanted(demandWithStartupCleanup, hold, input.grace);
    }
  };

  yield* forkParked(
    input.mode.pipe(
      Stream.changes,
      Stream.switchMap((mode) => Stream.fromEffect(holdFor(mode))),
      Stream.runDrain,
      Effect.catchCause((cause) => Effect.logWarning("lid sleep guard stopped", { cause })),
    ),
  );
  return service;
});

export const layer = Layer.effect(
  LidSleepGuard,
  Effect.gen(function* () {
    const platform = yield* HostProcess.Platform;
    const config = yield* ServerConfig.ServerConfig;
    const path = yield* Path.Path;
    const settings = yield* Settings.ServerSettingsService;
    const demand = yield* KeepAwake.demand;
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
      mode,
      demand,
      leasePath: path.join(config.stateDir, LEASE_FILE_NAME),
    });
  }),
);
