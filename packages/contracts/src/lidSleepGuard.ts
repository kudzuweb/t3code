import * as Schema from "effect/Schema";

/** `auto` holds the block while agents work; `on` holds it whenever the server runs. */
export const LidSleepGuardMode = Schema.Literals(["off", "auto", "on"]);
export type LidSleepGuardMode = typeof LidSleepGuardMode.Type;

export const LidSleepGuardError = Schema.Literals(["needs-sudoers-rule", "failed"]);
export type LidSleepGuardError = typeof LidSleepGuardError.Type;

/** Why the guard does nothing at all: `development-build` always, `keep-awake-off` in Auto only. */
export const LidSleepGuardInactiveReason = Schema.Literals(["development-build", "keep-awake-off"]);
export type LidSleepGuardInactiveReason = typeof LidSleepGuardInactiveReason.Type;

export const LidSleepGuardStatus = Schema.Struct({
  /** Whether macOS reported `SleepDisabled` the last time the server read it. */
  blocking: Schema.Boolean,
  /** Whether this server set the block and will release it. */
  owned: Schema.Boolean,
  error: Schema.optionalKey(LidSleepGuardError),
  inactive: Schema.optionalKey(LidSleepGuardInactiveReason),
});
export type LidSleepGuardStatus = typeof LidSleepGuardStatus.Type;
