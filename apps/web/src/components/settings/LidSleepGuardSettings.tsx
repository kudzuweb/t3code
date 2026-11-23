import { useAtomValue } from "@effect/atom-react";
import type { LidSleepGuardMode, LidSleepGuardStatus } from "@t3tools/contracts";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";
import { msg, useGT, useMessages } from "gt-react";

import { environmentAtomList, environmentIdsKey } from "../../state/environmentAtomList";
import { serverEnvironment } from "../../state/server";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingResetButton, SettingsRow } from "./settingsLayout";
import { settingsScopeEnvironmentLabel } from "./settingsScopeAxis";
import { useSearchableSetting } from "./settingsSearch";
import {
  useScopedSettings,
  useScopedSettingsMixed,
  useUpdateScopedSettings,
} from "./useScopedSettings";

export const LID_SLEEP_GUARD_MODE_LABELS: Record<LidSleepGuardMode, string> = {
  off: msg("Off"),
  auto: msg("Auto"),
  on: msg("On"),
};

const LID_SLEEP_GUARD_MODES = ["off", "auto", "on"] as const satisfies readonly LidSleepGuardMode[];

const lidSleepGuardStatusesAtom = environmentAtomList(
  "web-lid-sleep-guard-status",
  (environmentId) => serverEnvironment.lidSleepGuardStatus({ environmentId, input: {} }),
);

function isLidSleepGuardMode(value: unknown): value is LidSleepGuardMode {
  return LID_SLEEP_GUARD_MODES.some((mode) => mode === value);
}

export function LidSleepGuardSettingsRow() {
  const setting = useSearchableSetting();
  const gt = useGT();
  const messages = useMessages();
  const settings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  const mixed = useScopedSettingsMixed(["lidSleepGuard"]);
  const { connectedEnvironments } = useSettingsScope();
  const supported = connectedEnvironments.filter(
    (environment) => environment.serverConfig?.environment.capabilities.lidSleepGuard === true,
  );
  const statuses = useAtomValue(
    lidSleepGuardStatusesAtom(
      environmentIdsKey(supported.map(({ environmentId }) => environmentId)),
    ),
  );
  if (supported.length === 0) return null;

  const describe = (status: LidSleepGuardStatus) => {
    if (status.inactive === "development-build") {
      return gt("Lid sleep guard is disabled in development builds.");
    }
    if (status.inactive === "keep-awake-off") {
      return gt("Auto is inactive because Keep awake while agents work is off.");
    }
    if (status.error === "needs-sudoers-rule") return gt("Lid sleep guard needs its sudoers rule");
    if (status.error === "failed") return gt("Lid sleep guard could not change the sleep setting");
    if (status.blocking && status.owned) {
      return gt("Blocking lid sleep. T3 Code set this.");
    }
    if (status.blocking) {
      return gt("Lid sleep is blocked by something else. T3 Code will leave it alone.");
    }
    return gt("Not blocking lid sleep.");
  };
  const lines = supported.flatMap((environment, index) => {
    const query = statuses[index];
    if (query?._tag !== "Success") return [];
    const status = query.value;
    return [
      {
        key: environment.environmentId,
        failed: status.error !== undefined,
        text:
          supported.length > 1
            ? gt("{environment}: {status}", {
                environment: settingsScopeEnvironmentLabel(environment, connectedEnvironments),
                status: describe(status),
              })
            : describe(status),
      },
    ];
  });

  return (
    <SettingsRow
      serverScoped
      settingKeys={["lidSleepGuard"]}
      {...setting("lid-sleep-guard")}
      description={gt(
        "Keeps the Mac awake with the lid closed. Auto does it only while agents are working. On stays in effect after T3 Code quits, until you switch it Off or run `sudo pmset -a disablesleep 0`.",
      )}
      status={
        lines.length > 0 ? (
          <div className="space-y-0.5">
            {lines.map((line) => (
              <p key={line.key} className={line.failed ? "text-destructive" : undefined}>
                {line.text}
              </p>
            ))}
          </div>
        ) : null
      }
      resetAction={
        settings.lidSleepGuard !== DEFAULT_UNIFIED_SETTINGS.lidSleepGuard ? (
          <SettingResetButton
            ariaLabel={gt("Reset lid sleep guard to default")}
            onClick={() =>
              updateSettings({ lidSleepGuard: DEFAULT_UNIFIED_SETTINGS.lidSleepGuard })
            }
          />
        ) : null
      }
      control={
        <Select
          value={mixed ? null : settings.lidSleepGuard}
          onValueChange={(value) => {
            if (isLidSleepGuardMode(value)) updateSettings({ lidSleepGuard: value });
          }}
        >
          <SelectTrigger size="sm" className="w-full sm:w-40" aria-label={gt("Lid sleep guard")}>
            <SelectValue>
              {(value: LidSleepGuardMode | null) =>
                value === null
                  ? gt("Mixed", { $context: "values differ across targets" })
                  : messages(LID_SLEEP_GUARD_MODE_LABELS[value])
              }
            </SelectValue>
          </SelectTrigger>
          <SelectPopup align="end" alignItemWithTrigger={false}>
            {LID_SLEEP_GUARD_MODES.map((mode) => (
              <SelectItem key={mode} hideIndicator value={mode}>
                {messages(LID_SLEEP_GUARD_MODE_LABELS[mode])}
                {mode === "auto" && !settings.keepAwakeWhileAgentsWork ? (
                  <span className="block text-muted-foreground text-xs">
                    {gt("Needs Keep awake while agents work")}
                  </span>
                ) : null}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      }
    />
  );
}
