import { AuthSettingsWriteScope, type LidSleepGuardMode } from "@t3tools/contracts";
import { T, useGT, useMessages } from "gt-react";
import { LaptopIcon } from "lucide-react";

import { useEnvironments } from "../state/environments";
import { serverEnvironment } from "../state/server";
import { readEnvironmentScope } from "../state/session";
import { useAtomCommand } from "../state/use-atom-command";
import {
  ADDON_ICON_CLASS,
  ITEM_ICON_CLASS,
  type CommandPaletteSubmenuItem,
} from "./CommandPalette.logic";
import { LID_SLEEP_GUARD_MODE_LABELS } from "./settings/LidSleepGuardSettings";
import { stackedThreadToast, toastManager } from "./ui/toast";

const MODES = ["off", "auto", "on"] as const satisfies readonly LidSleepGuardMode[];

/** "Lid sleep guard": the three modes, applied to every connected Mac environment. */
export function useCommandPaletteLidSleepGuardItem(): CommandPaletteSubmenuItem | null {
  const gt = useGT();
  const messages = useMessages();
  const { environments } = useEnvironments();
  const update = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  const supported = environments.filter(
    (environment) =>
      environment.connection.phase === "connected" &&
      environment.serverConfig?.environment.capabilities.lidSleepGuard === true,
  );
  if (supported.length === 0) return null;

  const currentModes = new Set(
    supported.map(({ serverConfig }) => serverConfig?.settings.lidSleepGuard),
  );
  const current = currentModes.size === 1 ? [...currentModes][0] : undefined;

  const setMode = async (mode: LidSleepGuardMode) => {
    const outcomes = await Promise.all(
      supported.map((environment) =>
        readEnvironmentScope(environment.environmentId, AuthSettingsWriteScope)
          ? update({
              environmentId: environment.environmentId,
              input: { patch: { lidSleepGuard: mode } },
            })
          : Promise.resolve(null),
      ),
    );
    if (outcomes.some((outcome) => outcome === null || outcome._tag === "Failure")) {
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title: gt("Couldn't change lid sleep guard"),
          description: gt("Try again."),
        }),
      );
    }
  };

  return {
    kind: "submenu",
    value: "action:lid-sleep-guard",
    searchTerms: ["lid sleep guard", "close lid", "clamshell", "sleep", "awake", "mac"],
    title: gt("Lid sleep guard"),
    icon: <LaptopIcon className={ITEM_ICON_CLASS} />,
    addonIcon: <LaptopIcon className={ADDON_ICON_CLASS} />,
    groups: [
      {
        value: "lid-sleep-guard",
        label: gt("Lid sleep guard"),
        items: MODES.map((mode) => ({
          kind: "action",
          value: `lid-sleep-guard:${mode}`,
          title: messages(LID_SLEEP_GUARD_MODE_LABELS[mode]),
          searchTerms: [messages(LID_SLEEP_GUARD_MODE_LABELS[mode]), "lid sleep guard", "sleep"],
          icon: <LaptopIcon className={ITEM_ICON_CLASS} />,
          titleTrailingContent:
            current === mode ? (
              <span className="text-xs text-muted-foreground/70">
                <T context="the theme or mode in use now">Current</T>
              </span>
            ) : undefined,
          run: () => setMode(mode),
        })),
      },
    ],
  };
}
