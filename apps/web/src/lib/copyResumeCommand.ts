import { thrownErrorMessage } from "@t3tools/client-runtime/errors";
import { msg } from "gt-react";

import { stackedThreadToast, toastManager } from "../components/ui/toast";
import { writeTextToClipboard } from "../hooks/useCopyToClipboard";
import { m } from "../i18n/messages";

export async function copyResumeCommand(command: string): Promise<void> {
  try {
    await writeTextToClipboard(command, {
      target: "resume command",
      unavailableMessage: msg("Clipboard API is unavailable while copying resume command."),
      failureMessage: msg("Failed to copy resume command to the clipboard."),
    });
  } catch (error) {
    console.error(error);
    toastManager.add(
      stackedThreadToast({
        type: "error",
        title: m(msg("Failed to copy resume command")),
        description: thrownErrorMessage(error, m) ?? m(msg("An error occurred.")),
      }),
    );
  }
}
