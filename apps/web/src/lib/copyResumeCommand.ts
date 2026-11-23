import { thrownErrorMessage } from "@t3tools/client-runtime/errors";
import type { ThreadResume } from "@t3tools/client-runtime/resume-command";
import { msg } from "gt-react";

import { stackedThreadToast, toastManager } from "../components/ui/toast";
import { writeTextToClipboard } from "../hooks/useCopyToClipboard";
import { m } from "../i18n/messages";

async function copyCommandToClipboard(command: string): Promise<boolean> {
  try {
    await writeTextToClipboard(command, {
      target: "resume command",
      unavailableMessage: msg("Clipboard API is unavailable while copying resume command."),
      failureMessage: msg("Failed to copy resume command to the clipboard."),
    });
    return true;
  } catch (error) {
    console.error(error);
    toastManager.add(
      stackedThreadToast({
        type: "error",
        title: m(msg("Failed to copy resume command")),
        description: thrownErrorMessage(error, m) ?? m(msg("An error occurred.")),
      }),
    );
    return false;
  }
}

/**
 * Copies the thread's resume command at once. When T3 holds the session, it then asks the server
 * to let go and says whether that worked. `releaseHold` resolves to the failure, or null on success.
 */
export async function copyResumeCommand(
  resume: ThreadResume,
  releaseHold: () => Promise<unknown>,
): Promise<void> {
  const copied = await copyCommandToClipboard(resume.command);
  if (!copied || !resume.releaseFirst) return;
  const failure = await releaseHold();
  if (failure === null) {
    toastManager.add({
      type: "success",
      title: m(msg("T3 let go of this thread")),
      description: m(
        msg("Paste the command in a terminal; Codex can take a few seconds to free it."),
      ),
    });
    return;
  }
  toastManager.add(
    stackedThreadToast({
      type: "warning",
      title: m(msg("T3 still holds this thread")),
      description:
        thrownErrorMessage(failure, m) ?? m(msg("The command is copied, but Codex may refuse it.")),
    }),
  );
}
