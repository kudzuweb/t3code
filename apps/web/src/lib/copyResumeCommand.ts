import { thrownErrorMessage } from "@t3tools/client-runtime/errors";
import {
  classifyResumeRelease,
  createReleaseGuard,
  type ResumeReleaseAttempt,
  type ThreadResume,
} from "@t3tools/client-runtime/resume-command";
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

const guardRelease = createReleaseGuard();

/**
 * Copies the thread's resume command at once. When T3 holds the session, it then asks the server
 * to let go, once at a time per thread, and says so only if it released something or was refused.
 */
export async function copyResumeCommand(
  resume: ThreadResume,
  threadKey: string,
  releaseHold: () => Promise<ResumeReleaseAttempt>,
): Promise<void> {
  const copied = await copyCommandToClipboard(resume.command);
  if (!copied || !resume.releaseFirst) return;
  const attempt = await guardRelease(threadKey, releaseHold);
  if (attempt === undefined) return;
  const outcome = classifyResumeRelease(attempt);
  if (outcome === "nothing") return;
  if (outcome === "released") {
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
        ("failure" in attempt ? thrownErrorMessage(attempt.failure, m) : null) ??
        m(msg("The command is copied, but Codex may refuse it.")),
    }),
  );
}
