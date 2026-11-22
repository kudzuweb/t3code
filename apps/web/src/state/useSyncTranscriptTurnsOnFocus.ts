import { useAtomValue } from "@effect/atom-react";
import { shouldSyncTranscriptTurns } from "@t3tools/client-runtime/transcript-turns";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { useEffect } from "react";

import { useStableCallback } from "../hooks/useStableCallback";
import { useQuietThreadProjection } from "./entities";
import { orchestrationEnvironment } from "./orchestration";
import { useAtomCommand } from "./use-atom-command";

const { syncTranscriptTurns } = orchestrationEnvironment;

/**
 * Brings the thread up to date with turns made in its session outside T3 whenever the window
 * regains focus. The server already syncs when the thread is opened; focus does not resubscribe.
 */
export function useSyncTranscriptTurnsOnFocus(ref: ScopedThreadRef | null): void {
  const run = useAtomCommand(syncTranscriptTurns, { reportFailure: false });
  const permitted = useAtomValue(syncTranscriptTurns.permissionAtom(ref?.environmentId ?? null));
  const projection = useQuietThreadProjection(ref)?.projection;
  const syncable =
    ref !== null &&
    permitted &&
    projection !== undefined &&
    shouldSyncTranscriptTurns({
      activeProviderThreadId: projection.thread.activeProviderThreadId,
      providerThreads: projection.providerThreads,
    });

  const sync = useStableCallback(() => {
    if (ref !== null) {
      void run({ environmentId: ref.environmentId, input: { threadIds: [ref.threadId] } });
    }
  });

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- window focus has no React source; the platform's focus wakeup stream only reaches the connection layer
  useEffect(() => {
    if (!syncable) return;
    window.addEventListener("focus", sync);
    return () => window.removeEventListener("focus", sync);
  }, [syncable, sync]);
}
