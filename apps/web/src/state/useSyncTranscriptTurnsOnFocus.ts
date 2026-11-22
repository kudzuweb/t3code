import {
  syncTranscriptTurnsOnFocus,
  type TranscriptSyncThread,
} from "@t3tools/client-runtime/transcript-turns";
import { useEffect } from "react";

import { useStableCallback } from "../hooks/useStableCallback";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { orchestrationEnvironment } from "./orchestration";
import { useAtomCommand } from "./use-atom-command";

const { syncTranscriptTurns } = orchestrationEnvironment;

/**
 * Brings the given threads up to date with turns made in their sessions outside T3 whenever the
 * window regains focus. The server syncs a thread when it is opened and every few seconds on its
 * own; focus is the moment the user is about to look.
 */
export function useSyncTranscriptTurnsOnFocus(threads: ReadonlyArray<TranscriptSyncThread>): void {
  const run = useAtomCommand(syncTranscriptTurns, { reportFailure: false });

  const sync = useStableCallback(
    () =>
      void syncTranscriptTurnsOnFocus({
        threads,
        canSync: (environmentId) =>
          appAtomRegistry.get(syncTranscriptTurns.permissionAtom(environmentId)),
        send: ({ environmentId, threadIds }) => run({ environmentId, input: { threadIds } }),
      }),
  );

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- window focus has no React source; the platform's focus wakeup stream only reaches the connection layer
  useEffect(() => {
    window.addEventListener("focus", sync);
    return () => window.removeEventListener("focus", sync);
  }, [sync]);
}
