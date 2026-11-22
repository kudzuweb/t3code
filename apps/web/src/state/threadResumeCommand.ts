import { resolveThreadResumeCommand } from "@t3tools/client-runtime/resume-command";
import type { ScopedThreadRef } from "@t3tools/contracts";

import { appAtomRegistry } from "../rpc/atomRegistry";
import { useQuietThreadProjection } from "./entities";
import { environmentThreadDetails } from "./threads";

type ThreadStructureProjection = NonNullable<
  ReturnType<typeof useQuietThreadProjection>
>["projection"];

function resumeCommandFromProjection(
  projection: ThreadStructureProjection | undefined,
  directory: string | null,
): string | null {
  if (projection === undefined) return null;
  return resolveThreadResumeCommand({
    activeProviderThreadId: projection.thread.activeProviderThreadId,
    providerThreads: projection.providerThreads,
    directory,
  });
}

/** The terminal command that continues the thread's session, once its detail has loaded. */
export function useThreadResumeCommand(
  ref: ScopedThreadRef | null,
  directory: string | null,
): string | null {
  return resumeCommandFromProjection(useQuietThreadProjection(ref)?.projection, directory);
}

/** Reads the command once, for a handler that does not subscribe to the thread. */
export function readThreadResumeCommand(
  ref: ScopedThreadRef,
  directory: string | null,
): string | null {
  return resumeCommandFromProjection(
    appAtomRegistry.get(environmentThreadDetails.quietThreadAtom(ref))?.projection,
    directory,
  );
}
