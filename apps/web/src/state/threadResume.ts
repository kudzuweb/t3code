import { useAtomValue } from "@effect/atom-react";
import { resolveThreadResume, type ThreadResume } from "@t3tools/client-runtime/resume-command";
import type { ScopedThreadRef, ServerSettings } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import { appAtomRegistry } from "../rpc/atomRegistry";
import { useQuietThreadProjection } from "./entities";
import { serverEnvironment } from "./server";
import { environmentThreadDetails } from "./threads";

const NO_SETTINGS_ATOM = Atom.make<ServerSettings | null>(null);

type ThreadStructureProjection = NonNullable<
  ReturnType<typeof useQuietThreadProjection>
>["projection"];
type ProviderInstances = Parameters<typeof resolveThreadResume>[0]["providerInstances"];

function resumeCommandFromProjection(
  projection: ThreadStructureProjection | undefined,
  providerInstances: ProviderInstances,
  directory: string | null,
): ThreadResume | null {
  if (projection === undefined) return null;
  return resolveThreadResume({
    activeProviderThreadId: projection.thread.activeProviderThreadId,
    providerThreads: projection.providerThreads,
    providerSessions: projection.providerSessions,
    providerInstances,
    directory,
  });
}

/** The terminal command that continues the thread's session, once its detail has loaded. */
export function useThreadResume(
  ref: ScopedThreadRef | null,
  directory: string | null,
): ThreadResume | null {
  const projection = useQuietThreadProjection(ref)?.projection;
  const settings = useAtomValue(
    ref === null ? NO_SETTINGS_ATOM : serverEnvironment.settingsValueAtom(ref.environmentId),
  );
  return resumeCommandFromProjection(projection, settings?.providerInstances ?? {}, directory);
}

/** Reads the command once, for a handler that does not subscribe to the thread. */
export function readThreadResume(
  ref: ScopedThreadRef,
  directory: string | null,
): ThreadResume | null {
  return resumeCommandFromProjection(
    appAtomRegistry.get(environmentThreadDetails.quietThreadAtom(ref))?.projection,
    appAtomRegistry.get(serverEnvironment.settingsValueAtom(ref.environmentId))
      ?.providerInstances ?? {},
    directory,
  );
}
