import type { ScopedProjectRef } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import { appAtomRegistry } from "~/rpc/atomRegistry";

/**
 * Whether the Resume session dialog is open, and for which project. A request without a project
 * uses the project on screen. The palette, the sidebar menu and the keybinding all close before
 * the dialog shows, so the app renders it once.
 */
export interface ResumeSessionRequest {
  readonly projectRef: ScopedProjectRef | null;
}

export const resumeSessionRequestAtom = Atom.make<ResumeSessionRequest | null>(null).pipe(
  Atom.keepAlive,
  Atom.withLabel("resume-session:dialog-request"),
);

export function openResumeSessionDialog(projectRef: ScopedProjectRef | null = null): void {
  appAtomRegistry.set(resumeSessionRequestAtom, { projectRef });
}
