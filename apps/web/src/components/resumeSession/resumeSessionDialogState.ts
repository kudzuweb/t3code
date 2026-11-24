import type { ScopedProjectRef } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import type { DraftId } from "~/composerDraftStore";
import { appAtomRegistry } from "~/rpc/atomRegistry";

/**
 * Whether the Resume session dialog is open, for which project, and for which new-thread draft.
 * With a draft, picking a session turns that draft into the session's thread. A request without a
 * project uses the project on screen.
 */
export interface ResumeSessionRequest {
  readonly projectRef: ScopedProjectRef | null;
  readonly draftId: DraftId | null;
}

export const resumeSessionRequestAtom = Atom.make<ResumeSessionRequest | null>(null).pipe(
  Atom.keepAlive,
  Atom.withLabel("resume-session:dialog-request"),
);

export function openResumeSessionDialog(request: Partial<ResumeSessionRequest> = {}): void {
  appAtomRegistry.set(resumeSessionRequestAtom, { projectRef: null, draftId: null, ...request });
}
