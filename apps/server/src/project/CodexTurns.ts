import type {
  OrchestrationV2ProviderTurn,
  OrchestrationV2TranscriptTurn,
} from "@t3tools/contracts";

import type { AgentSessionThreadMessage, TurnsAfterAnchor } from "./AgentSessionScanner.ts";

export type CodexTurnStatus = "inProgress" | "completed" | "interrupted" | "failed";

/** The one rule for a Codex turn being done, which the import and the sync both apply. */
export const isEndedCodexTurnStatus = (status: CodexTurnStatus) => status !== "inProgress";

/** The Codex turn id T3 stored for a turn that ended; null when the turn never reached Codex. */
export const codexTurnCursor = (turn: OrchestrationV2ProviderTurn): string | null =>
  turn.nativeTurnRef?.driver === "codex" &&
  turn.nativeTurnRef.strength === "strong" &&
  turn.nativeTurnRef.nativeId !== null
    ? turn.nativeTurnRef.nativeId
    : null;

/**
 * The turns of a Codex rollout after the anchor turn id. `messages` are the rollout's visible
 * messages, each naming its turn, with a turn still running already left out. A message sent while
 * a turn runs is a prompt of its own. Codex numbers items by position in history, so a recorded
 * message is named by its turn id, its role and its place in the turn.
 */
export function codexTurnsAfter(
  messages: ReadonlyArray<AgentSessionThreadMessage>,
  anchorNativeId: string,
): TurnsAfterAnchor {
  const turnIds: Array<string> = [];
  const messagesByTurn = new Map<string, Array<AgentSessionThreadMessage>>();
  for (const message of messages) {
    if (message.nativeId === undefined) continue;
    const inTurn = messagesByTurn.get(message.nativeId);
    if (inTurn === undefined) {
      turnIds.push(message.nativeId);
      messagesByTurn.set(message.nativeId, [message]);
    } else {
      inTurn.push(message);
    }
  }
  // A rollout from before Codex named its turns has nothing to place a turn by.
  if (turnIds.length === 0) return { _tag: "Unsupported" };
  const anchorIndex = turnIds.indexOf(anchorNativeId);
  if (anchorIndex === -1) return { _tag: "AnchorNotFound" };

  const turns: Array<OrchestrationV2TranscriptTurn> = [];
  for (const turnId of turnIds.slice(anchorIndex + 1)) {
    let prompts = 0;
    let open:
      | { prompt: AgentSessionThreadMessage; replies: Array<AgentSessionThreadMessage> }
      | undefined;
    const closeTurn = () => {
      if (open === undefined) return;
      const lastReply = open.replies.at(-1);
      turns.push({
        userNativeId: `${turnId}:user:${prompts - 1}`,
        userText: open.prompt.text,
        userAt: open.prompt.createdAt,
        assistantNativeId: lastReply === undefined ? null : `${turnId}:assistant:${prompts - 1}`,
        assistantText: open.replies.map((reply) => reply.text).join("\n\n"),
        assistantAt: lastReply?.createdAt ?? null,
        nativeRefId: turnId,
      });
      open = undefined;
    };
    for (const message of messagesByTurn.get(turnId) ?? []) {
      if (message.role === "user") {
        closeTurn();
        prompts += 1;
        open = { prompt: message, replies: [] };
      } else {
        open?.replies.push(message);
      }
    }
    closeTurn();
  }
  return { _tag: "Turns", turns, throughNativeId: turnIds.at(-1) ?? anchorNativeId };
}
