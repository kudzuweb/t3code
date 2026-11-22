import type {
  OrchestrationV2ProviderTurn,
  OrchestrationV2TranscriptTurn,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type * as CodexSchema from "effect-codex-app-server/schema";

import type { TurnsAfterAnchor } from "./AgentSessionScanner.ts";

type CodexThreadItem = CodexSchema.V2ThreadReadResponse__ThreadItem;
export type CodexTurnStatus = CodexSchema.V2ThreadReadResponse__TurnStatus;

/** The one rule for a Codex turn being done, which the import and the sync both apply. */
export const isEndedCodexTurnStatus = (status: CodexTurnStatus) => status !== "inProgress";

/** The Codex turn id T3 stored for a turn that ended; null when the turn never reached Codex. */
export const codexTurnCursor = (turn: OrchestrationV2ProviderTurn): string | null =>
  turn.nativeTurnRef?.driver === "codex" &&
  turn.nativeTurnRef.strength === "strong" &&
  turn.nativeTurnRef.nativeId !== null
    ? turn.nativeTurnRef.nativeId
    : null;

const isoOf = (seconds: number | null | undefined, fallback: string) =>
  seconds == null ? fallback : DateTime.formatIso(DateTime.makeUnsafe(seconds * 1000));

const userText = (item: Extract<CodexThreadItem, { readonly type: "userMessage" }>) =>
  item.content
    .flatMap((input) => (input.type === "text" ? [input.text] : []))
    .join("\n")
    .trim();

/**
 * The turns of a Codex thread after the anchor, which names a turn or one of its items. A turn
 * that has not ended is left for the next read with every turn after it. A message sent while a
 * turn runs is a prompt of its own, as it is in the rollout the importer reads.
 */
export function codexTurnsAfter(
  thread: Pick<CodexSchema.V2ThreadReadResponse__Thread, "turns">,
  anchorNativeId: string,
  fallbackTimestamp: string,
): TurnsAfterAnchor {
  const anchorIndex = thread.turns.findIndex(
    (turn) => turn.id === anchorNativeId || turn.items.some((item) => item.id === anchorNativeId),
  );
  if (anchorIndex === -1) return { _tag: "AnchorNotFound" };

  const turns: Array<OrchestrationV2TranscriptTurn> = [];
  let throughNativeId = anchorNativeId;
  for (const codexTurn of thread.turns.slice(anchorIndex + 1)) {
    if (!isEndedCodexTurnStatus(codexTurn.status)) break;
    const startedAt = isoOf(codexTurn.startedAt, fallbackTimestamp);
    const endedAt = isoOf(codexTurn.completedAt, startedAt);
    let open: { userNativeId: string; userText: string; replies: Array<string> } | undefined;
    let lastReplyId: string | null = null;
    const closeTurn = () => {
      if (open === undefined) return;
      turns.push({
        userNativeId: open.userNativeId,
        userText: open.userText,
        userAt: startedAt,
        assistantNativeId: lastReplyId,
        assistantText: open.replies.join("\n\n"),
        assistantAt: lastReplyId === null ? null : endedAt,
      });
      throughNativeId = lastReplyId ?? open.userNativeId;
      open = undefined;
      lastReplyId = null;
    };
    for (const item of codexTurn.items) {
      if (item.type === "userMessage") {
        closeTurn();
        const text = userText(item);
        if (text.length > 0) open = { userNativeId: item.id, userText: text, replies: [] };
      } else if (item.type === "agentMessage" && open !== undefined && item.text.trim() !== "") {
        open.replies.push(item.text.trim());
        lastReplyId = item.id;
      }
    }
    closeTurn();
  }
  return { _tag: "Turns", turns, throughNativeId };
}
