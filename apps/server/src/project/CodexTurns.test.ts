import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";

import {
  parseAgentSessionTranscript,
  type AgentSessionThreadMessage,
} from "./AgentSessionScanner.ts";
import { codexTurnsAfter } from "./CodexTurns.ts";

const message = (
  role: "user" | "assistant",
  turnId: string | undefined,
  text: string,
  second = 0,
): AgentSessionThreadMessage => ({
  role,
  text,
  createdAt: `2026-11-20T10:00:${String(second).padStart(2, "0")}.000Z`,
  ...(turnId === undefined ? {} : { nativeId: turnId }),
});

describe("codexTurnsAfter", () => {
  const shown = [
    message("user", "turn-shown", "Shown"),
    message("assistant", "turn-shown", "Done"),
  ];

  it("names a recorded turn by its turn id, role and place, and its refs by the turn id", () => {
    const result = codexTurnsAfter(
      [
        ...shown,
        message("user", "turn-1", "First", 1),
        message("assistant", "turn-1", "Looking.", 2),
        message("assistant", "turn-1", "First answer", 3),
      ],
      "turn-shown",
    );
    assert.deepStrictEqual(result, {
      _tag: "Turns",
      turns: [
        {
          userNativeId: "turn-1:user:0",
          userText: "First",
          userAt: "2026-11-20T10:00:01.000Z",
          assistantNativeId: "turn-1:assistant:0",
          assistantText: "Looking.\n\nFirst answer",
          assistantAt: "2026-11-20T10:00:03.000Z",
          nativeRefId: "turn-1",
        },
      ],
      throughNativeId: "turn-1",
    });
  });

  it("keeps a message sent while a turn ran as a prompt of its own, under the same turn id", () => {
    const result = codexTurnsAfter(
      [
        ...shown,
        message("user", "turn-1", "Start"),
        message("assistant", "turn-1", "Working"),
        message("user", "turn-1", "Also this"),
        message("assistant", "turn-1", "Both done"),
      ],
      "turn-shown",
    );
    assert.deepStrictEqual(
      result._tag === "Turns" && result.turns.map((t) => [t.userNativeId, t.nativeRefId]),
      [
        ["turn-1:user:0", "turn-1"],
        ["turn-1:user:1", "turn-1"],
      ],
    );
  });

  it("returns no turns, still through the last turn, when nothing follows the anchor", () => {
    assert.deepStrictEqual(codexTurnsAfter(shown, "turn-shown"), {
      _tag: "Turns",
      turns: [],
      throughNativeId: "turn-shown",
    });
  });

  it("does not find an anchor that no message names, as after Codex dropped the turn", () => {
    assert.deepStrictEqual(codexTurnsAfter(shown, "turn-reverted"), { _tag: "AnchorNotFound" });
  });

  it("leaves out a message that names no turn", () => {
    const result = codexTurnsAfter(
      [...shown, message("user", undefined, "Which turn?"), message("user", "turn-1", "Next")],
      "turn-shown",
    );
    assert.equal(result._tag === "Turns" && result.turns.length, 1);
  });
});

describe("a Codex turn still running when its session is imported", () => {
  const record = (value: unknown) => JSON.stringify(value);
  const line = (role: "user" | "assistant", turnId: string, text: string) =>
    record({
      type: "response_item",
      timestamp: "2026-11-20T10:00:00.000Z",
      payload: {
        type: "message",
        role,
        internal_chat_message_metadata_passthrough: { turn_id: turnId },
        content: [{ type: role === "user" ? "input_text" : "output_text", text }],
      },
    });
  const event = (type: string, turnId: string) =>
    record({ type: "event_msg", payload: { type, turn_id: turnId } });
  const rollout = (...lines: ReadonlyArray<string>) =>
    parseAgentSessionTranscript({
      contents: [record({ type: "session_meta", payload: { id: "codex-session" } }), ...lines].join(
        "\n",
      ),
      source: "codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      fallbackSessionId: "unused",
      lastActiveAtMs: Date.parse("2026-11-20T12:00:00.000Z"),
    });
  const finishedFirstTurn = [
    event("task_started", "turn-1"),
    line("user", "turn-1", "First"),
    line("assistant", "turn-1", "First reply"),
    event("task_complete", "turn-1"),
  ];
  const secondTurn = [
    event("task_started", "turn-2"),
    line("user", "turn-2", "Second"),
    line("assistant", "turn-2", "Partial"),
  ];

  it("is left out of the import, and recorded whole by the sync once it completes", () => {
    const imported = rollout(...finishedFirstTurn, ...secondTurn);
    assert.deepStrictEqual(
      imported?.messages.map((entry) => entry.text),
      ["First", "First reply"],
    );
    const anchor = imported?.messages.at(-1)?.nativeId ?? "";
    assert.equal(anchor, "turn-1");

    const completed = rollout(
      ...finishedFirstTurn,
      ...secondTurn,
      line("assistant", "turn-2", "Full reply"),
      event("task_complete", "turn-2"),
    );
    const read = codexTurnsAfter(completed?.messages ?? [], anchor);
    assert.equal(read._tag === "Turns" && read.turns[0]?.assistantText, "Partial\n\nFull reply");
  });

  it("drops every message of the running turn, not only those after its last prompt", () => {
    const imported = rollout(
      ...finishedFirstTurn,
      event("task_started", "turn-2"),
      line("user", "turn-2", "Second"),
      line("assistant", "turn-2", "Working"),
      line("user", "turn-2", "Also this"),
    );
    assert.deepStrictEqual(
      imported?.messages.map((entry) => entry.text),
      ["First", "First reply"],
    );
  });

  it("keeps the whole import when its only turn is the running one", () => {
    assert.equal(rollout(...secondTurn.slice(0, 3))?.messages.length, 2);
  });

  it("counts an interrupted turn as finished", () => {
    const imported = rollout(...finishedFirstTurn, ...secondTurn, event("turn_aborted", "turn-2"));
    assert.equal(imported?.messages.length, 4);
  });
});
