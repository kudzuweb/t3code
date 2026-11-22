import { assert, describe, it } from "@effect/vitest";
import type * as CodexSchema from "effect-codex-app-server/schema";

import { codexTurnsAfter } from "./CodexTurns.ts";

type Turn = CodexSchema.V2ThreadReadResponse__Turn;
type Item = CodexSchema.V2ThreadReadResponse__ThreadItem;

const user = (id: string, text: string): Item => ({
  type: "userMessage",
  id,
  content: [{ type: "text", text, text_elements: [] }],
});
const agent = (id: string, text: string, phase: "commentary" | "final_answer" = "final_answer") =>
  ({ type: "agentMessage", id, text, phase }) satisfies Item;
const command: Item = {
  type: "commandExecution",
  id: "item-command",
  command: "ls",
  commandActions: [],
  cwd: "/tmp/project",
  source: "agent",
  status: "completed",
} as unknown as Item;
const turn = (
  id: string,
  items: ReadonlyArray<Item>,
  status: Turn["status"] = "completed",
): Turn => ({
  id,
  status,
  items,
  startedAt: 1_784_000_000,
  completedAt: status === "inProgress" ? null : 1_784_000_030,
});

const fallback = "2026-11-22T12:00:00.000Z";
const after = (turns: ReadonlyArray<Turn>, anchor: string) =>
  codexTurnsAfter({ turns }, anchor, fallback);

describe("codexTurnsAfter", () => {
  const shown = turn("turn-shown", [user("u-shown", "Shown"), agent("a-shown", "Shown reply")]);

  it("returns the turns after the anchor turn, with a prompt and the reply text of each", () => {
    const result = after(
      [
        shown,
        turn("turn-1", [
          user("u-1", "First from the terminal"),
          command,
          agent("a-1-note", "Looking.", "commentary"),
          agent("a-1", "First answer"),
        ]),
        turn("turn-2", [user("u-2", "Second"), agent("a-2", "Second answer")]),
      ],
      "turn-shown",
    );
    assert.deepStrictEqual(result, {
      _tag: "Turns",
      turns: [
        {
          userNativeId: "u-1",
          userText: "First from the terminal",
          userAt: "2026-07-14T03:33:20.000Z",
          assistantNativeId: "a-1",
          assistantText: "Looking.\n\nFirst answer",
          assistantAt: "2026-07-14T03:33:50.000Z",
        },
        {
          userNativeId: "u-2",
          userText: "Second",
          userAt: "2026-07-14T03:33:20.000Z",
          assistantNativeId: "a-2",
          assistantText: "Second answer",
          assistantAt: "2026-07-14T03:33:50.000Z",
        },
      ],
      throughNativeId: "a-2",
    });
  });

  it("finds an anchor that names an item of a turn, as a recorded terminal turn leaves", () => {
    const result = after(
      [shown, turn("turn-1", [user("u-1", "Next"), agent("a-1", "Answer")])],
      "a-shown",
    );
    assert.equal(result._tag === "Turns" && result.turns.length, 1);
  });

  it("holds back a turn still in progress and every turn after it", () => {
    const result = after(
      [
        shown,
        turn("turn-1", [user("u-1", "Done"), agent("a-1", "Done reply")]),
        turn("turn-2", [user("u-2", "Running"), agent("a-2", "Partial")], "inProgress"),
      ],
      "turn-shown",
    );
    assert.equal(result._tag === "Turns" && result.turns.length, 1);
    assert.equal(result._tag === "Turns" && result.throughNativeId, "a-1");
  });

  it("counts an interrupted turn and a failed turn without a reply as finished", () => {
    const result = after(
      [
        shown,
        turn("turn-1", [user("u-1", "Stopped"), agent("a-1", "Half")], "interrupted"),
        turn("turn-2", [user("u-2", "Broke")], "failed"),
      ],
      "turn-shown",
    );
    assert.deepStrictEqual(
      result._tag === "Turns" && result.turns.map((t) => [t.userNativeId, t.assistantNativeId]),
      [
        ["u-1", "a-1"],
        ["u-2", null],
      ],
    );
    assert.equal(result._tag === "Turns" && result.throughNativeId, "u-2");
  });

  it("returns no turns, still through the anchor, when nothing follows it", () => {
    assert.deepStrictEqual(after([shown], "a-shown"), {
      _tag: "Turns",
      turns: [],
      throughNativeId: "a-shown",
    });
  });

  it("reports a missing anchor, as after a rollback, so nothing is written", () => {
    assert.deepStrictEqual(after([shown], "turn-reverted"), { _tag: "AnchorNotFound" });
  });

  it("keeps a message sent while a turn ran as a prompt of its own", () => {
    const result = after(
      [
        shown,
        turn("turn-1", [
          user("u-1", "Start"),
          agent("a-1", "Working"),
          user("u-steer", "Also this"),
          agent("a-steer", "Done both"),
        ]),
      ],
      "turn-shown",
    );
    assert.deepStrictEqual(result._tag === "Turns" && result.turns.map((t) => t.userNativeId), [
      "u-1",
      "u-steer",
    ]);
  });

  it("leaves out a prompt with no text, such as an image, and the replies that follow it", () => {
    const imageOnly: Item = {
      type: "userMessage",
      id: "u-image",
      content: [{ type: "image", url: "https://example.com/a.png" }],
    };
    const result = after(
      [shown, turn("turn-1", [imageOnly, agent("a-1", "A picture")])],
      "turn-shown",
    );
    assert.deepStrictEqual(result, { _tag: "Turns", turns: [], throughNativeId: "turn-shown" });
  });
});
