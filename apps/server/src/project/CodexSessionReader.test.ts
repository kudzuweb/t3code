import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";

import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as CodexSessionReader from "./CodexSessionReader.ts";
import * as ProjectService from "./ProjectService.ts";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const thread = {
  id: ThreadId.make("thread:codex-read"),
  projectId: ProjectId.make("project:codex-read"),
  modelSelection: { instanceId, model: "gpt-5.6-mini" },
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  worktreePath: null,
};
const providerThread = (now: DateTime.Utc): OrchestrationV2ProviderThread => ({
  id: ProviderThreadId.make("provider-thread:codex-read"),
  driver,
  providerInstanceId: instanceId,
  providerSessionId: null,
  appThreadId: thread.id,
  ownerNodeId: null,
  nativeThreadRef: { driver, nativeId: "codex-thread-1", strength: "strong" },
  nativeConversationHeadRef: null,
  status: "idle",
  firstRunOrdinal: null,
  lastRunOrdinal: null,
  handoffIds: [],
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
});

// What `thread/read` with `includeTurns` returns for the thread.
const codexThread = {
  id: "codex-thread-1",
  turns: [
    {
      id: "turn-shown",
      status: "completed",
      items: [
        { type: "userMessage", id: "u-shown", content: [{ type: "text", text: "Shown" }] },
        { type: "agentMessage", id: "a-shown", text: "Shown reply" },
      ],
    },
    {
      id: "turn-terminal",
      status: "completed",
      startedAt: 1_784_000_000,
      completedAt: 1_784_000_030,
      items: [
        { type: "userMessage", id: "u-terminal", content: [{ type: "text", text: "Terminal" }] },
        { type: "agentMessage", id: "a-terminal", text: "Terminal reply" },
      ],
    },
  ],
};

const makeReader = Effect.fn("CodexSessionReader.test.makeReader")(function* (payload: unknown) {
  const calls = yield* Ref.make<Array<string>>([]);
  const closed = yield* Ref.make(false);
  const record = (call: string) => Ref.update(calls, (all) => [...all, call]);
  const adapter = {
    instanceId,
    driver,
    // Only a session open and a thread read exist, so a turn start would defect.
    openSession: (input: ProviderAdapter.ProviderAdapterV2OpenSessionInput) =>
      Effect.gen(function* () {
        yield* record(`open:${input.initialNativeThreadId}:${input.runtimePolicy.cwd}`);
        yield* Scope.addFinalizer(yield* Scope.Scope, Ref.set(closed, true));
        return {
          readThreadSnapshot: () => record("read").pipe(Effect.as({ providerPayload: payload })),
        };
      }),
  } as unknown as ProviderAdapter.ProviderAdapterV2["Service"];
  const reader = yield* CodexSessionReader.CodexSessionReader.pipe(
    Effect.provide(
      CodexSessionReader.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            ProviderAdapterRegistry.layerFromAdapters([adapter]),
            IdAllocator.layer,
            Layer.mock(ProjectService.ProjectService)({
              getById: () => Effect.succeedSome({ workspaceRoot: "/work/project" } as never),
            }),
          ),
        ),
      ),
    ),
  );
  return { reader, calls, closed };
});

it.effect(
  "reads a Codex thread through a read-only session and returns the turns after the anchor",
  () =>
    Effect.gen(function* () {
      const { reader, calls, closed } = yield* makeReader(codexThread);
      const now = yield* DateTime.now;

      const result = yield* reader.readTurnsAfter({
        thread,
        providerThread: providerThread(now),
        anchorNativeId: "turn-shown",
      });

      assert.deepStrictEqual(result, {
        _tag: "Turns",
        turns: [
          {
            userNativeId: "u-terminal",
            userText: "Terminal",
            userAt: "2026-07-14T03:33:20.000Z",
            assistantNativeId: "a-terminal",
            assistantText: "Terminal reply",
            assistantAt: "2026-07-14T03:33:50.000Z",
          },
        ],
        throughNativeId: "a-terminal",
      });
      assert.deepStrictEqual(yield* Ref.get(calls), ["open:codex-thread-1:/work/project", "read"]);
      assert.equal(yield* Ref.get(closed), true);
    }),
);

it.effect(
  "reports no transcript when the session returns something that is not a Codex thread",
  () =>
    Effect.gen(function* () {
      const { reader } = yield* makeReader({ unexpected: true });
      const result = yield* reader.readTurnsAfter({
        thread,
        providerThread: providerThread(yield* DateTime.now),
        anchorNativeId: "turn-shown",
      });
      assert.deepStrictEqual(result, { _tag: "NoTranscript" });
    }),
);
