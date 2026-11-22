import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { runClaudeWorker } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";

const decodeChain = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(Schema.String)));

export class ClaudeSessionChainError extends Schema.TaggedError<ClaudeSessionChainError>()(
  "ClaudeSessionChainError",
  { cause: Schema.Defect() },
) {}

/**
 * The uuids of a Claude session's live conversation, in order, as Claude Code's own session
 * reader reports them. Which branch is live after a rewind or fork is Claude's decision.
 */
export class ClaudeSessionChain extends Context.Service<
  ClaudeSessionChain,
  {
    readonly read: (input: {
      readonly sessionId: string;
      readonly environment: NodeJS.ProcessEnv;
      /** The project the session belongs to, so Claude reads the same file as the caller. */
      readonly directory?: string;
    }) => Effect.Effect<ReadonlyArray<string>, ClaudeSessionChainError>;
  }
>()("t3/project/ClaudeSessionChain") {}

export const layer = Layer.effect(
  ClaudeSessionChain,
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    return ClaudeSessionChain.of({
      read: Effect.fn("ClaudeSessionChain.read")(function* ({ sessionId, environment, directory }) {
        return yield* runClaudeWorker({
          method: "getSessionUuids",
          sessionId,
          environment,
          ...(directory === undefined ? {} : { options: { dir: directory } }),
        }).pipe(
          Effect.flatMap(decodeChain),
          Effect.provideService(Path.Path, path),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.mapError((cause) => new ClaudeSessionChainError({ cause })),
        );
      }),
    });
  }),
);
