import {
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as CodexSchema from "effect-codex-app-server/schema";

import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import type { TranscriptSignature, TurnsAfterAnchor } from "./AgentSessionScanner.ts";
import { codexTurnsAfter } from "./CodexTurns.ts";
import * as ProjectService from "./ProjectService.ts";

const NATIVE_THREAD_READ_TIMEOUT = "2 minutes";

const decodeCodexThread = Schema.decodeUnknownOption(
  Schema.Struct({
    turns: Schema.Array(CodexSchema.V2ThreadReadResponse__Turn),
    path: Schema.optional(Schema.NullOr(Schema.String)),
  }),
);

export class CodexSessionReadError extends Schema.TaggedError<CodexSessionReadError>()(
  "CodexSessionReadError",
  { cause: Schema.Defect() },
) {}

/**
 * Reads what a Codex thread gained after an anchor through the Codex adapter's own session, which
 * asks the app-server for the thread with its turns. Opening that session and reading start no
 * turn, so no model is called.
 */
export class CodexSessionReader extends Context.Service<
  CodexSessionReader,
  {
    readonly readTurnsAfter: (input: {
      readonly thread: Pick<
        OrchestrationV2AppThread,
        "id" | "projectId" | "modelSelection" | "runtimeMode" | "interactionMode" | "worktreePath"
      >;
      readonly providerThread: OrchestrationV2ProviderThread;
      readonly anchorNativeId: string;
    }) => Effect.Effect<
      /** `rolloutPath` is the rollout file Codex says holds the thread, which `rolloutSignature` watches. */
      TurnsAfterAnchor & { readonly rolloutPath?: string },
      CodexSessionReadError
    >;
    /**
     * What the rollout looked like: its size, and the newest change to it or to the folder holding
     * it, which a rewrite as a sibling file touches. Undefined when it cannot be read, as when
     * Codex has since compressed it under another name.
     */
    readonly rolloutSignature: (
      rolloutPath: string,
    ) => Effect.Effect<TranscriptSignature | undefined>;
  }
>()("t3/project/CodexSessionReader") {}

export const layer = Layer.effect(
  CodexSessionReader,
  Effect.gen(function* () {
    const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
    const projects = yield* ProjectService.ProjectService;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    return CodexSessionReader.of({
      rolloutSignature: Effect.fn("CodexSessionReader.rolloutSignature")(
        function* (rolloutPath) {
          const modifiedAt = (info: FileSystem.File.Info) =>
            info.mtime._tag === "Some" ? info.mtime.value.getTime() : 0;
          const file = yield* fileSystem.stat(rolloutPath);
          const folder = yield* fileSystem.stat(path.dirname(rolloutPath));
          return {
            size: Number(file.size),
            mtimeMs: Math.max(modifiedAt(file), modifiedAt(folder)),
          };
        },
        Effect.orElseSucceed(() => undefined),
      ),
      readTurnsAfter: Effect.fn("CodexSessionReader.readTurnsAfter")(
        function* ({ thread, providerThread, anchorNativeId }) {
          const nativeThreadId = providerThread.nativeThreadRef?.nativeId;
          if (nativeThreadId == null) return { _tag: "NoTranscript" } as const;
          const project = yield* projects.getById(thread.projectId);
          const cwd = thread.worktreePath ?? Option.getOrUndefined(project)?.workspaceRoot;
          if (cwd === undefined) return { _tag: "NoTranscript" } as const;
          const providerSessionId = yield* idAllocator.allocate.providerSession({
            providerInstanceId: providerThread.providerInstanceId,
            threadId: thread.id,
          });
          const adapter = yield* registry.get(providerThread.providerInstanceId);
          const snapshot = yield* adapter
            .openSession({
              threadId: thread.id,
              providerSessionId,
              modelSelection: thread.modelSelection,
              runtimePolicy: {
                runtimeMode: thread.runtimeMode,
                interactionMode: thread.interactionMode,
                cwd,
              },
              initialNativeThreadId: nativeThreadId,
              ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
                ? {}
                : {
                    initialProviderItemIdentityVersion:
                      providerThread.nativeMetadata.itemIdentityVersion,
                  }),
            })
            .pipe(
              Effect.flatMap((session) => session.readThreadSnapshot({ providerThread })),
              Effect.timeout(NATIVE_THREAD_READ_TIMEOUT),
              Effect.scoped,
            );
          const codexThread = decodeCodexThread(snapshot.providerPayload);
          if (Option.isNone(codexThread)) return { _tag: "NoTranscript" } as const;
          return {
            ...codexTurnsAfter(
              codexThread.value,
              anchorNativeId,
              DateTime.formatIso(yield* DateTime.now),
            ),
            ...(codexThread.value.path == null ? {} : { rolloutPath: codexThread.value.path }),
          };
        },
        Effect.mapError((cause) => new CodexSessionReadError({ cause })),
      ),
    });
  }),
);
