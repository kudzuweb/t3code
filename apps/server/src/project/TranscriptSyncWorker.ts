import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";

import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { forkParked } from "../serverActivation.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as TranscriptSyncService from "./TranscriptSyncService.ts";

/** How long a tick waits after the previous one finishes before it starts. */
export const TRANSCRIPT_SYNC_INTERVAL = Duration.seconds(10);

/** Threads quiet for longer than this are left to the open and focus triggers. */
export const TRANSCRIPT_SYNC_ACTIVITY_WINDOW = Duration.days(30);

/**
 * Keeps threads up to date with turns made in their sessions outside T3, whether or not a client
 * is looking. An unchanged session costs one file stat, so a tick over every recent thread is cheap.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const scanner = yield* AgentSessionScanner.AgentSessionScanner;
    const sync = yield* TranscriptSyncService.TranscriptSyncService;

    const tick = Effect.gen(function* () {
      const now = yield* DateTime.now;
      const candidates = yield* projections.listTranscriptSyncCandidates({
        updatedSince: DateTime.subtractDuration(now, TRANSCRIPT_SYNC_ACTIVITY_WINDOW),
      });
      const supported = candidates.filter((candidate) =>
        scanner.supportsTranscriptSync(candidate.driver),
      );
      if (supported.length > 0) yield* sync.pollMany(supported);
    }).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterruptsOnly(cause),
        (cause) => Effect.logWarning("Transcript sync tick failed", { cause }),
      ),
    );

    yield* forkParked(
      tick.pipe(Effect.repeat(Schedule.spaced(TRANSCRIPT_SYNC_INTERVAL)), Effect.asVoid),
    );
  }),
);
