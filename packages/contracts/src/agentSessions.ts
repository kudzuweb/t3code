import * as Schema from "effect/Schema";
import {
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

export const AgentSessionSource = Schema.Literals(["claudeAgent", "codex", "pi"]);
export type AgentSessionSource = typeof AgentSessionSource.Type;

export const AgentSessionImportSource = Schema.Struct({
  provider: AgentSessionSource,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
  filePath: TrimmedNonEmptyString,
  size: NonNegativeInt,
  mtimeMs: Schema.NullOr(Schema.Number),
  device: Schema.Number,
  // A decimal string keeps file IDs above 2^53 (common on NTFS) exact. Older
  // records stored a number.
  inode: Schema.NullOr(Schema.Union([Schema.String, Schema.Number])),
  birthtimeMs: Schema.NullOr(Schema.Number),
});
export type AgentSessionImportSource = typeof AgentSessionImportSource.Type;

export const AgentSessionScanInput = Schema.Struct({});
export type AgentSessionScanInput = typeof AgentSessionScanInput.Type;

export const AgentSessionProjectGit = Schema.Struct({
  remoteKey: Schema.NullOr(Schema.String),
  repository: Schema.NullOr(Schema.String),
});
export type AgentSessionProjectGit = typeof AgentSessionProjectGit.Type;

export const AgentSessionProjectCandidate = Schema.Struct({
  path: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString,
  projectId: Schema.optional(ProjectId),
  sources: Schema.Array(AgentSessionSource),
  threadCount: NonNegativeInt,
  lastActiveAt: Schema.NullOr(IsoDateTime),
  alreadyImported: Schema.Boolean,
  git: Schema.optionalKey(Schema.NullOr(AgentSessionProjectGit)),
});
export type AgentSessionProjectCandidate = typeof AgentSessionProjectCandidate.Type;

export const AgentSessionScanResult = Schema.Struct({
  candidates: Schema.Array(AgentSessionProjectCandidate),
  scannedAt: IsoDateTime,
  truncated: Schema.optional(Schema.Boolean),
});
export type AgentSessionScanResult = typeof AgentSessionScanResult.Type;

export const AgentSessionImportInput = Schema.Struct({
  projectId: ProjectId,
  expectedWorkspaceRoot: Schema.optional(TrimmedNonEmptyString),
});
export type AgentSessionImportInput = typeof AgentSessionImportInput.Type;

export class AgentSessionImportProjectNotFoundError extends Schema.TaggedError<AgentSessionImportProjectNotFoundError>()(
  "AgentSessionImportProjectNotFoundError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project '${this.projectId}' does not exist.`;
  }
}

export class AgentSessionImportProjectChangedError extends Schema.TaggedError<AgentSessionImportProjectChangedError>()(
  "AgentSessionImportProjectChangedError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project '${this.projectId}' changed directories. Scan for projects again before importing history.`;
  }
}

export const AgentSessionImportResult = Schema.Struct({
  importedCount: NonNegativeInt,
  skippedCount: NonNegativeInt,
});
export type AgentSessionImportResult = typeof AgentSessionImportResult.Type;

export class AgentSessionScanError extends Schema.TaggedError<AgentSessionScanError>()(
  "AgentSessionScanError",
  {
    operation: Schema.Literals([
      "read-settings",
      "read-projects",
      "read-owners",
      "read-transcript",
      "import-session",
    ]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to scan agent sessions during ${this.operation}.`;
  }
}

export class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

export class AgentSessionNotFoundError extends Schema.TaggedError<AgentSessionNotFoundError>()(
  "AgentSessionNotFoundError",
  {
    projectId: ProjectId,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' was not found in project '${this.projectId}'.`;
  }
}

export class AgentSessionUnreadableError extends Schema.TaggedError<AgentSessionUnreadableError>()(
  "AgentSessionUnreadableError",
  {
    projectId: ProjectId,
    providerSessionId: Schema.String,
    filePath: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' was listed but its transcript could not be read in full.`;
  }
}

export const AgentSessionListInput = Schema.Struct({
  projectId: ProjectId,
});
export type AgentSessionListInput = typeof AgentSessionListInput.Type;

export const AgentSessionWorktree = Schema.Struct({
  path: TrimmedNonEmptyString,
  branch: Schema.NullOr(TrimmedNonEmptyString),
});
export type AgentSessionWorktree = typeof AgentSessionWorktree.Type;

export const AgentSessionSummary = Schema.Struct({
  provider: AgentSessionSource,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString,
  lastActiveAt: Schema.NullOr(IsoDateTime),
  worktree: Schema.NullOr(AgentSessionWorktree),
  /** The T3 thread that already holds this session. */
  threadId: Schema.NullOr(ThreadId),
});
export type AgentSessionSummary = typeof AgentSessionSummary.Type;

export const AgentSessionListResult = Schema.Struct({
  sessions: Schema.Array(AgentSessionSummary),
  truncated: Schema.Boolean,
});
export type AgentSessionListResult = typeof AgentSessionListResult.Type;

export const AgentSessionImportOneInput = Schema.Struct({
  projectId: ProjectId,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
});
export type AgentSessionImportOneInput = typeof AgentSessionImportOneInput.Type;

export const AgentSessionImportOneResult = Schema.Struct({
  threadId: ThreadId,
  imported: Schema.Boolean,
});
export type AgentSessionImportOneResult = typeof AgentSessionImportOneResult.Type;
