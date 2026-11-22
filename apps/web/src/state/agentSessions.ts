import { WS_METHODS } from "@t3tools/contracts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";

import { connectionAtomRuntime } from "../connection/runtime";

export const agentSessionScan = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:agent-sessions:scan",
  tag: WS_METHODS.agentSessionsScan,
  staleTimeMs: 30_000,
  idleTtlMs: 5 * 60_000,
});

export const agentSessionImport = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:agent-sessions:import",
  tag: WS_METHODS.agentSessionsImport,
});

export const agentSessionList = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:agent-sessions:list",
  tag: WS_METHODS.agentSessionsList,
  // Sessions change while the user works in a terminal. A list older than this is refetched when
  // it mounts; the dialog also refetches on window focus.
  staleTimeMs: 10_000,
  idleTtlMs: 60_000,
});

export const agentSessionImportOne = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:agent-sessions:import-one",
  tag: WS_METHODS.agentSessionsImportOne,
});
