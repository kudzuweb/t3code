import { useAtomValue } from "@effect/atom-react";
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  buildResumeSessionRows,
  filterResumeSessionRows,
  resumeSessionErrorMessage,
  type ResumeSessionRow,
} from "@t3tools/client-runtime/resume-session";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type { ScopedProjectRef } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { T, useGT, useMessages } from "gt-react";
import { GitBranchIcon } from "lucide-react";
import { useEffect, useRef, useState, type KeyboardEvent, type ReactNode } from "react";

import { useHandleNewThread } from "~/hooks/useHandleNewThread";
import { cn } from "~/lib/utils";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { agentSessionImportOne, agentSessionList } from "~/state/agentSessions";
import { useEnvironmentQuery } from "~/state/query";
import { useAtomCommand } from "~/state/use-atom-command";
import { buildThreadRouteParams } from "~/threadRoutes";

import { CommandPaletteContent } from "../CommandPaletteContent";
import { RelativeTime } from "../RelativeTime";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { CommandDialog, CommandDialogPopup } from "../ui/command";
import { ScrollArea } from "../ui/scroll-area";
import { Spinner } from "../ui/spinner";
import { resumeSessionRequestAtom } from "./resumeSessionDialogState";

const closeDialog = () => appAtomRegistry.set(resumeSessionRequestAtom, null);

const scrollSelectedRowIntoView = (row: HTMLElement | null) => {
  row?.scrollIntoView({ block: "nearest" });
};

export function ResumeSessionDialogHost() {
  const gt = useGT();
  const request = useAtomValue(resumeSessionRequestAtom);
  if (request === null) return null;
  return (
    <CommandDialog
      open
      onOpenChange={(open) => {
        if (!open) closeDialog();
      }}
    >
      <CommandDialogPopup
        aria-label={gt("Resume session")}
        className="h-105 overflow-hidden"
        data-testid="resume-session-dialog"
        onBackdropPointerDown={closeDialog}
      >
        <ResumeSessionDialog requestedProjectRef={request.projectRef} />
      </CommandDialogPopup>
    </CommandDialog>
  );
}

function ResumeSessionDialog(props: { readonly requestedProjectRef: ScopedProjectRef | null }) {
  const { activeThread, activeDraftThread } = useHandleNewThread();
  const onScreen = activeThread ?? activeDraftThread;
  const projectRef =
    props.requestedProjectRef ??
    (onScreen ? scopeProjectRef(onScreen.environmentId, onScreen.projectId) : null);
  const gt = useGT();
  if (projectRef === null) {
    return (
      <ResumeSessionFrame search="" onSearchChange={() => {}} disabled>
        <Notice>{gt("Open a project to resume one of its sessions.")}</Notice>
      </ResumeSessionFrame>
    );
  }
  return <ProjectSessions projectRef={projectRef} />;
}

function ProjectSessions(props: { readonly projectRef: ScopedProjectRef }) {
  const { environmentId, projectId } = props.projectRef;
  const gt = useGT();
  const messages = useMessages();
  const navigate = useNavigate();
  const canResume = useAtomValue(agentSessionImportOne.permissionAtom(environmentId));
  const importOne = useAtomCommand(agentSessionImportOne, { reportFailure: false });
  const sessions = useEnvironmentQuery(
    agentSessionList({ environmentId, input: { projectId } }),
    messages,
  );
  const [search, setSearch] = useState("");
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [resumingKey, setResumingKey] = useState<string | null>(null);
  const importingRef = useRef(false);
  const [failure, setFailure] = useState<string | null>(null);

  const rows = buildResumeSessionRows(sessions.data?.sessions ?? []);
  const visibleRows = filterResumeSessionRows(rows, search);
  const [searchSeen, setSearchSeen] = useState(search);
  if (searchSeen !== search) {
    setSearchSeen(search);
    setSelectedIndex(0);
    setFailure(null);
  }
  const activeIndex = Math.min(selectedIndex, Math.max(visibleRows.length - 1, 0));
  const select = (index: number) => {
    if (index === activeIndex) return;
    setSelectedIndex(index);
    setFailure(null);
  };

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- sessions change in a terminal while this window is away, so returning to it refetches the list
  useEffect(() => {
    window.addEventListener("focus", sessions.refresh);
    return () => window.removeEventListener("focus", sessions.refresh);
  }, [sessions.refresh]);

  const openThread = (threadId: NonNullable<ResumeSessionRow["threadId"]>) => {
    closeDialog();
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(scopeThreadRef(environmentId, threadId)),
    });
  };

  const choose = async (row: ResumeSessionRow) => {
    if (importingRef.current) return;
    setFailure(null);
    if (row.state === "open" && row.threadId !== null) {
      openThread(row.threadId);
      return;
    }
    importingRef.current = true;
    setResumingKey(row.key);
    const openedFor = appAtomRegistry.get(resumeSessionRequestAtom);
    const result = await importOne({
      environmentId,
      input: {
        projectId,
        providerInstanceId: row.session.providerInstanceId,
        providerSessionId: row.session.providerSessionId,
      },
    });
    importingRef.current = false;
    setResumingKey(null);
    // The server import finishes either way; a dialog closed or reopened meanwhile stays put.
    if (appAtomRegistry.get(resumeSessionRequestAtom) !== openedFor) return;
    if (result._tag === "Success") {
      openThread(result.value.threadId);
      return;
    }
    if (isAtomCommandInterrupted(result)) return;
    setFailure(resumeSessionErrorMessage(squashAtomCommandFailure(result), messages));
    sessions.refresh();
  };

  const canChoose = canResume && sessions.data !== null;
  const status = !canResume ? (
    <Notice>{gt("You do not have permission to resume sessions in this environment.")}</Notice>
  ) : sessions.data === null && sessions.error !== null ? (
    <div className="m-auto flex flex-col items-center gap-3 px-6 text-center text-sm">
      <p className="text-destructive">{sessions.error}</p>
      <Button size="sm" variant="outline" onClick={sessions.refresh}>
        <T>Retry</T>
      </Button>
    </div>
  ) : sessions.data === null ? (
    <div className="m-auto flex items-center gap-2 text-muted-foreground text-sm">
      <Spinner size="sm" /> <T>Loading sessions…</T>
    </div>
  ) : visibleRows.length === 0 ? (
    <Notice>
      {rows.length === 0 ? gt("No sessions for this project") : gt("No matching sessions")}
    </Notice>
  ) : null;

  return (
    <ResumeSessionFrame
      disabled={!canChoose}
      search={search}
      onSearchChange={setSearch}
      onKeyDown={(event) => {
        if (event.nativeEvent.isComposing || event.keyCode === 229) return;
        if (visibleRows.length === 0) return;
        if (event.key === "ArrowDown") {
          event.preventDefault();
          select((activeIndex + 1) % visibleRows.length);
        } else if (event.key === "ArrowUp") {
          event.preventDefault();
          select((activeIndex - 1 + visibleRows.length) % visibleRows.length);
        } else if (event.key === "Enter" && canChoose) {
          event.preventDefault();
          const row = visibleRows[activeIndex];
          if (row) void choose(row);
        }
      }}
    >
      {failure !== null ? (
        <p className="shrink-0 border-b px-3 py-2 text-destructive text-xs" role="alert">
          {failure}
        </p>
      ) : null}
      {status ?? (
        <ScrollArea className="min-h-0 flex-1" scrollFade>
          <div className="py-1">
            {visibleRows.map((row, index) => (
              <SessionRow
                key={row.key}
                row={row}
                selected={index === activeIndex}
                resuming={resumingKey === row.key}
                disabled={!canChoose || resumingKey !== null}
                onHover={() => select(index)}
                onChoose={() => void choose(row)}
              />
            ))}
            {sessions.data?.truncated ? (
              <p className="px-3 py-2 text-muted-foreground text-xs">
                <T>This list may be incomplete.</T>
              </p>
            ) : null}
          </div>
        </ScrollArea>
      )}
    </ResumeSessionFrame>
  );
}

function ResumeSessionFrame(props: {
  readonly search: string;
  readonly onSearchChange: (search: string) => void;
  readonly disabled?: boolean;
  readonly onKeyDown?: (event: KeyboardEvent<HTMLInputElement>) => void;
  readonly children: ReactNode;
}) {
  const gt = useGT();
  return (
    <CommandPaletteContent
      aria-label={gt("Resume session")}
      footerActionLabel={gt("Resume")}
      inputProps={{
        disabled: props.disabled,
        placeholder: gt("Search sessions…"),
        onKeyDown: props.onKeyDown,
      }}
      mode="none"
      onValueChange={props.onSearchChange}
      panelSize="fill"
      testId="resume-session"
      value={props.search}
    >
      {props.children}
    </CommandPaletteContent>
  );
}

function Notice(props: { readonly children: ReactNode }) {
  return (
    <div className="flex flex-1 items-center justify-center px-6 text-center text-muted-foreground text-sm">
      {props.children}
    </div>
  );
}

function SessionRow(props: {
  readonly row: ResumeSessionRow;
  readonly selected: boolean;
  readonly resuming: boolean;
  readonly disabled: boolean;
  readonly onHover: () => void;
  readonly onChoose: () => void;
}) {
  const { row } = props;
  return (
    <button
      type="button"
      ref={props.selected ? scrollSelectedRowIntoView : undefined}
      className={cn(
        "flex w-full min-w-0 flex-col gap-0.5 px-3 py-1.5 text-left hover:bg-accent/60 disabled:pointer-events-none",
        props.selected && "bg-accent text-accent-foreground",
      )}
      disabled={props.disabled}
      onMouseEnter={props.onHover}
      onClick={props.onChoose}
    >
      <span className="flex min-w-0 items-center gap-2">
        <span className="min-w-0 flex-1 truncate text-sm">{row.title}</span>
        {props.resuming ? <Spinner size="sm" /> : null}
        {row.state === "open" ? (
          <Badge size="sm" variant="secondary">
            <T>In T3</T>
          </Badge>
        ) : null}
      </span>
      <span className="flex min-w-0 items-center gap-2 text-muted-foreground text-xs">
        <span className="shrink-0">{row.providerLabel}</span>
        {row.lastActiveAt !== null ? (
          <span className="shrink-0">
            <RelativeTime timestamp={row.lastActiveAt} />
          </span>
        ) : null}
        {row.branch !== null ? (
          <span className="flex min-w-0 items-center gap-1">
            <GitBranchIcon className="size-3 shrink-0" />
            <span className="truncate">{row.branch}</span>
          </span>
        ) : null}
      </span>
    </button>
  );
}
