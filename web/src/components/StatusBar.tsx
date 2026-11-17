import type { ReactNode } from "react";
import { useApp } from "../store";

/**
 * Terminal-style footer: connection, endpoint, the active tab's scope, and how
 * the last statement went. Everything here already lives in the store or the
 * catalog query, so the bar adds no state of its own.
 */
export function StatusBar({ connected, busy }: { connected: boolean; busy: boolean }) {
  const { tabs, activeTabId } = useApp();
  const tab = tabs.find((item) => item.id === activeTabId) ?? tabs[0];
  const scope = tab.schema ? `${tab.catalog}.${tab.schema}` : tab.catalog;

  return (
    <footer className="flex h-7 shrink-0 items-center overflow-hidden bg-page font-mono text-[10.5px] text-fg-muted">
      <Cell
        className={
          connected
            ? "flex items-center gap-2 text-link"
            : "flex items-center gap-2 bg-danger-bg text-danger-fg"
        }
      >
        <span aria-hidden className={`h-[7px] w-[7px] ${connected ? "bg-link" : "bg-danger-fg"}`} />
        {connected ? "Connected" : busy ? "Connecting" : "Offline"}
      </Cell>
      <Cell>{window.location.origin.replace(/^https?:/, "grpc:")}</Cell>
      <Cell className="truncate">{scope}</Cell>
      <Outcome />
      <Cell className="ml-auto border-r-0 border-l border-line">⌘↵ Run</Cell>
    </footer>
  );
}

function Cell({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <span className={`shrink-0 border-r border-line px-3 leading-[18px] ${className}`}>{children}</span>
  );
}

function Outcome() {
  const run = useApp((state) => state.run);

  if (run.status === "running") {
    return (
      <Cell>
        {run.streamedRows > 0 ? `${run.streamedRows.toLocaleString()} rows…` : "Running…"}
      </Cell>
    );
  }
  if (run.status === "error") {
    return <Cell className="bg-danger-bg text-danger-fg">Statement failed</Cell>;
  }
  if (run.status !== "done" || !run.result) {
    return null;
  }
  return (
    <>
      <Cell>{run.result.rowCount.toLocaleString()} rows</Cell>
      <Cell>{Math.round(run.result.elapsedMs)} ms</Cell>
    </>
  );
}
