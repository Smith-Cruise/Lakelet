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
    <footer className="flex h-6 shrink-0 items-center border-t-2 border-line-strong bg-fg font-mono text-[10.5px] tracking-[.1em] text-page/70">
      {/* On ink, a saturated red reads at about 2.5:1 as text, so a failure
          state is a filled chip rather than coloured lettering. */}
      <Cell
        className={
          connected
            ? "flex items-center gap-2 text-accent"
            : "flex items-center gap-2 bg-danger-fg text-page"
        }
      >
        <span aria-hidden className={`h-[7px] w-[7px] ${connected ? "bg-accent" : "bg-page"}`} />
        {connected ? "CONNECTED" : busy ? "CONNECTING" : "OFFLINE"}
      </Cell>
      <Cell>{window.location.origin.replace(/^https?:/, "grpc:")}</Cell>
      <Cell className="truncate">{scope}</Cell>
      <Outcome />
      <Cell className="ml-auto border-r-0 border-l border-l-page/15">⌘↵ RUN</Cell>
    </footer>
  );
}

function Cell({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <span className={`border-r border-page/15 px-3 leading-[22px] ${className}`}>{children}</span>
  );
}

function Outcome() {
  const run = useApp((state) => state.run);

  if (run.status === "running") {
    return (
      <Cell>
        {run.streamedRows > 0 ? `${run.streamedRows.toLocaleString()} ROWS…` : "RUNNING…"}
      </Cell>
    );
  }
  if (run.status === "error") {
    return <Cell className="bg-danger-fg text-page">STATEMENT FAILED</Cell>;
  }
  if (run.status !== "done" || !run.result) {
    return null;
  }
  return (
    <>
      <Cell>{run.result.rowCount.toLocaleString()} ROWS</Cell>
      <Cell>{Math.round(run.result.elapsedMs)} MS</Cell>
    </>
  );
}
