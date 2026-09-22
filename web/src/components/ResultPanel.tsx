import { CircleAlert, Download, LoaderCircle } from "lucide-react";
import { TableView } from "./TableView";
import { downloadCsv } from "../lib/csv";
import { useApp } from "../store";
import { ROW_LIMIT } from "../flight/client";

export function ResultPanel() {
  const run = useApp((state) => state.run);
  const result = run.result;

  return (
    <section className="flex h-full min-h-0 flex-col">
      <div className="flex h-[30px] shrink-0 items-center gap-3 border-y border-line-strong bg-page px-3">
        <h2 className="m-0 font-mono text-[10px] font-bold tracking-[.16em] text-fg-faint uppercase">
          Results
        </h2>
        <div className="flex-1" />
        <Status />
        {run.status === "done" && result && result.columns.length > 0 ? (
          <button
            type="button"
            onClick={() => downloadCsv(result)}
            className="flex h-6 items-center gap-1.5 border border-line px-2 font-mono text-[10px] font-bold tracking-[.12em] text-fg-muted hover:border-line-strong hover:text-fg"
          >
            <Download size={11} />
            CSV
          </button>
        ) : null}
      </div>

      <div className="min-h-0 flex-1">
        {run.status === "error" ? (
          <div className="m-3 flex items-start gap-2.5 border border-danger-fg bg-danger-bg px-3.5 py-3 text-danger-fg">
            <CircleAlert size={16} className="mt-px shrink-0" />
            <pre className="m-0 font-mono text-[12px] leading-relaxed whitespace-pre-wrap">{run.error}</pre>
          </div>
        ) : result && result.columns.length > 0 ? (
          <TableView result={result} />
        ) : (
          <div className="flex h-full items-center justify-center bg-panel-alt">
            <div className="border border-line px-5 py-4 text-center">
              <p className="m-0 font-mono text-[11px] font-bold tracking-[.16em] text-fg-muted uppercase">
                {run.status === "running" ? "Running…" : "No result yet"}
              </p>
              {run.status === "running" ? null : (
                <p className="m-0 mt-2 font-mono text-[11px] text-fg-faint">
                  <kbd className="border border-line bg-sub px-1.5 py-0.5 font-mono">⌘↵</kbd> runs the
                  statement at the cursor
                </p>
              )}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}

function Status() {
  const run = useApp((state) => state.run);

  if (run.status === "running") {
    return (
      <span className="flex items-center gap-1.5 font-mono text-[11px] text-fg-muted">
        <LoaderCircle size={12} className="animate-spin text-fg-muted" />
        {run.streamedRows > 0 ? `${run.streamedRows.toLocaleString()} rows…` : "running…"}
      </span>
    );
  }
  if (run.status !== "done" || !run.result) {
    return null;
  }

  const { rowCount, elapsedMs, truncated } = run.result;
  return (
    <span className="flex items-center gap-2.5 font-mono text-[11px] text-fg-muted">
      <span className="flex items-center gap-2">
        <span aria-hidden className="h-2 w-2 bg-accent" />
        {rowCount.toLocaleString()} rows · {Math.round(elapsedMs)} ms
      </span>
      {truncated ? (
        <span className="bg-warning-bg px-2 py-0.5 text-warning-fg">
          truncated at {ROW_LIMIT.toLocaleString()}
        </span>
      ) : null}
    </span>
  );
}
