import { CircleAlert, CircleCheck, Download, LoaderCircle, TableProperties } from "lucide-react";
import { TableView } from "./TableView";
import { downloadCsv } from "../lib/csv";
import { useApp } from "../store";
import { ROW_LIMIT } from "../flight/client";

export function ResultPanel() {
  const run = useApp((state) => state.run);
  const result = run.result;

  return (
    <section className="flex h-full min-h-0 flex-col">
      <div className="flex h-10 shrink-0 items-center gap-3 border-b border-line px-3.5">
        <h2 className="m-0 text-[13px] font-medium">Results</h2>
        <div className="flex-1" />
        <Status />
        {run.status === "done" && result && result.columns.length > 0 ? (
          <button
            type="button"
            onClick={() => downloadCsv(result)}
            className="flex h-7 items-center gap-1.5 rounded-md border border-line px-2.5 text-[12px] font-medium text-fg-muted hover:bg-hover hover:text-fg"
          >
            <Download size={13} />
            CSV
          </button>
        ) : null}
      </div>

      <div className="min-h-0 flex-1">
        {run.status === "error" ? (
          <div className="m-3 flex items-start gap-2.5 rounded-lg bg-danger-bg px-3.5 py-3 text-danger-fg">
            <CircleAlert size={16} className="mt-px shrink-0" />
            <pre className="m-0 font-mono text-[12px] leading-relaxed whitespace-pre-wrap">{run.error}</pre>
          </div>
        ) : result && result.columns.length > 0 ? (
          <TableView result={result} />
        ) : (
          <div className="flex h-full flex-col items-center justify-center gap-2 text-fg-faint">
            <TableProperties size={28} strokeWidth={1.5} />
            <p className="m-0 text-[13px] text-fg-muted">
              {run.status === "running" ? "Running…" : "Run a statement to see results"}
            </p>
            {run.status === "running" ? null : (
              <p className="m-0 text-[12px]">
                <kbd className="rounded border border-line bg-sub px-1.5 py-0.5 font-sans">⌘↵</kbd> runs the
                statement at the cursor
              </p>
            )}
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
      <span className="flex items-center gap-1.5 text-[12px] text-fg-muted">
        <LoaderCircle size={14} className="animate-spin text-accent" />
        {run.streamedRows > 0 ? `${run.streamedRows.toLocaleString()} rows…` : "running…"}
      </span>
    );
  }
  if (run.status !== "done" || !run.result) {
    return null;
  }

  const { rowCount, elapsedMs, truncated } = run.result;
  return (
    <span className="flex items-center gap-2.5 text-[12px] text-fg-muted">
      <span className="flex items-center gap-1.5">
        <CircleCheck size={14} className="text-success-fg" />
        {rowCount.toLocaleString()} rows · {Math.round(elapsedMs)} ms
      </span>
      {truncated ? (
        <span className="rounded-md bg-warning-bg px-2 py-0.5 text-warning-fg">
          truncated at {ROW_LIMIT.toLocaleString()}
        </span>
      ) : null}
    </span>
  );
}
