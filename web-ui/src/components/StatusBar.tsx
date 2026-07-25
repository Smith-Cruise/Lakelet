import { useEffect, useState } from "react";
import { useQuery } from "@/stores/query";

function formatElapsed(ms: number): string {
  return `${(ms / 1000).toFixed(1)}s`;
}

export function StatusBar() {
  const running = useQuery((state) => state.running);
  const startedAt = useQuery((state) => state.startedAt);
  const finishedAt = useQuery((state) => state.finishedAt);
  const rowCount = useQuery((state) => state.rowCount);
  const batchCount = useQuery((state) => state.batches.length);
  const error = useQuery((state) => state.error);
  const cancelled = useQuery((state) => state.cancelled);

  // Ticks while a query is running so the elapsed time is live.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!running) {
      return;
    }
    // Resync immediately: `now` is frozen at the last query's end, which
    // would render a negative elapsed time until the first tick.
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), 100);
    return () => clearInterval(timer);
  }, [running]);

  let statusText = "Ready";
  if (running && startedAt) {
    statusText = `Running… ${formatElapsed(now - startedAt)}`;
  } else if (error) {
    statusText = "Error";
  } else if (cancelled) {
    statusText = "Cancelled";
  } else if (startedAt && finishedAt) {
    statusText = `Done in ${formatElapsed(finishedAt - startedAt)}`;
  }

  return (
    <footer className="flex h-7 shrink-0 items-center gap-4 border-t px-3 font-mono text-[11px] text-muted-foreground">
      <span className={error ? "text-destructive" : undefined}>{statusText}</span>
      <div className="flex-1" />
      {startedAt !== null && (
        <>
          <span>{rowCount.toLocaleString()} rows</span>
          <span>{batchCount} batches</span>
        </>
      )}
    </footer>
  );
}
