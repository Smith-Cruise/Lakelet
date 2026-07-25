import { useQuery } from "@/stores/query";
import { cn } from "@/lib/utils";

interface HistoryListProps {
  onPick: (sql: string) => void;
}

const STATUS_DOT: Record<string, string> = {
  ok: "bg-emerald-500",
  error: "bg-destructive",
  cancelled: "bg-amber-500",
};

export function HistoryList({ onPick }: HistoryListProps) {
  const history = useQuery((state) => state.history);

  if (history.length === 0) {
    return <div className="px-3 py-2 text-[13px] text-muted-foreground">No queries yet</div>;
  }

  return (
    <div className="min-h-0 flex-1 overflow-auto px-1.5 py-1.5">
      {history.map((entry, index) => (
        <button
          key={`${entry.at}-${index}`}
          className="w-full rounded-md px-2 py-1.5 text-left hover:bg-accent"
          onClick={() => onPick(entry.sql)}
          title={entry.sql}
        >
          <div className="truncate font-mono text-xs">{entry.sql}</div>
          <div className="mt-0.5 flex items-center gap-1.5 text-[11px] text-muted-foreground">
            <span className={cn("size-1.5 rounded-full", STATUS_DOT[entry.status])} />
            <span>{new Date(entry.at).toLocaleTimeString()}</span>
            <span>·</span>
            <span>{(entry.durationMs / 1000).toFixed(2)}s</span>
            {entry.status === "ok" && (
              <>
                <span>·</span>
                <span>{entry.rows} rows</span>
              </>
            )}
          </div>
        </button>
      ))}
    </div>
  );
}
