import { Droplets, Loader2, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useConnection } from "@/stores/connection";

/**
 * Full-page state shown when the same-origin probe fails: the page was opened
 * without a Lakelet server behind it (server stopped, or the hosted site was
 * visited directly instead of through `lakelet --ui`).
 */
export function NotConnected() {
  const status = useConnection((state) => state.status);
  const probing = useConnection((state) => state.probing);
  const error = useConnection((state) => state.error);
  const connect = useConnection((state) => state.connect);

  // Initial silent probe: don't flash the instructions unless it fails.
  if (status === "probing") {
    return (
      <div className="flex flex-1 items-center justify-center">
        <Loader2 className="size-5 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="flex flex-1 items-center justify-center p-6">
      <div className="w-full max-w-md rounded-xl border bg-card p-6 shadow-sm">
        <div className="mb-4 flex items-center gap-2.5">
          <Droplets className="size-6 text-sky-500" />
          <div>
            <h1 className="text-base font-semibold">Lakelet server not reachable</h1>
            <p className="text-xs text-muted-foreground">
              This page talks to the Lakelet server that serves it.
            </p>
          </div>
        </div>
        <p className="mb-3 text-sm text-muted-foreground">
          Start Lakelet with <code className="font-mono">--ui</code> and open the Web UI address
          it prints (default http://127.0.0.1:6060):
        </p>
        <pre className="mb-4 overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs leading-relaxed">
          {"lakelet --config config.toml --ui"}
        </pre>
        {error && <p className="mb-4 text-xs text-destructive">{error}</p>}
        <Button size="sm" disabled={probing} onClick={() => void connect()}>
          <RefreshCw className="size-3.5" /> Retry
        </Button>
      </div>
    </div>
  );
}
