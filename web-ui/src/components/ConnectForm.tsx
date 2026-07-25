import { useState, type FormEvent } from "react";
import { Loader2, Plug } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useConnection } from "@/stores/connection";

/** Port form shared by the first-run screen and the settings dialog. */
export function ConnectForm() {
  const probing = useConnection((state) => state.probing);
  const port = useConnection((state) => state.port);
  const error = useConnection((state) => state.error);
  const connect = useConnection((state) => state.connect);
  const [value, setValue] = useState(String(port));

  const parsed = Number(value);
  const valid = Number.isInteger(parsed) && parsed >= 1 && parsed <= 65535;

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (valid && !probing) {
      void connect(parsed);
    }
  };

  return (
    <form onSubmit={submit} className="flex flex-col gap-4">
      <div className="flex items-center gap-2">
        <div className="flex h-9 items-center rounded-md border border-input bg-muted px-3 font-mono text-sm text-muted-foreground">
          127.0.0.1
        </div>
        <span className="font-mono text-sm text-muted-foreground">:</span>
        <Input
          type="number"
          min={1}
          max={65535}
          value={value}
          onChange={(event) => setValue(event.target.value)}
          className="w-28 font-mono"
          autoFocus
        />
        <Button type="submit" disabled={!valid || probing}>
          {probing ? <Loader2 className="animate-spin" /> : <Plug />}
          Connect
        </Button>
      </div>
      {error && <p className="text-sm break-words text-destructive">{error}</p>}
      <div className="space-y-1.5 text-xs text-muted-foreground">
        <p>
          Start the server on this machine, then connect. Set{" "}
          <code className="rounded bg-muted px-1 py-0.5 font-mono">web-ui-port</code> under{" "}
          <code className="rounded bg-muted px-1 py-0.5 font-mono">[server]</code> in the config
          (e.g. 6060):
        </p>
        <pre className="rounded-md bg-muted px-2.5 py-2 font-mono">
          lakelet --config config.toml --web-ui
        </pre>
        <p>
          Queries run entirely on your machine; this page only talks to 127.0.0.1. Use Chrome,
          Edge or Firefox — Safari blocks HTTPS pages from reaching local servers.
        </p>
      </div>
    </form>
  );
}
