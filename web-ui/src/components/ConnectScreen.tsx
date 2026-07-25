import { Droplets, Loader2 } from "lucide-react";
import { ConnectForm } from "@/components/ConnectForm";
import { useConnection } from "@/stores/connection";

/** Full-page state shown until a Lakelet server connection is established. */
export function ConnectScreen() {
  const status = useConnection((state) => state.status);
  const error = useConnection((state) => state.error);

  // Initial silent probe of the saved/default port: don't flash the form
  // unless the probe actually fails.
  if (status === "probing" && error === null) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <Loader2 className="size-5 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="flex flex-1 items-center justify-center p-6">
      <div className="w-full max-w-md rounded-xl border bg-card p-6 shadow-sm">
        <div className="mb-5 flex items-center gap-2.5">
          <Droplets className="size-6 text-sky-500" />
          <div>
            <h1 className="text-base font-semibold">Connect to Lakelet</h1>
            <p className="text-xs text-muted-foreground">
              A local lakehouse SQL engine — this UI connects to it over localhost.
            </p>
          </div>
        </div>
        <ConnectForm />
      </div>
    </div>
  );
}
