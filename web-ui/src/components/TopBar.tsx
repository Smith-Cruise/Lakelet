import { Droplets, Moon, Sun } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import { ConnectForm } from "@/components/ConnectForm";
import { useConnection } from "@/stores/connection";
import { useTheme } from "@/stores/theme";
import { cn } from "@/lib/utils";

export function TopBar() {
  const status = useConnection((state) => state.status);
  const port = useConnection((state) => state.port);
  const dialogOpen = useConnection((state) => state.dialogOpen);
  const openSettings = useConnection((state) => state.openSettings);
  const closeSettings = useConnection((state) => state.closeSettings);
  const dark = useTheme((state) => state.dark);
  const toggleTheme = useTheme((state) => state.toggle);

  const connected = status === "connected";

  return (
    <header className="flex h-12 shrink-0 items-center gap-3 border-b px-4">
      <div className="flex items-center gap-2">
        <Droplets className="size-5 text-sky-500" />
        <span className="text-sm font-semibold tracking-tight">Lakelet</span>
      </div>
      <div className="flex-1" />
      {connected && (
        <button
          className="flex items-center gap-1.5 rounded-md px-2 py-1 font-mono text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          onClick={openSettings}
          title="Connection settings"
        >
          <span className={cn("size-2 rounded-full", "bg-emerald-500")} />
          127.0.0.1:{port}
        </button>
      )}
      <Button variant="ghost" size="icon" onClick={toggleTheme} title="Toggle theme">
        {dark ? <Sun /> : <Moon />}
      </Button>

      <Dialog open={dialogOpen} onOpenChange={(open) => (open ? openSettings() : closeSettings())}>
        <DialogContent>
          <DialogTitle>Connection</DialogTitle>
          <DialogDescription>
            Point this page at the Lakelet server running on your machine.
          </DialogDescription>
          <ConnectForm />
        </DialogContent>
      </Dialog>
    </header>
  );
}
