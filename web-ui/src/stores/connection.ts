import { create } from "zustand";
import { getInfo, setApiBase, type ServerInfo } from "@/lib/api";
import { baseUrl, loadPort, savePort } from "@/lib/connection";

/** Which screen to show. `probing` is only the initial silent probe. */
export type ConnectionStatus = "probing" | "setup" | "connected";

interface ConnectionState {
  status: ConnectionStatus;
  /** A connection attempt is in flight (initial probe or a reconnect). */
  probing: boolean;
  port: number;
  info: ServerInfo | null;
  error: string | null;
  /** Settings dialog visibility while already connected. */
  dialogOpen: boolean;
  connect: (port: number) => Promise<void>;
  openSettings: () => void;
  closeSettings: () => void;
}

export const useConnection = create<ConnectionState>((set, get) => ({
  status: "probing",
  probing: true,
  port: loadPort(),
  info: null,
  error: null,
  dialogOpen: false,

  connect: async (port: number) => {
    // `port` only moves on success, so a failed reconnect keeps the status
    // bar pointing at the server actually in use.
    set({ probing: true, error: null });
    try {
      const info = await getInfo(baseUrl(port), AbortSignal.timeout(3000));
      setApiBase(baseUrl(port));
      savePort(port);
      set({ status: "connected", port, info, error: null, probing: false, dialogOpen: false });
    } catch (err) {
      let message: string;
      if (err instanceof DOMException && (err.name === "TimeoutError" || err.name === "AbortError")) {
        message = `Connection to 127.0.0.1:${port} timed out`;
      } else if (err instanceof TypeError) {
        // fetch network failures surface as bare TypeErrors ("Failed to fetch").
        message = `Could not reach 127.0.0.1:${port} — is the Lakelet server running?`;
      } else {
        message = err instanceof Error ? err.message : String(err);
      }
      // A failed reconnect keeps the working session (and the editor with it);
      // the error shows inside the still-open settings dialog.
      const connected = get().status === "connected";
      set({
        status: connected ? "connected" : "setup",
        error: message,
        probing: false,
        info: connected ? get().info : null,
      });
    }
  },

  openSettings: () => set({ dialogOpen: true }),
  closeSettings: () => set({ dialogOpen: false, error: null }),
}));
