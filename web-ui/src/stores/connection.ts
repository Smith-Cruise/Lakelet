import { create } from "zustand";
import { getInfo, type ServerInfo } from "@/lib/api";

/** Which screen to show. `probing` is only the initial silent probe. */
export type ConnectionStatus = "probing" | "disconnected" | "connected";

interface ConnectionState {
  status: ConnectionStatus;
  /** A connection attempt is in flight (initial probe or a retry). */
  probing: boolean;
  info: ServerInfo | null;
  error: string | null;
  connect: () => Promise<void>;
}

// The API is same-origin: this page is served by `lakelet --ui` next to the
// API routes. There is no server address to configure — if the probe fails,
// the page was opened without a Lakelet server behind it (e.g. directly on
// the CDN) and all we can do is explain and retry.
export const useConnection = create<ConnectionState>((set) => ({
  status: "probing",
  probing: true,
  info: null,
  error: null,

  connect: async () => {
    set({ probing: true, error: null });
    try {
      const info = await getInfo(AbortSignal.timeout(3000));
      set({ status: "connected", info, error: null, probing: false });
    } catch (err) {
      let message: string;
      if (err instanceof DOMException && (err.name === "TimeoutError" || err.name === "AbortError")) {
        message = "Connection to the Lakelet server timed out";
      } else if (err instanceof TypeError) {
        // fetch network failures surface as bare TypeErrors ("Failed to fetch").
        message = "Could not reach the Lakelet server behind this page";
      } else {
        message = err instanceof Error ? err.message : String(err);
      }
      set({ status: "disconnected", error: message, probing: false, info: null });
    }
  },
}));
