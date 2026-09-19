import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const FLIGHT_PREFIX = "/arrow.flight.protocol.FlightService";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    // Served by the Rust binary from `src/app/src/server/web`, which embeds
    // this directory.
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    // In production the page and the Flight service share an origin. The dev
    // server has to forward the gRPC-Web calls so the client can keep using
    // window.location.origin either way.
    proxy: {
      [FLIGHT_PREFIX]: {
        target: "http://localhost:32010",
        changeOrigin: false,
      },
    },
  },
});
