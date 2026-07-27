import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": new URL("./src", import.meta.url).pathname,
    },
  },
  server: {
    proxy: {
      // The app calls the API with same-origin relative URLs and the API
      // sends no CORS headers, so the dev server must forward /api itself.
      // Match `web-ui-port` in your Lakelet config (default 6060).
      "/api": "http://127.0.0.1:6060",
    },
  },
});
