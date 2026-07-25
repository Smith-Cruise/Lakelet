# Lakelet Web UI

A browser UI for [Lakelet](../README.md). The page connects to a Lakelet
server running on your own machine.

## Run locally

1. Add a port to your Lakelet config and start the server:

   ```toml
   [server]
   web-ui-port = 6060
   ```

   ```bash
   lakelet --config config.toml --web-ui
   ```

2. Start the web UI:

   ```bash
   npm install
   npm run dev
   ```

3. Open http://localhost:5173. The page connects to `127.0.0.1:6060`
   automatically; if you used a different `web-ui-port`, enter that port when
   asked.

## Build for deployment

```bash
npm run build
```

The static site is generated in `dist/`. Upload that folder to any static
host or CDN — no server-side setup is needed.
