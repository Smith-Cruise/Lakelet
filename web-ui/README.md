# Lakelet Web UI

A browser UI for [Lakelet](../README.md). In normal use nothing needs to be
run from this directory: `lakelet --ui` serves this UI (deployed at
https://ui.lakelet.dev) through a local reverse proxy on `web-ui-port`
(default 6060), on the same origin as the internal HTTP API the page talks
to.

```bash
lakelet --config config.toml --ui
```

Then open http://127.0.0.1:6060.

## Develop the UI

1. Start the Lakelet server as above (`--ui`).

2. Start the Vite dev server:

   ```bash
   npm install
   npm run dev
   ```

3. Open http://localhost:5173. The dev server forwards `/api/*` to
   `http://127.0.0.1:6060` (see the `server.proxy` entry in
   `vite.config.ts`; adjust it if you configured a different `web-ui-port`).
   The API sends no CORS headers, so going through the dev-server proxy is
   required — the page cannot call the API cross-origin.

To exercise the Rust reverse-proxy path against a local build instead:

```bash
npm run build && npx vite preview          # serves dist/ on :4173
LAKELET_UI_URL=http://localhost:4173 lakelet --config config.toml --ui
```

Note the Rust proxy forwards HTTP only (no WebSockets), so Vite HMR does not
work through it — use `npm run dev` for day-to-day UI work.

## Build for deployment

```bash
npm run build
```

The static site is generated in `dist/` and deployed to https://ui.lakelet.dev
via the Cloudflare Worker config in `wrangler.jsonc`. The app calls the API
with same-origin relative URLs, so it only works when served through
`lakelet --ui`'s proxy (or the Vite dev server); opened directly on the CDN it
shows instructions to run Lakelet instead.
