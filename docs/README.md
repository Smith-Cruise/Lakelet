# Lakelet Documentation

The documentation site is built with [Zensical](https://zensical.org/), the
successor to MkDocs and Material for MkDocs by the same authors.

- `zensical.toml` — site configuration
- `src/` — markdown sources and assets

## Local development

Requires Python 3.10 or newer. From this directory (`docs/`):

```bash
pip install -r requirements.txt
zensical serve
```

Then open <http://127.0.0.1:8000>. The site reloads automatically when you
edit files under `src/` or `zensical.toml`.

To build the static site without serving it:

```bash
zensical build
```

The output goes to `site/` (gitignored).

## Deployment

The site is deployed automatically by Cloudflare Pages: every push to `main`
that touches `docs/` triggers a build (`zensical build` with root directory
`docs`) and publishes the result to <https://lakelet.dev/>. No manual steps
are needed.
