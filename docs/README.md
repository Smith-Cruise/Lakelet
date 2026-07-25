# Lakelet Documentation

The documentation site is built with [MkDocs](https://www.mkdocs.org/) and the
[Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) theme.

- `mkdocs.yml` — site configuration
- `src/` — markdown sources and assets

## Local development

Requires Python 3. From this directory (`docs/`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdocs serve
```

Then open <http://127.0.0.1:8000>. The site reloads automatically when you
edit files under `src/` or `mkdocs.yml`.

To build the static site without serving it:

```bash
mkdocs build
```

The output goes to `site/` (gitignored).

## Deployment

The site is deployed automatically by Cloudflare Pages: every push to `main`
that touches `docs/` triggers a build (`mkdocs build` with root directory
`docs`) and publishes the result to <https://lakelet.dev/>. No manual steps
are needed.
