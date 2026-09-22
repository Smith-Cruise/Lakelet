## Project description
Lakelet is a Datafusion based query engine. It focuses on data lake query.

### Supported table format
* Iceberg (parquet)
* Delta Lake (parquet)
* Paimon (parquet)
* Hive (textfile, parquet)

### Supported catalogs
* HMS
* Glue
* Paimon filesystem catalog

## Agents rules
Agents should follow below rules:

* The language of the proposed plan matches the language of the user's question.
* Code's comments, commit message, pull request must use English.
* If config items are modified, please note that the `@docs/` needs to be updated.
* Keep the PR descriptions as concise as possible.
* After change the code, don't commit it by yourself, unless user required it.
* When writing unit tests, first check if a similar test case already exists; if so, simply expand its scope to avoid unnecessary redundancy.
* When writing `/docs`, avoid making users aware of the technical implementation details.
* Before changing anything visual in `web/` or `docs/`, read [DESIGN.md](./DESIGN.md) and follow the design language it describes. Build from its tokens rather than introducing new colours, fonts or geometry. Do not depart from it — a different accent colour, dark mode, rounded corners or soft shadows — unless the user explicitly asks; if a change would erode it, say so instead of shipping it. When a token value, font or geometry rule does change, update `web/src/styles.css`, `docs/src/stylesheets/extra.css` and `DESIGN.md` together.

### Pull request checklist
Agents should finish below checklist before pull request.

Pass format check:
```bash
cargo fmt --all -- --check
```

Pass clippy check:
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Pass tests:
```bash
cargo test --all-targets --all-features --verbose
```

When `web/` is changed, pass the web UI typecheck too:
```bash
pnpm -C web typecheck
```
