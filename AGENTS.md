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
