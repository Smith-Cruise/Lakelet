---
icon: lucide/rocket
---

# Getting Started

Lakelet's philosophy is to be simple, fast, and be easy to use.

## Install Lakelet

The quickest way is the installation script, which downloads the latest nightly
binary into the current directory as `./lakelet`, along with the example
configuration `config_demo.toml`:

```bash
curl -fsSL https://lakelet.dev/install.sh | sh
```

The script supports Linux and macOS (x86_64 and aarch64).

Note that the x86_64 binaries target x86-64-v3 and require a CPU with AVX2
(Intel Haswell 2013+ / AMD Excavator+), and the Linux binaries require
glibc 2.35 or newer.

Alternatively, download an archive directly from
[GitHub Releases](https://github.com/Smith-Cruise/Lakelet/releases) — this is
also the way to get the Windows build
(`lakelet-nightly-x86_64-pc-windows-msvc.zip`).

### Build from source

Lakelet requires a Rust toolchain. Build the release binary from the repository
root:

```bash
git clone https://github.com/Smith-Cruise/Lakelet.git
cd Lakelet
cargo build --release
cp target/release/lakelet .
```

## Create a Configuration File

Copy the example configuration and edit it for your environment:

```bash
cp config_demo.toml config.toml
```

A configuration file can contain the following top-level sections:

| Section | Required | Description |
| --- | --- | --- |
| `[server]` | No | Server-wide settings. |
| `[[catalog.hms]]` | No | A Hive Metastore catalog. Repeat the block to configure multiple HMS catalogs. |
| `[[catalog.glue]]` | No | An AWS Glue catalog. Repeat the block to configure multiple Glue catalogs. |
| `[[catalog.paimon-fs]]` | No | A Paimon filesystem catalog. Repeat the block to configure multiple warehouses. |

At least one external catalog is needed to
query external tables. The built-in `internal` catalog is always available,
even when no external catalog is configured.

The following is a minimal HMS configuration:

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
```

Add an `s3-storage` or `oss-storage` entry to the catalog when its tables use object storage:

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

See [Server](server.md), [Catalog](catalogs/index.md), and [Storage](storage.md) for
the complete configuration reference.

## Start Lakelet

Pass the configuration file with `--config`:

```bash
./lakelet --config config.toml
```

The configuration file is required for normal execution.

You can get more help by `./lakelet --help`.