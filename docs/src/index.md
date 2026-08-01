---
hide:
  - navigation
  - toc
  - footer
---

<div class="ll-page" markdown>

<header class="ll-nav" markdown>
<span class="ll-brand">LAKELET <span class="ll-rev">rev. nightly</span></span>
<nav class="ll-nav-links" markdown>
[Docs](getting-started.md) · [GitHub](https://github.com/Smith-Cruise/Lakelet)
</nav>
</header>

<section class="ll-hero" markdown>

<p class="ll-fig">fig. 01 — query engine, single unit</p>

# Focused on data lake queries,<br>powered by DataFusion

One binary that speaks Iceberg, Delta Lake, Paimon, and Hive.
Point it at your catalog and start writing SQL — no Spark, no JVM, no cluster.

<div class="ll-install" markdown>
<p class="ll-install-head">install — 1 command, 30 seconds</p>

```sh
curl -fsSL https://lakelet.dev/install.sh | sh
```

</div>

<p class="ll-note" markdown>linux · macos&nbsp;&nbsp;//&nbsp;&nbsp;windows → [releases/nightly](https://github.com/Smith-Cruise/Lakelet/releases/tag/nightly)</p>

[Get started](getting-started.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/Smith-Cruise/Lakelet){ .md-button }

</section>

<section class="ll-specs" markdown>

<div class="ll-grid" markdown>

<div class="ll-cell" markdown>
<p class="ll-cell-head">a. formats</p>
Iceberg · Delta Lake<br>Paimon · Hive
</div>

<div class="ll-cell" markdown>
<p class="ll-cell-head">b. catalogs</p>
Hive Metastore<br>AWS Glue
</div>

<div class="ll-cell" markdown>
<p class="ll-cell-head">c. interfaces</p>
CLI · Flight SQL<br>Web UI
</div>

<div class="ll-cell" markdown>
<p class="ll-cell-head">d. storage</p>
S3 · Aliyun OSS<br>HDFS
</div>

<div class="ll-cell" markdown>
<p class="ll-cell-head">e. engine</p>
Apache DataFusion<br>vectorized execution
</div>

<div class="ll-cell" markdown>
<p class="ll-cell-head">f. deploy</p>
single binary<br>no JVM · no Spark
</div>

</div>

</section>

<section class="ll-demo" markdown>

<p class="ll-fig">fig. 02 — usage</p>

<div class="ll-demo-cols" markdown>

<div class="ll-demo-text" markdown>

### Bring a config, get a lakehouse shell

Describe your catalog in a few lines of TOML and run `lakelet`.
Query interactively, pipe SQL through `--command`, or serve the
engine over Arrow Flight SQL with a built-in web UI.

</div>

<div class="ll-demo-code" markdown>

```console
$ ./lakelet --config config.toml \
    --command "SELECT city, count(*) AS pv
               FROM hms.logs.events
               GROUP BY city ORDER BY pv DESC"
```

</div>

</div>

</section>

<section class="ll-cta" markdown>

<p class="ll-fig">fig. 03 — next steps</p>

[Get started](getting-started.md){ .md-button .md-button--primary }
[Download nightly](https://github.com/Smith-Cruise/Lakelet/releases/tag/nightly){ .md-button }

</section>

</div>
