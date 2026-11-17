---
icon: lucide/house
hide:
  - navigation
  - toc
---

<div class="ll-home" markdown>

<div class="ll-band ll-hero" markdown>

<div class="ll-hero__copy" markdown>

# A lightweight lakehouse query engine.

<p class="ll-hero__lead">Explore Iceberg, Delta Lake, Paimon and Hive with SQL. From your terminal or your browser.</p>

[<span class="ll-button-face">Getting started</span>](getting-started.md){ .md-button .md-button--primary }
[<span class="ll-button-face">View on GitHub</span>](https://github.com/Smith-Cruise/Lakelet){ .md-button }

<div class="ll-install" markdown>

```sh
curl -fsSL https://lakelet.dev/install.sh | sh
```

</div>

<p class="ll-note">The shell installer supports Linux and macOS. For Windows, grab a build from
<a href="https://github.com/Smith-Cruise/Lakelet/releases/tag/nightly">releases/nightly</a>.</p>

</div>

<img class="ll-hero__art" src="assets/lake-query.svg" width="560" height="460" alt="" aria-hidden="true">

</div>

<div class="ll-overview" markdown>

<div class="ll-section" markdown>

## Two ways to query

<div class="ll-modes" markdown>

<div class="ll-mode" markdown>

<div class="ll-mode__id" markdown>
  <div class="ll-mode__icon" aria-hidden="true">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="square" stroke-linejoin="miter">
      <rect width="20" height="7" x="2" y="3"></rect>
      <rect width="20" height="7" x="2" y="14"></rect>
      <line x1="6" x2="6.01" y1="6.5" y2="6.5"></line>
      <line x1="6" x2="6.01" y1="17.5" y2="17.5"></line>
    </svg>
  </div>

### Server

</div>

Explore catalogs and run SQL in the built-in web console.

[See the web console →](#query-from-the-browser){ .ll-mode__go }

</div>

<div class="ll-mode" markdown>

<div class="ll-mode__id" markdown>
  <div class="ll-mode__icon" aria-hidden="true">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="square" stroke-linejoin="miter">
      <polyline points="4 17 10 11 4 5"></polyline>
      <line x1="12" x2="20" y1="19" y2="19"></line>
    </svg>
  </div>

### CLI

</div>

Run queries, explore tables and execute scripts from your terminal.

[See terminal →](#query-from-the-cli){ .ll-mode__go }

</div>

</div>

</div>

<div class="ll-section" markdown>

## One engine across your lake

<p class="ll-sub">Query open table formats through the catalogs you already use.</p>

<div class="ll-matrix" markdown>

<table class="ll-matrix-table">
<thead>
<tr><th>Catalog</th><th>Iceberg</th><th>Delta Lake</th><th>Paimon</th><th>Hive</th></tr>
</thead>
<tbody>
<tr>
  <th><img src="assets/logos/hive.svg" alt=""> Hive Metastore</th>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
  <td><span class="ll-beta" role="img" aria-label="Experimental" title="Experimental">β</span></td>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
</tr>
<tr>
  <th><img src="assets/logos/aws-glue.png" alt=""> AWS Glue</th>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
  <td><span class="ll-beta" role="img" aria-label="Experimental" title="Experimental">β</span></td>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
</tr>
<tr>
  <th><img src="assets/logos/iceberg-icon.png" alt=""> Iceberg REST</th>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
  <td><span class="ll-no" role="img" aria-label="Not served by this catalog">—</span></td>
  <td><span class="ll-no" role="img" aria-label="Not served by this catalog">—</span></td>
  <td><span class="ll-no" role="img" aria-label="Not served by this catalog">—</span></td>
</tr>
<tr>
  <th><img src="assets/logos/paimon-icon.svg" alt=""> Paimon FileSystem</th>
  <td><span class="ll-no" role="img" aria-label="Not served by this catalog">—</span></td>
  <td><span class="ll-no" role="img" aria-label="Not served by this catalog">—</span></td>
  <td><span class="ll-yes" role="img" aria-label="Supported" title="Supported"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span></td>
  <td><span class="ll-no" role="img" aria-label="Not served by this catalog">—</span></td>
</tr>
</tbody>
</table>

</div>

<p class="ll-legend">
<span><span class="ll-yes" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter"><polyline points="20 6 9 17 4 12"></polyline></svg></span> Supported</span>
<span><span class="ll-beta" aria-hidden="true">β</span> Experimental</span>
<span><span class="ll-no" aria-hidden="true">—</span> Not served by this catalog</span>
</p>

</div>

</div>

<div class="ll-section ll-console-section" markdown>

<div class="ll-showcase__header" markdown>

<div markdown>

## Query from the browser

<p class="ll-sub">Start Lakelet with <code>--server</code>, query in the built-in web console.</p>

</div>

[Server guide →](server.md){ .ll-go }

</div>

<div class="ll-shot">
<img src="assets/console.png" width="2880" height="1800" decoding="async" alt="The Lakelet web console: catalog explorer, SQL editor and a result grid">
</div>

</div>

<div class="ll-section" markdown>

<div class="ll-showcase__header" markdown>

<div markdown>

## Query from the CLI

<p class="ll-sub">Start in a SQL shell.</p>

</div>

[CLI quick start →](getting-started.md#start-lakelet-cli){ .ll-go }

</div>

<div class="ll-term">
<div class="ll-term__bar">
<span class="ll-term__dots" aria-hidden="true"></span>
<div class="ll-switch" role="tablist" aria-label="CLI examples">
<button class="ll-switch__tab" type="button" role="tab" id="cli-tab-explore" aria-controls="cli-panel-explore" aria-selected="true">Explore</button>
<button class="ll-switch__tab" type="button" role="tab" id="cli-tab-query" aria-controls="cli-panel-query" aria-selected="false" tabindex="-1">Query</button>
<button class="ll-switch__tab" type="button" role="tab" id="cli-tab-script" aria-controls="cli-panel-script" aria-selected="false" tabindex="-1">Script</button>
</div>
</div>
<div class="ll-term__body">
<pre class="ll-term__panel" id="cli-panel-explore" role="tabpanel" aria-labelledby="cli-tab-explore" tabindex="0"><span class="ll-t-dim">$</span> <span class="ll-t-cmd">./lakelet --config config.toml</span>
<span class="ll-t-dim">Enter SQL ending with &#x27;;&#x27;. Type &#x27;quit;&#x27; to disconnect.</span>
<span class="ll-t-gap"></span>
<span class="ll-t-p">sql&gt; </span><span class="ll-t-kw">show catalogs</span>;
<span class="ll-t-box">╭──────────────┬──────────────╮</span>
<span class="ll-t-box">│</span><span class="ll-t-h"> catalog_name </span><span class="ll-t-box">│</span><span class="ll-t-h"> catalog_type </span><span class="ll-t-box">│</span>
<span class="ll-t-box">╞══════════════╪══════════════╡</span>
<span class="ll-t-box">│</span> glue_catalog <span class="ll-t-box">│</span> GLUE         <span class="ll-t-box">│</span>
<span class="ll-t-box">├──────────────┼──────────────┤</span>
<span class="ll-t-box">│</span> iceberg_rest <span class="ll-t-box">│</span> ICEBERG-REST <span class="ll-t-box">│</span>
<span class="ll-t-box">├──────────────┼──────────────┤</span>
<span class="ll-t-box">│</span> internal     <span class="ll-t-box">│</span> INTERNAL     <span class="ll-t-box">│</span>
<span class="ll-t-box">╰──────────────┴──────────────╯</span>
<span class="ll-t-dim">3 row(s) fetched. </span>
<span class="ll-t-dim">Elapsed 0.004 seconds.</span>
<span class="ll-t-gap"></span>
<span class="ll-t-p">sql&gt; </span><span class="ll-t-kw">use</span> glue_catalog.tpch_sf300;
<span class="ll-t-dim">0 row(s) fetched. </span>
<span class="ll-t-dim">Elapsed 0.001 seconds.</span>
<span class="ll-t-gap"></span>
<span class="ll-t-p">sql&gt; </span><span class="ll-t-kw">show tables</span>;
<span class="ll-t-box">╭────────────╮</span>
<span class="ll-t-box">│</span><span class="ll-t-h"> table_name </span><span class="ll-t-box">│</span>
<span class="ll-t-box">╞════════════╡</span>
<span class="ll-t-box">│</span> customer   <span class="ll-t-box">│</span>
<span class="ll-t-box">├────────────┤</span>
<span class="ll-t-box">│</span> lineitem   <span class="ll-t-box">│</span>
<span class="ll-t-box">├────────────┤</span>
<span class="ll-t-box">│</span> orders     <span class="ll-t-box">│</span>
<span class="ll-t-box">╰────────────╯</span>
<span class="ll-t-dim">3 row(s) fetched. </span>
<span class="ll-t-dim">Elapsed 0.088 seconds.</span></pre>
<pre class="ll-term__panel" id="cli-panel-query" role="tabpanel" aria-labelledby="cli-tab-query" tabindex="0" hidden><span class="ll-t-p">sql&gt; </span><span class="ll-t-kw">select</span>
<span class="ll-t-p">  -&gt; </span>  o_orderpriority,
<span class="ll-t-p">  -&gt; </span>  <span class="ll-t-kw">count</span>(*) <span class="ll-t-kw">as</span> orders,
<span class="ll-t-p">  -&gt; </span>  <span class="ll-t-kw">round</span>(<span class="ll-t-kw">avg</span>(o_totalprice), 2) <span class="ll-t-kw">as</span> avg_price
<span class="ll-t-p">  -&gt; </span><span class="ll-t-kw">from</span> orders
<span class="ll-t-p">  -&gt; </span><span class="ll-t-kw">where</span> o_orderdate &gt;= <span class="ll-t-kw">date</span> &#x27;1998-01-01&#x27;
<span class="ll-t-p">  -&gt; </span><span class="ll-t-kw">group by</span> o_orderpriority
<span class="ll-t-p">  -&gt; </span><span class="ll-t-kw">order by</span> o_orderpriority;
<span class="ll-t-box">╭─────────────────┬─────────┬───────────╮</span>
<span class="ll-t-box">│</span><span class="ll-t-h"> o_orderpriority </span><span class="ll-t-box">│</span><span class="ll-t-h"> orders  </span><span class="ll-t-box">│</span><span class="ll-t-h"> avg_price </span><span class="ll-t-box">│</span>
<span class="ll-t-box">╞═════════════════╪═════════╪═══════════╡</span>
<span class="ll-t-box">│</span> 1-URGENT        <span class="ll-t-box">│</span> 5763412 <span class="ll-t-box">│</span> 151243.77 <span class="ll-t-box">│</span>
<span class="ll-t-box">├─────────────────┼─────────┼───────────┤</span>
<span class="ll-t-box">│</span> 2-HIGH          <span class="ll-t-box">│</span> 5761905 <span class="ll-t-box">│</span> 151198.02 <span class="ll-t-box">│</span>
<span class="ll-t-box">├─────────────────┼─────────┼───────────┤</span>
<span class="ll-t-box">│</span> 3-MEDIUM        <span class="ll-t-box">│</span> 5764880 <span class="ll-t-box">│</span> 151322.41 <span class="ll-t-box">│</span>
<span class="ll-t-box">├─────────────────┼─────────┼───────────┤</span>
<span class="ll-t-box">│</span> 4-NOT SPECIFIED <span class="ll-t-box">│</span> 5762233 <span class="ll-t-box">│</span> 151176.95 <span class="ll-t-box">│</span>
<span class="ll-t-box">├─────────────────┼─────────┼───────────┤</span>
<span class="ll-t-box">│</span> 5-LOW           <span class="ll-t-box">│</span> 5763004 <span class="ll-t-box">│</span> 151259.63 <span class="ll-t-box">│</span>
<span class="ll-t-box">╰─────────────────┴─────────┴───────────╯</span>
<span class="ll-t-dim">5 row(s) fetched. </span>
<span class="ll-t-dim">Elapsed 2.184 seconds.</span>
<span class="ll-t-gap"></span>
<span class="ll-t-p">sql&gt; </span><span class="ll-t-kw">select</span> * <span class="ll-t-kw">from</span> lineitm <span class="ll-t-kw">limit</span> 5;
<span class="ll-t-err">Error during planning: table &#x27;glue_catalog.tpch_sf300.lineitm&#x27; not found</span>
<span class="ll-t-gap"></span>
<span class="ll-t-p">sql&gt; </span><span class="ll-t-kw">quit</span>;
<span class="ll-t-dim">Goodbye!</span></pre>
<pre class="ll-term__panel" id="cli-panel-script" role="tabpanel" aria-labelledby="cli-tab-script" tabindex="0" hidden><span class="ll-t-dim">$</span> <span class="ll-t-cmd">./lakelet --version</span>
Lakelet: compiled with a0f747f 2026-09-21 13:47:20 UTC
<span class="ll-t-gap"></span>
<span class="ll-t-dim">$</span> <span class="ll-t-cmd">./lakelet --config config.toml --command &quot;show catalogs;&quot;</span>
<span class="ll-t-box">+--------------+--------------+</span>
<span class="ll-t-box">|</span><span class="ll-t-h"> catalog_name </span><span class="ll-t-box">|</span><span class="ll-t-h"> catalog_type </span><span class="ll-t-box">|</span>
<span class="ll-t-box">+--------------+--------------+</span>
<span class="ll-t-box">|</span> glue_catalog <span class="ll-t-box">|</span> GLUE         <span class="ll-t-box">|</span>
<span class="ll-t-box">|</span> iceberg_rest <span class="ll-t-box">|</span> ICEBERG-REST <span class="ll-t-box">|</span>
<span class="ll-t-box">|</span> internal     <span class="ll-t-box">|</span> INTERNAL     <span class="ll-t-box">|</span>
<span class="ll-t-box">+--------------+--------------+</span>
<span class="ll-t-dim">3 row(s) fetched. </span>
<span class="ll-t-dim">Elapsed 0.004 seconds.</span>
<span class="ll-t-gap"></span>
<span class="ll-t-dim">$</span> <span class="ll-t-cmd">cat daily.sql</span>
<span class="ll-t-kw">use</span> glue_catalog.tpch_sf300;
<span class="ll-t-kw">select</span> <span class="ll-t-kw">count</span>(*) <span class="ll-t-kw">as</span> n_orders <span class="ll-t-kw">from</span> orders;
<span class="ll-t-gap"></span>
<span class="ll-t-dim">$</span> <span class="ll-t-cmd">./lakelet --config config.toml --file daily.sql</span>
<span class="ll-t-dim">0 row(s) fetched. </span>
<span class="ll-t-dim">Elapsed 0.001 seconds.</span>
<span class="ll-t-gap"></span>
<span class="ll-t-box">+-----------+</span>
<span class="ll-t-box">|</span><span class="ll-t-h"> n_orders  </span><span class="ll-t-box">|</span>
<span class="ll-t-box">+-----------+</span>
<span class="ll-t-box">|</span> 450000000 <span class="ll-t-box">|</span>
<span class="ll-t-box">+-----------+</span>
<span class="ll-t-dim">1 row(s) fetched. </span>
<span class="ll-t-dim">Elapsed 1.204 seconds.</span></pre>
</div>
</div>

</div>

<div class="ll-band ll-close" markdown>

## Start querying

`curl -fsSL https://lakelet.dev/install.sh | sh`

</div>

</div>
