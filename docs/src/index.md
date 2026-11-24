---
icon: lucide/house
hide:
  - navigation
  - toc
---

<div class="ll-home" markdown>

<div class="ll-band ll-hero" markdown>

<div class="ll-hero__copy" markdown>

# A Lightweight Lakehouse Query Engine

<p class="ll-hero__lead">Explore Iceberg, Delta Lake, Paimon and Hive with SQL. From your terminal or your browser.</p>

[<span class="ll-button-face">Getting Started</span>](getting-started.md){ .md-button .md-button--primary }
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

## Two Ways to Query

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

## Supported Catalogs and Table Formats <span id="one-engine-across-your-lake"></span>

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

## Query from the Browser

<p class="ll-sub">Start Lakelet with <code>--server</code>, query in the built-in web console.</p>

</div>

[Server guide →](server.md){ .ll-go }

</div>

<div class="ll-console" role="region" aria-label="Lakelet console demo">
<div class="ll-console__viewport" tabindex="0" aria-label="Console preview. Scroll horizontally on small screens.">
<div class="ll-console__app">
<div class="ll-console__topbar">
<img src="assets/lakelet-mark.svg" width="28" height="28" alt="">
<strong>Lakelet</strong><span class="ll-console__title">SQL console</span>
<span class="ll-console__demo">Demo</span>
</div>
<div class="ll-console__workspace">
<aside class="ll-console__explorer" aria-label="Sample catalog">
<div class="ll-console__explorer-title">Data explorer</div>
<div class="ll-console__tree">
<details class="ll-console__branch" open><summary><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5M3 12a9 3 0 0 0 18 0"/></svg>tpch</summary>
<div class="ll-console__schema"><details class="ll-console__branch" open><summary><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M3 7V4h6l3 3h9v13H3Z"/></svg>sf1</summary>
<div class="ll-console__tables">
<details class="ll-console__branch ll-console__table-node" open><summary><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="1"/><path d="M3 9h18M9 9v12"/></svg>nation</summary><div class="ll-console__columns"><div class="ll-console__column"><span>n_nationkey</span><span>Int64</span></div><div class="ll-console__column"><span>n_name</span><span>Utf8View</span></div><div class="ll-console__column"><span>n_regionkey</span><span>Int64</span></div><div class="ll-console__column"><span>n_comment</span><span>Utf8View</span></div></div></details>
<details class="ll-console__branch ll-console__table-node" open><summary><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="1"/><path d="M3 9h18M9 9v12"/></svg>region</summary><div class="ll-console__columns"><div class="ll-console__column"><span>r_regionkey</span><span>Int64</span></div><div class="ll-console__column"><span>r_name</span><span>Utf8View</span></div><div class="ll-console__column"><span>r_comment</span><span>Utf8View</span></div></div></details>
</div></details></div></details>
</div>
</aside>
<div class="ll-console__main">
<div class="ll-switch ll-console__tabs" role="tablist" aria-label="SQL examples">
<button class="ll-switch__tab ll-console__tab" type="button" role="tab" id="console-tab-join" aria-controls="console-panel-join" aria-selected="true">Join tables</button>
<button class="ll-switch__tab ll-console__tab" type="button" role="tab" id="console-tab-regions" aria-controls="console-panel-regions" aria-selected="false" tabindex="-1">Browse regions</button>
<button class="ll-switch__tab ll-console__tab" type="button" role="tab" id="console-tab-counts" aria-controls="console-panel-counts" aria-selected="false" tabindex="-1">Count nations</button>
</div>
<div class="ll-console__panel" role="tabpanel" id="console-panel-join" aria-labelledby="console-tab-join">
<div class="ll-console__toolbar"><span class="ll-console__scope"><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5M3 12a9 3 0 0 0 18 0"/></svg>tpch <span>/</span> sf1</span>
<button class="ll-console__copy" type="button" data-console-copy aria-label="Copy join tables SQL">Copy SQL</button>
<button class="md-button md-button--primary ll-console__run" type="button" data-console-run aria-label="Run join tables demo" title="Show the preloaded example results"><span class="ll-button-face"><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="m7 4 14 8-14 8Z" fill="currentColor" stroke="none"/></svg><span data-console-run-label>Run</span></span></button>
</div>
<div class="ll-console__editor">
<div class="ll-console__lines" aria-hidden="true"><span>1</span><span>2</span><span>3</span><span>4</span><span>5</span><span>6</span><span>7</span></div>
<pre class="ll-console__sql" tabindex="0" aria-label="Join tables SQL, read only"><code><span class="ll-console__comment">-- Find nations and their regions.</span>
<span class="ll-console__keyword">select</span> n.n_nationkey <span class="ll-console__keyword">as</span> nation_id,
       n.n_name <span class="ll-console__keyword">as</span> nation, r.r_name <span class="ll-console__keyword">as</span> region
<span class="ll-console__keyword">from</span> tpch.sf1.nation n
<span class="ll-console__keyword">join</span> tpch.sf1.region r <span class="ll-console__keyword">on</span> n.n_regionkey = r.r_regionkey
<span class="ll-console__keyword">order</span> <span class="ll-console__keyword">by</span> region, nation
<span class="ll-console__keyword">limit</span> <span class="ll-console__number">10</span>;</code></pre>
</div>
<div class="ll-console__results-heading"><span>Results</span><span>10 rows</span><span class="ll-console__example-label">Example result</span></div>
<div class="ll-console__results" tabindex="0" role="region" aria-label="Join tables example results">
<table class="ll-console__grid" aria-label="Join tables example results">
<thead><tr><th scope="col" class="ll-console__row-number"><span class="ll-console__sr">Row</span></th><th scope="col" class="ll-console__numeric" title="Int64"><span>nation_id</span></th><th scope="col" title="Utf8View"><span>nation</span></th><th scope="col" title="Utf8View"><span>region</span></th></tr></thead>
<tbody>
<tr><th scope="row" class="ll-console__row-number">1</th><td class="ll-console__numeric">0</td><td>ALGERIA</td><td>AFRICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">2</th><td class="ll-console__numeric">5</td><td>ETHIOPIA</td><td>AFRICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">3</th><td class="ll-console__numeric">14</td><td>KENYA</td><td>AFRICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">4</th><td class="ll-console__numeric">15</td><td>MOROCCO</td><td>AFRICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">5</th><td class="ll-console__numeric">16</td><td>MOZAMBIQUE</td><td>AFRICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">6</th><td class="ll-console__numeric">1</td><td>ARGENTINA</td><td>AMERICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">7</th><td class="ll-console__numeric">2</td><td>BRAZIL</td><td>AMERICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">8</th><td class="ll-console__numeric">3</td><td>CANADA</td><td>AMERICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">9</th><td class="ll-console__numeric">17</td><td>PERU</td><td>AMERICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">10</th><td class="ll-console__numeric">24</td><td>UNITED STATES</td><td>AMERICA</td></tr>
</tbody></table>
</div>
</div>
<div class="ll-console__panel" role="tabpanel" id="console-panel-regions" aria-labelledby="console-tab-regions" hidden>
<div class="ll-console__toolbar"><span class="ll-console__scope"><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5M3 12a9 3 0 0 0 18 0"/></svg>tpch <span>/</span> sf1</span>
<button class="ll-console__copy" type="button" data-console-copy aria-label="Copy browse regions SQL">Copy SQL</button>
<button class="md-button md-button--primary ll-console__run" type="button" data-console-run aria-label="Run browse regions demo" title="Show the preloaded example results"><span class="ll-button-face"><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="m7 4 14 8-14 8Z" fill="currentColor" stroke="none"/></svg><span data-console-run-label>Run</span></span></button>
</div>
<div class="ll-console__editor">
<div class="ll-console__lines" aria-hidden="true"><span>1</span><span>2</span><span>3</span><span>4</span><span>5</span></div>
<pre class="ll-console__sql" tabindex="0" aria-label="Browse regions SQL, read only"><code><span class="ll-console__comment">-- Explore a table in the sample catalog.</span>
<span class="ll-console__keyword">select</span> r_regionkey <span class="ll-console__keyword">as</span> region_id,
       r_name <span class="ll-console__keyword">as</span> region
<span class="ll-console__keyword">from</span> tpch.sf1.region
<span class="ll-console__keyword">order</span> <span class="ll-console__keyword">by</span> region_id;</code></pre>
</div>
<div class="ll-console__results-heading"><span>Results</span><span>5 rows</span><span class="ll-console__example-label">Example result</span></div>
<div class="ll-console__results" tabindex="0" role="region" aria-label="Browse regions example results">
<table class="ll-console__grid" aria-label="Browse regions example results">
<thead><tr><th scope="col" class="ll-console__row-number"><span class="ll-console__sr">Row</span></th><th scope="col" class="ll-console__numeric" title="Int64"><span>region_id</span></th><th scope="col" title="Utf8View"><span>region</span></th></tr></thead>
<tbody>
<tr><th scope="row" class="ll-console__row-number">1</th><td class="ll-console__numeric">0</td><td>AFRICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">2</th><td class="ll-console__numeric">1</td><td>AMERICA</td></tr>
<tr><th scope="row" class="ll-console__row-number">3</th><td class="ll-console__numeric">2</td><td>ASIA</td></tr>
<tr><th scope="row" class="ll-console__row-number">4</th><td class="ll-console__numeric">3</td><td>EUROPE</td></tr>
<tr><th scope="row" class="ll-console__row-number">5</th><td class="ll-console__numeric">4</td><td>MIDDLE EAST</td></tr>
</tbody></table>
</div>
</div>
<div class="ll-console__panel" role="tabpanel" id="console-panel-counts" aria-labelledby="console-tab-counts" hidden>
<div class="ll-console__toolbar"><span class="ll-console__scope"><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5M3 12a9 3 0 0 0 18 0"/></svg>tpch <span>/</span> sf1</span>
<button class="ll-console__copy" type="button" data-console-copy aria-label="Copy count nations SQL">Copy SQL</button>
<button class="md-button md-button--primary ll-console__run" type="button" data-console-run aria-label="Run count nations demo" title="Show the preloaded example results"><span class="ll-button-face"><svg class="ll-console__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="m7 4 14 8-14 8Z" fill="currentColor" stroke="none"/></svg><span data-console-run-label>Run</span></span></button>
</div>
<div class="ll-console__editor">
<div class="ll-console__lines" aria-hidden="true"><span>1</span><span>2</span><span>3</span><span>4</span><span>5</span><span>6</span></div>
<pre class="ll-console__sql" tabindex="0" aria-label="Count nations SQL, read only"><code><span class="ll-console__comment">-- Summarize nations by region.</span>
<span class="ll-console__keyword">select</span> r.r_name <span class="ll-console__keyword">as</span> region, <span class="ll-console__keyword">count</span>(*) <span class="ll-console__keyword">as</span> nations
<span class="ll-console__keyword">from</span> tpch.sf1.nation n
<span class="ll-console__keyword">join</span> tpch.sf1.region r <span class="ll-console__keyword">on</span> n.n_regionkey = r.r_regionkey
<span class="ll-console__keyword">group</span> <span class="ll-console__keyword">by</span> r.r_name
<span class="ll-console__keyword">order</span> <span class="ll-console__keyword">by</span> region;</code></pre>
</div>
<div class="ll-console__results-heading"><span>Results</span><span>5 rows</span><span class="ll-console__example-label">Example result</span></div>
<div class="ll-console__results" tabindex="0" role="region" aria-label="Count nations example results">
<table class="ll-console__grid" aria-label="Count nations example results">
<thead><tr><th scope="col" class="ll-console__row-number"><span class="ll-console__sr">Row</span></th><th scope="col" title="Utf8View"><span>region</span></th><th scope="col" class="ll-console__numeric" title="Int64"><span>nations</span></th></tr></thead>
<tbody>
<tr><th scope="row" class="ll-console__row-number">1</th><td>AFRICA</td><td class="ll-console__numeric">5</td></tr>
<tr><th scope="row" class="ll-console__row-number">2</th><td>AMERICA</td><td class="ll-console__numeric">5</td></tr>
<tr><th scope="row" class="ll-console__row-number">3</th><td>ASIA</td><td class="ll-console__numeric">5</td></tr>
<tr><th scope="row" class="ll-console__row-number">4</th><td>EUROPE</td><td class="ll-console__numeric">5</td></tr>
<tr><th scope="row" class="ll-console__row-number">5</th><td>MIDDLE EAST</td><td class="ll-console__numeric">5</td></tr>
</tbody></table>
</div>
</div>
</div>
</div>
<div class="ll-console__statusbar"><span>Example data</span><span>tpch.sf1</span><span data-console-status role="status" aria-live="polite" aria-atomic="true">Choose a tab to explore</span></div>
</div>
</div>
</div>

</div>

<div class="ll-section" markdown>

<div class="ll-showcase__header" markdown>

<div markdown>

## Query from the CLI

<p class="ll-sub">Start in a SQL shell.</p>

</div>

[CLI quick start →](getting-started.md#lakelet-cli){ .ll-go }

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

## Start Querying

`curl -fsSL https://lakelet.dev/install.sh | sh`

</div>

</div>
