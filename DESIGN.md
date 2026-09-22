# Lakebed — Lakelet's design language

Lakelet has two user-facing surfaces, and they share one look:

- **the docs site** — `docs/`, built with [Zensical](https://zensical.org/)
- **the web console** — `web/`, React 19 + Vite + Tailwind v4, embedded in the binary and
  served on the same port as Flight SQL

Lakebed is near-black ink on warm paper, square corners, hard rules, and a single
high-voltage accent. The docs site is the loud version of it; the console is the quiet
version. Same vocabulary, different volume — the console shows a hundred rows of data at a
time, so anything that fights the data loses.

This file is the contract. Read it before changing anything visual on either surface.

---

## The five rules

These are the ones that break the design if you get them wrong. Everything else is detail.

1. **Volt is a fill and mark colour, never a text colour.** `#C8F135` sits at roughly 2:1
   against paper — unreadable as type. Use it as a background, a bar, a border, a block.
   Anything drawn *on* volt is ink. A "success" tick is a filled volt square, not a volt
   glyph. An active tree row is a volt wash with ink text and a volt edge bar.
2. **There is one theme.** No dark mode, no theme toggle, no `prefers-color-scheme`
   branch. This is deliberate, not unfinished — see [No dark mode](#no-dark-mode).
3. **Nothing is rounded.** Zero radius everywhere. No pills, no capsules, no rounded
   avatars.
4. **Depth is offset, never blurred.** Raised things carry a hard shadow
   (`4px 4px 0`, `5px 5px 0`, `6px 6px 0` by weight). No soft shadows, no blur, no glow,
   no gradients — the one exception is the hero band's faint grid texture.
5. **Both surfaces share one palette.** A colour changed in one belongs in the other.
   See [Where the tokens live](#where-the-tokens-live).

---

## Colour

One palette, two names for it: `--lk-*` in the console, `--ll-*` in the docs. The values
are identical; only the prefix differs.

| Role | Value | Console token | Docs token |
| --- | --- | --- | --- |
| Ink — body text and structural borders | `#14170F` | `--lk-fg`, `--lk-border-strong` | `--ll-ink`, `--ll-rule` |
| Ink, secondary text | `#5A5E4E` | `--lk-fg-muted` | `--ll-ink-soft` |
| Ink, tertiary text and placeholders | `#8C907E` | `--lk-fg-faint` | `--ll-ink-faint` |
| Paper — page canvas, recessed strips | `#EBE9DE` | `--lk-page` | `--ll-paper` |
| Panel — the working surface | `#F7F6EF` | `--lk-panel` | `--ll-panel` |
| Panel alt — grid rows, popovers, cards | `#FFFFFF` | `--lk-panel-alt` | `--ll-panel-alt` |
| Sub — inset chips, `kbd` | `#E4E2D6` | `--lk-sub` | — |
| Hairline — internal dividers | `rgba(20,23,15,.14)` | `--lk-border` | `--ll-line` |
| Finest line — grid rows | `rgba(20,23,15,.07)` | `--lk-border-soft` | — |
| **Volt** — the only accent | `#C8F135` | `--lk-accent` | `--ll-volt` |
| Volt deep — volt hovers and edges | `#A7CE1C` | `--lk-accent-deep` | `--ll-volt-deep` |
| Volt wash — selection, run marks | `#EDF9C4` | `--lk-accent-bg` | `--ll-volt-wash` |
| On volt — text sitting on volt | `#14170F` | `--lk-accent-contrast` | (ink) |
| Danger | `#C4392B` / wash `#F7DED9` | `--lk-danger-fg` / `-bg` | `--ll-danger` |
| Warning | `#B8791A` / wash `#F6E9C9` | `--lk-warning-fg` / `-bg` | `--ll-warning` |

**Ink is greenish-black, paper is warm off-white.** Neither is neutral grey. Do not
substitute `#000`, `#111`, `#FFF` or a Tailwind grey.

**Success is volt.** `--lk-success-fg` is ink and `--lk-success-bg` is the volt wash, on
purpose: a successful query lights up the one high-voltage colour. There is no green.

### Colour on ink bands

The header, the tab strip, the status bar and the closing band are solid ink. On ink, saturated mid-tone colours fail: `#C4392B` reads at about 2.5:1. So a failure
state on an ink band is a **filled chip** — danger background with paper text — not
coloured lettering. Volt is the exception; it is bright enough to be text on ink, and that
is the only place volt may carry a word.

### SQL syntax colours

Deliberately low in chroma, so volt stays the only saturated thing on screen.

| Token | Value | Console token |
| --- | --- | --- |
| keyword | `#B5451F` | `--lk-syntax-keyword` |
| string | `#3D7A2E` | `--lk-syntax-string` |
| number | `#8A6A1F` | `--lk-syntax-number` |
| comment | `#8C907E` | `--lk-syntax-comment` |

The docs mirror these through `--md-code-hl-*`.

---

## Typography

Two families, and the split carries meaning: **mono is for anything machine-shaped** —
headings, labels, identifiers, code, data, numbers, status. **Inter is for prose** —
paragraphs, descriptions, help text.

| Use | Family | Weight | Notes |
| --- | --- | --- | --- |
| Display / hero | JetBrains Mono | 800 | `letter-spacing: -0.03em`, `line-height: 0.95` |
| Headings | JetBrains Mono | 800 / 700 | H2 on the docs home is uppercase |
| Eyebrow labels | JetBrains Mono | 700 | 10–11px, uppercase, `letter-spacing: .16em` |
| Body | Inter | 400 / 500 | `line-height` 1.6 |
| Code, data, UI values | JetBrains Mono | 400 / 500 | every result-grid cell |

Eyebrows are everywhere: `DATA EXPLORER`, `RESULTS`, `CONSOLE`, `INTERFACES`, `COVERAGE`.
They mark a region without a heading. Keep them small, wide-tracked and faint.

**Fonts are self-hosted in the console.** It ships inside the binary and often runs with no
internet, so `web/src/styles.css` declares its own `@font-face` rules pointing at the
`@fontsource-variable/*` latin woff2 files. **Do not `@import` the packages' own
stylesheets** — they carry every script and drag ~2 MB into the bundle instead of 88 KB.
The docs site loads the same two families from Google Fonts, plus one extra `@import` for
JetBrains Mono 800, because the theme's own font link stops at 700.

---

## The mark

A bold `L` in ink on a volt tile, on a 32×32 artboard:

```
<rect width="32" height="32" fill="#C8F135"/>
<rect x="6" y="5" width="6.5" height="22" fill="#14170F"/>
<rect x="6" y="20.5" width="20" height="6.5" fill="#14170F"/>
```

It is deliberately typographic rather than pictorial. The mark has to survive a 16px
favicon, where any symbol carrying an idea — ripples, a wave, layers, a depth profile —
collapses into a blur; a letter degrades into a legible letter. It also rhymes with the
mono wordmark beside it, so the lockup reads as one thing. The personality lives in the
volt tile and the type, not in the glyph.

The tile is square and full-bleed: it carries its own background, so never pad it into a
circle, add a radius, or drop the tile and use the bare `L`. It lives in four places and
they must stay identical — `docs/src/assets/lakelet-mark.svg` (the docs header),
`docs/src/assets/favicon.png` (64×64, generated from the same rectangles),
`web/public/favicon.svg`, and `web/src/components/Logo.tsx`, which is the only copy that
draws from CSS variables rather than literal hexes.

## Geometry, depth and motion

- **Radius: 0.** Everywhere.
- **Borders:** structural edges are full-strength ink — 2px in the docs, 1px in the
  console. Internal dividers are the hairline token. Grid rows use the finest line.
- **Shadows:** `4px 4px 0` for popovers and buttons, `5px 5px 0` for cards, `6px 6px 0`
  for the install card, `8px 8px 0` for the console screenshot frame. Buttons use
  `2px 2px 0` and swap it for a 1px translate on `:active`, so pressing looks like
  pressing.
- **Focus:** `0 0 0 2px var(--lk-accent)`. Highly visible and on-brand. Never remove a
  focus ring.
- **Hover on raised cards:** translate `-2px, -2px` and deepen the shadow — the card lifts
  toward you. 120ms.
- **Motion is minimal.** Short transitions on transform, shadow and colour. No entrance
  animations, no parallax, no easing showpieces.

---

## No dark mode

Lakebed is a single theme. This was a decision, not an omission, and it is enforced in
both surfaces:

- `docs/zensical.toml` has **no `[[project.theme.palette]]` table**. Without one the theme
  emits no `data-md-color-scheme` on `<body>`, renders no light/dark toggle in the header,
  and falls back to its own `:root` defaults, which `extra.css` overrides wholesale. Adding
  a palette entry back would reintroduce the toggle.
- `web/src/styles.css` has a single `:root` with `color-scheme: light`, no
  `@media (prefers-color-scheme: dark)` block and no `[data-theme]` selector. The store
  carries no theme state and clears the retired `lakelet.theme` key on load.

If dark mode is ever wanted again, the token indirection makes it cheap: add a second set
of values behind the same `--lk-*` / `--ll-*` names and the components do not change. Do
not reintroduce it without being asked.

---

## Where the tokens live

| What | File |
| --- | --- |
| Console tokens, fonts, Tailwind theme bridge | `web/src/styles.css` |
| Console grid theme | `web/src/components/TableView.tsx` (`themeQuartz.withParams`) |
| Console editor theme and syntax colours | `web/src/editor/theme.ts` |
| Docs tokens, site chrome, home page | `docs/src/stylesheets/extra.css` |
| Docs theme config, logo, favicon, fonts | `docs/zensical.toml` |
| Docs home page markup | `docs/src/index.md` |
| Mark, favicon, console screenshot | `docs/src/assets/`, `web/public/favicon.svg`, `web/src/components/Logo.tsx` |

### Console specifics

- Tailwind v4 is CSS-first; there is no `tailwind.config.js`. Tokens reach utility classes
  through the `@theme inline` block: `bg-page`, `bg-panel`, `bg-panel-alt`, `border-line`,
  `border-line-strong`, `text-fg`/`-muted`/`-faint`, `bg-accent`, `bg-accent-bg`,
  `text-accent-contrast`, `shadow-card`, `shadow-press`, `font-mono`, `font-sans`.
  **Use these, never raw Tailwind palette colours** (`slate-500` and friends do not exist
  in this design).
- The same `@theme inline` block sets every `--radius-*` to `0`, which flattens all
  `rounded-*` utilities at once. If something is still rounded, it is using an arbitrary
  value like `rounded-[7px]` — fix that at the call site rather than adding overrides.
- `TableView.tsx` and `editor/theme.ts` read the CSS variables directly rather than
  duplicating hexes. Keep it that way.

### Docs specifics

- `extra.css` overrides Material's `--md-*` variables on plain `:root`. `extra_css` loads
  after the theme stylesheet, so same-specificity wins. There is no
  `[data-md-color-scheme=…]` selector anywhere, and there should not be.
- The `modern` variant paints the header translucent white; Lakebed forces it to ink so the
  header and tab row read as one block, matching the console's top bar. The header's search
  icon is a mask painted with the default foreground, so it needs an explicit paper colour
  or it disappears.
- `.md-header`, `.md-tabs` and the prev/next footer sit **outside** `.md-typeset`, so
  they do not inherit the typeset link colour. Style them explicitly. `.md-footer-meta`
  is the opposite trap: it *carries* `.md-typeset`, so the site's link rule reaches it,
  and on top of that the theme repaints its links with an `html`-prefixed selector
  (`html .md-footer-meta.md-typeset a:not(:focus, :hover)`) that resolves to ink — ink
  on an ink band. Overriding it needs the same `html` prefix.
- Doc tables render as `display: inline-block` so they can scroll. A full-width table must
  say `display: table` and carry its own class, or Material's `table:not([class])` rules
  collapse it to its content width.
- Home page sections are full-bleed bands. `body:has(.ll-home)` strips the content padding
  and the grid max-width; each band supplies its own padding. A block with side margins
  must use `width: auto; max-width: …`, not `width: min(100%, …)`, or it overflows on
  narrow screens.
- `navigation.instant` swaps page content without a reload, so every handler in
  `docs/src/javascripts/extra.js` is delegated on `document`. Never bind to an element at
  load time — it survives exactly one navigation.
- `md_in_html` wraps a markdown link in its own `<p>`, so an `<a>` inside a card is not
  the flex item. Padding on an inline box does not grow its line, so vertical padding on
  such a link bleeds over the text above it. Put the spacing and any `margin-top: auto` on
  the wrapping paragraph, and give the link `display: inline-block`.
- The theme's `.md-typeset ol:not([hidden])` out-specifies `.md-typeset .some-class`, so
  a class alone cannot change a list's `display`. Name the element too (`ol.some-class`).

---

## Surface patterns

### Docs home page

Seven bands, top to bottom, all full-bleed. Classes live in `docs/src/index.md` and are
styled in the home-page section of `extra.css`.

1. `.ll-hero` — volt band with a faint ink grid, an ink `.ll-kicker` chip, a mono-800 H1,
   an Inter `.ll-lead`, and two buttons (primary = ink fill with volt text).
2. `.ll-install` — terminal card straddling the band's bottom edge, `6px 6px 0` shadow.
   The `$` prompt is a `::before`, so the copied command stays runnable.
3. `.ll-modes` — two hard-edged cards that lift on hover. They are a mid-page table of
   contents, not a destination: each jumps to its own showcase band further down, in the
   same order the bands appear. The links out to the docs belong in those bands, not here.
4. `.ll-matrix` — catalog × table-format grid. Three states, three shapes, so colour is
   never the only cue: `.ll-yes` (filled volt block with a tick), `.ll-beta` (outlined,
   volt-wash, `β`), `.ll-no` (a dash). Always ship the `.ll-legend` with it. **Keep the
   cells honest** — they are read out of `docs/src/catalogs/*.md` and
   `docs/src/table-formats/*.md`, not invented.
5. `.ll-shot` — the console. A real screenshot, nothing on top of it, in a 2px frame
   with an `8px 8px 0` shadow.
6. `.ll-term` — the CLI. A terminal card with a tab strip over three captured sessions.
7. `.ll-close` — ink band, volt-bordered install command, colophon on its own line.

Bands 5 and 6 are a pair and should stay one: a picture of the console, then the same
engine as type. Each closes with a `.ll-caption` giving the command that starts it and a
`.ll-go` link into the docs.

The screenshot was once annotated: four numbered volt markers over the image, each
opening a card about the region under it. It was tried and removed as a net loss. The
markers land on top of the one thing the band exists to show, and they sell the UI short
— a console whose parts need labelling has not made its case. If a part of the console
genuinely needs explaining, explain it in `server.md`, not on top of the picture.

There was once another band between the install card and the modes: a stat strip of four
big volt figures. It was removed, and a replacement should not be added back without new
material. Two of its four cells were not quantities at all, so the device contradicted
itself, and three of the four restated the hero's own lead sentence six lines above them.
The page does not need a band there — the hero's weight, the install card's shadow, the
matrix's ink header row and the closing band already carry the rhythm.

#### Terminal card — `.ll-term`

A grown-up `.ll-install`: same ink frame, same three window marks, same paper surface,
`8px 8px 0` shadow. It is deliberately **not** a dark terminal; a black panel here would
read as a stray dark theme.

- Sessions are `<pre>` with hand-marked spans: `.ll-t-p` for the prompt (volt-deep, the
  only accented thing), `.ll-t-box` for box-drawing characters, `.ll-t-dim` for banners
  and footers, `.ll-t-kw` / `.ll-t-str` from the SQL palette, `.ll-t-err` in danger.
- `line-height` stays at exactly `1`, at a whole-pixel `font-size`. The box-drawing
  characters are not in the webfont's subset, come from the fallback monospace, and fill
  exactly one em: any looser leading leaves a printed table as stacked rectangles, and a
  fractional size gaps on subpixel rounding. Terminal leading is the right look anyway —
  the blank lines between statements carry the spacing.
- A blank line inside the `<pre>` is written as `<span class="ll-t-gap"></span>`. A truly
  empty line would end the raw HTML block that the `<pre>` lives in.
- `.ll-term__body` carries a `min-height` equal to the tallest session, so switching tabs
  never resizes the card. Re-measure it when a session changes.
- **The sessions are captured, not composed.** Run the binary and paste what it prints.
  The REPL renders with comfy-table (`╭─┬─╮`); `--command` and `--file` render plain
  ASCII (`+---+`). Getting that wrong is the tell that a session was invented. Table and
  column names may stand in for a lake nobody can reach, but every byte of the framing,
  the prompts (`sql> `, `  -> `) and the `N row(s) fetched.` / `Elapsed …` footer is real.

#### Tab strips — `.ll-switch`

Hard-edged file tabs on the recessed tone, the active one lifted onto the panel under a
2px volt cap — the same device the console uses for its editor tabs, and it should stay
that way on both surfaces. Real `role="tablist"` / `role="tab"` / `aria-selected`, panels
toggled with `hidden`, and roving `tabindex` driven from
`docs/src/javascripts/extra.js`.

### Docs content pages

Ink header and tab row; volt underline on the active tab; volt left bar plus wash on the
active sidebar item; square code blocks with a 2px ink border; inline code on panel with a
hairline (**not** volt wash — several per paragraph would turn the accent into wallpaper);
ink header rows on tables; admonitions square with a 4px coloured left edge; links are ink
with a volt underline, and gain a volt wash on hover.

### Web console

Fixed rhythm — keep it unless there is a reason: top bar 44px, tab strip 30px, editor
toolbar 38px, results header 30px, status bar 24px, tree row 26px, grid row 27px, grid
header 30px, base font 13px, editor 13px, grid 12px.

- **Top bar** — ink band, volt logo tile, mono wordmark, `CONSOLE` badge outlined in volt,
  and a quiet `DOCS ↗` link on the right. No theme toggle.
- **Explorer** — mono tree on panel, square rows; the selected table gets the volt wash,
  ink text and `inset 3px 0 0` volt bar.
- **Editor** — tabs are hard-edged files on the recessed page tone; the active one lifts
  onto the panel with a volt cap on top. Run is a volt button with ink text, an ink border
  and a press shadow. The statement Run would execute is marked by the volt gutter bar plus
  a wash across its lines.
- **Results** — recessed header strip with a mono `RESULTS` eyebrow, a volt block and mono
  figures for the outcome, and a square CSV button. The grid is entirely monospaced so
  digits line up down a column; the header sits a shade back over one full-strength rule,
  rows are separated by the finest line, and there is no zebra striping.
- **Status bar** — ink strip of mono cells: connection, endpoint, scope, rows, elapsed,
  and the run shortcut. Failure states are filled chips.

---

## Accessibility

- Volt never carries text on paper. On ink, volt is the only colour that may.
- State is never colour alone: pair it with a shape, an icon or a word — the matrix's
  block/outline/dash, the results' volt square beside its row count.
- Body text meets 4.5:1 against its surface. Check any new pairing, especially on ink
  bands.
- Focus rings are 2px volt and must stay visible. Do not trade them for a subtler cue.
- Pointer targets are 24×24 CSS px in the docs. The console is denser, and a few icon
  buttons sit at 20×20 inside a 30px strip; treat 20px as the floor there and do not go
  below it for an action that has no other route.
- Interactive home-page devices carry real semantics, not just styling: the tab strip is
  a `role="tablist"` with roving `tabindex` and arrow/Home/End keys, and its panels are
  toggled with `hidden`. Anything hidden at a narrow width reappears as readable text
  rather than disappearing.
- Where a focus ring lands on paper, volt alone is far too quiet, so pair it:
  `0 0 0 2px volt, 0 0 0 4px ink` — volt first to stay on-brand, ink behind it to be
  seen. The home page's tab strip and session panels use exactly that.

---

## Changing the design

Small change — a new component, a new page section:

1. Build it from existing tokens. If you need a colour that is not in the table above,
   that is a signal the design is being diluted, not that a token is missing.
2. Check it against [the five rules](#the-five-rules).
3. Verify in a browser, both surfaces if the change touches shared vocabulary.

Changing a token value, a font, or the geometry:

1. Update **both** `web/src/styles.css` and `docs/src/stylesheets/extra.css`.
2. Update this file's tables in the same change.
3. Re-check the ink bands — most contrast regressions land there.
4. Re-take `docs/src/assets/console.png` if the console's look changed; the docs home page
   shows the real UI, and a stale screenshot is worse than none.

Do not switch away from Lakebed — different accent, dark mode, rounded corners, a soft
shadow scale — unless the user explicitly asks for it. If a change would quietly erode it,
say so instead of shipping it.

### Verifying

```bash
pnpm -C web dev                 # console on :5173, proxies Flight SQL to :32010
pnpm -C web typecheck           # required when web/ changes
docs/.venv/bin/zensical serve   # docs on :8000
```

Check: no horizontal scroll at 1440 / 768 / 375; `prefers-color-scheme: dark` leaves both
surfaces on paper; the docs header has no light/dark toggle; fonts actually load
(`document.fonts.check`); focus rings visible; a real query renders in the grid.

To see the console as shipped rather than in dev, `pnpm -C web build` then `cargo build` —
the binary embeds `web/dist`.
