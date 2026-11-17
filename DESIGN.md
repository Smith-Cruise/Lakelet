# Lakeside — Lakelet's design language

Lakelet's docs and web console share a light, illustrated language inspired by
MotherDuck: warm paper, lake-blue controls, charcoal outlines, and small hard shadows.
The subject is Lakelet's own world: a small engine exploring open data lakes. The
homepage's lake-and-table illustration is the focal point; the console is the quiet
working counterpart, where SQL and data take priority over decoration.

Read this file before changing anything visual in `web/` or `docs/`.

## Principles

1. Paper is the canvas, blue is the action, yellow is a highlight. Pale fills carry
   charcoal text. Use the separate deep-blue token for links and focus outlines.
2. One light theme. No theme toggle or automatic colour-scheme change.
3. Quiet structure: 1px outlines in the console, up to 2px on docs feature frames.
   Internal separators are lighter. Avoid nested decorative boxes.
4. Hard depth, small scale. No blurred shadows, gradients or glows. Buttons and
   floating surfaces use solid depth; data panes stay flat.
5. Shared vocabulary, different density. The homepage is expressive; prose pages and
   the console support sustained reading.

## Shared tokens

The console uses `--lk-*`; the docs use `--ll-*`. Update both together.

| Role | Value | Console | Docs |
| --- | --- | --- | --- |
| Ink / structural border | `#30383A` | `--lk-fg`, `--lk-border-strong` | `--ll-ink`, `--ll-rule` |
| Secondary text | `#5B686A` | `--lk-fg-muted` | `--ll-ink-soft` |
| Quiet text | `#627174` | `--lk-fg-faint` | `--ll-ink-faint` |
| Paper | `#F5F1E9` | `--lk-page` | `--ll-paper` |
| Panel | `#FFFCF7` | `--lk-panel` | `--ll-panel` |
| Work surface | `#FFFFFF` | `--lk-panel-alt` | `--ll-panel-alt` |
| Inset surface | `#ECE8DF` | `--lk-sub` | `--ll-sub` |
| Hairline | `rgba(48,56,58,.18)` | `--lk-border` | `--ll-line` |
| Grid line | `rgba(48,56,58,.08)` | `--lk-border-soft` | `--ll-line-soft` |
| Lake blue | `#95D5EE` | `--lk-accent` | `--ll-sky` |
| Blue hover fill | `#78C4E2` | `--lk-accent-deep` | `--ll-sky-deep` |
| Blue wash | `#E5F4FA` | `--lk-accent-bg` | `--ll-sky-wash` |
| Deep blue text / focus | `#24657C` | `--lk-link` | `--ll-link` |
| Sun yellow | `#F3CF65` | `--lk-sun` | `--ll-sun` |
| Yellow wash | `#FFF3CE` | `--lk-sun-bg` | `--ll-sun-wash` |
| Danger / wash | `#A6362B` / `#FCE9E4` | `--lk-danger-fg` / `-bg` | `--ll-danger` / `-wash` |
| Warning / wash | `#805A12` / `#FFF3CE` | `--lk-warning-fg` / `-bg` | `--ll-warning` / `-wash` |

Success uses ink on blue wash and an explicit icon or label. Syntax colours are
`#285E88` (keyword), `#39704E` (string), `#95602C` (number), and quiet text (comment).
The docs mirror these through `--md-code-hl-*`.

## Typography

Use JetBrains Mono for display, code, identifiers and data; Inter for prose,
navigation and ordinary controls. The lighter mono heading feels approachable.
Avoid artificially wide uppercase labels.

| Use | Family | Weight / size |
| --- | --- | --- |
| Homepage heading | JetBrains Mono | 500, fluid 32–54px, 1.16 leading |
| Section / document headings | JetBrains Mono | 500, 24–36px |
| Brand wordmark | Inter | 700 |
| Prose | Inter | 400, 1.7 leading |
| UI labels | Inter | 500 / 600, 12–13px |
| Editor / grid | JetBrains Mono | 400, 13px / 12px |

Both fonts are self-hosted, including the docs. Use only the Latin variable WOFF2
subsets from the installed Fontsource packages, with their OFL licences. Do not import
all script subsets or depend on a font CDN. The docs set `theme.font = false` and
use font faces declared in `extra.css`.

## Geometry and motion

- Controls and cards have a 2px radius; tables, pane joins and code stay square.
- Primary actions use a fixed ink base and a separate coloured face. At rest they
  overlap. Hover moves the face 7px right and 7px up in 120ms ease-in-out, exposing
  the base below and to the left. Press returns it to the base in 30ms; release
  restores the hover lift. Apply this to homepage CTAs and the console Run button.
  Keep the base as the stable hover target and do not move surrounding layout.
- Hard shadow: 3px on floating menus and 5px on showcase frames.
- Focus: 2px deep-blue outline with a 2px offset. Pale blue alone is insufficient.
- Keyboard focus gets the same lift plus an outline. Disabled controls do not lift;
  reduced motion uses a colour change instead. Pointer hover requires a fine pointer.
  Shared button tokens are `--lk/ll-button-lift`, `--lk/ll-button-duration` and
  `--lk/ll-button-press-duration`. No entrance animations, parallax or decorative loops.
- Illustrations may use curves: lake contours and ripples are organic shapes.

## Mark and illustration

Keep the existing cut-through bolt; its tile is now sun yellow and its ink charcoal.
All copies must match: `web/src/components/Logo.tsx`, `web/public/favicon.svg`,
`docs/src/assets/lakelet-mark.svg`, and the generated 64px `docs/src/assets/favicon.png`.
Keep the tile square and its three cuts legible.

`docs/src/assets/lake-query.svg` is an original vector illustration: a blue lake connects
four open table formats to a small SQL window. The image is decorative; the text and
coverage matrix carry the facts. Do not copy MotherDuck's ducks or proprietary graphics.

## Layout

Home and Docs share a 58.4rem (1168px) outer grid for the header, tabs and footer.
The docs reading layout uses this same grid. Only the homepage's main container
extends to full width for its paper / blue-wash bands, with content capped at
56rem (1120px). Keep shared chrome at the same width across navigation.
Content aligns left. The two-column hero becomes one column on phones.

```text
Lakelet                            Home   Docs   Search   GitHub
---------------------------------------------------------------
Product heading                    Lake + open tables
Explanation                        SQL window illustration
Get started / GitHub
Install command
---------------------------------------------------------------
Two ways to query                  Server / CLI entry points
One engine across your lake        Catalog × format matrix
---------------------------------------------------------------
Query from the browser             Guide
[ Real console screenshot                                    ]
---------------------------------------------------------------
Query from the CLI                 Guide
[ Explore | Query | Script                                   ]
---------------------------------------------------------------
Start querying                     Install command
```

The lake illustration makes the design specific to Lakelet. Avoid arbitrary statistics,
repeated numbered labels and screenshot annotations. Keep supported/experimental
states and captured CLI output accurate.

### Documentation

Cream header and tab row, blue active navigation, readable prose, white code and table
surfaces. Links have deep-blue text and visible underlines. Table headers use blue wash;
alerts use a label/icon and a coloured edge. Wide code and tables scroll locally.

Zensical details to preserve:

- No `[[project.theme.palette]]` table, which would restore a theme toggle.
- `project.extra.generator = false` removes the footer generator credit.
- Overrides use plain `:root` and load after the theme stylesheet.
- Header and footer are outside `.md-typeset`; style their links explicitly.
- The coverage table uses `display: table; width: 100%` inside a scrolling wrapper.
- Instant navigation requires delegated handlers in `extra.js`.
- Markdown wraps card links in paragraphs; put flex spacing on those wrappers.
- CLI tabs keep their roles, roving tabindex and hidden panels. Captured terminal
  output uses whole-pixel font sizes and exactly `line-height: 1` so box characters
  connect. Do not fabricate output or change the captured sessions for decoration.

### Web console

Cream top and status bars frame an outlined workspace. The explorer is paper; editor
and grid are white. Active query tabs have a blue cap; Run is a blue press button.
Results have a light-blue header and fine row dividers.

Rhythm: top bar 60px, tabs 38px, toolbar 46px, results header 38px, status bar 28px,
tree rows 30px, grid rows 31px, grid header 36px. Preserve resizable panels. The console
targets desktop use with a 960px minimum workspace width; it does not need a mobile
layout or collapsible explorer. Long scope names truncate without moving Run offscreen.

The editor and grid read CSS variables in `editor/theme.ts` and `TableView.tsx`.
Tailwind v4's `@theme inline` maps shared tokens to utilities. Use those utilities
instead of raw palette classes. The embedded console's fonts must work offline.

## Verification and maintenance

When changing colour, font or geometry, update `web/src/styles.css`,
`docs/src/stylesheets/extra.css` and this file together. Retake
`docs/src/assets/console.png` when the console changes; it must show the real UI.

Verify docs at desktop and phone widths, and the console at desktop widths. Check
keyboard focus, navigation/search, CLI tabs,
catalog expansion, SQL execution, error feedback and result sorting. Colour never
carries state alone. Run web typecheck/build and docs build in the selected Python
environment. Follow `AGENTS.md` for PR checks.
