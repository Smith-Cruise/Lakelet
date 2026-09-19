import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { EditorView } from "@codemirror/view";
import { tags } from "@lezer/highlight";

const MONO = 'ui-monospace, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace';

/**
 * Colours come straight from the page's CSS variables, so the editor follows
 * the light/dark switch with no theme swap of its own.
 */
export const editorTheme = EditorView.theme({
  "&": {
    height: "100%",
    background: "var(--lk-panel)",
    color: "var(--lk-fg)",
    fontSize: "14px",
  },
  "&.cm-focused": { outline: "none" },
  ".cm-scroller": { fontFamily: MONO, lineHeight: "22px" },
  ".cm-content": { padding: "12px 0", caretColor: "var(--lk-fg)" },
  ".cm-line": { padding: "0 12px 0 6px" },

  ".cm-gutters": {
    background: "var(--lk-panel)",
    color: "var(--lk-fg-faint)",
    border: "none",
    paddingLeft: "10px",
  },
  ".cm-lineNumbers .cm-gutterElement": { minWidth: "26px", padding: "0 4px 0 0" },
  ".cm-activeLineGutter": { background: "transparent", color: "var(--lk-fg-muted)" },
  ".cm-activeLine": { background: "color-mix(in srgb, var(--lk-accent) 6%, transparent)" },

  ".cm-cursor, .cm-dropCursor": { borderLeftColor: "var(--lk-fg)", borderLeftWidth: "2px" },
  "&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground":
    { background: "color-mix(in srgb, var(--lk-accent) 18%, transparent)" },
  ".cm-selectionMatch": { background: "color-mix(in srgb, var(--lk-accent) 10%, transparent)" },

  // Completion list, styled as one of the page's cards.
  ".cm-tooltip": { border: "none", background: "transparent" },
  ".cm-tooltip.cm-tooltip-autocomplete": {
    background: "var(--lk-panel)",
    border: "0.5px solid var(--lk-border)",
    borderRadius: "8px",
    boxShadow: "var(--lk-shadow)",
    padding: "4px",
    overflow: "hidden",
  },
  ".cm-tooltip.cm-tooltip-autocomplete > ul": {
    fontFamily: MONO,
    fontSize: "13px",
    maxHeight: "268px",
    minWidth: "180px",
  },
  ".cm-tooltip.cm-tooltip-autocomplete > ul > li": {
    height: "28px",
    lineHeight: "28px",
    padding: "0 10px",
    borderRadius: "6px",
    color: "var(--lk-fg)",
  },
  ".cm-tooltip.cm-tooltip-autocomplete > ul > li[aria-selected]": {
    background: "var(--lk-accent-bg)",
    color: "var(--lk-fg)",
  },
  ".cm-completionMatchedText": {
    color: "var(--lk-accent)",
    fontWeight: "500",
    textDecoration: "none",
  },
  ".cm-completionDetail": { color: "var(--lk-fg-faint)", fontStyle: "normal", marginLeft: "8px" },
});

/** The same four syntax colours the rest of the app defines. */
export const sqlHighlight = syntaxHighlighting(
  HighlightStyle.define([
    { tag: [tags.keyword, tags.operatorKeyword, tags.typeName], color: "var(--lk-syntax-keyword)" },
    { tag: [tags.string, tags.special(tags.string)], color: "var(--lk-syntax-string)" },
    { tag: [tags.number, tags.integer, tags.float], color: "var(--lk-syntax-number)" },
    {
      tag: [tags.comment, tags.lineComment, tags.blockComment],
      color: "var(--lk-syntax-comment)",
      fontStyle: "italic",
    },
  ]),
);
