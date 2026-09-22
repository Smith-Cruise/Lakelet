import { RangeSetBuilder, StateField, type EditorState, type RangeSet } from "@codemirror/state";
import { GutterMarker, gutter } from "@codemirror/view";
import { splitStatements, statementAt, type StatementRange } from "../lib/sql";

/**
 * What Run would execute right now: every statement inside the selection, or
 * the statement under the caret when nothing is selected. The gutter bar and
 * the Run button both read this, so they cannot disagree.
 */
export function runTargets(state: EditorState): StatementRange[] {
  const { main } = state.selection;
  if (main.empty) {
    const range = statementAt(state.doc.toString(), main.head);
    return range ? [range] : [];
  }
  // The splitter sees only the selected slice; shift its offsets back into
  // the document.
  return splitStatements(state.sliceDoc(main.from, main.to)).map(({ start, end }) => ({
    start: main.from + start,
    end: main.from + end,
  }));
}

export function runStatements(state: EditorState): string[] {
  return runTargets(state).map(({ start, end }) => state.sliceDoc(start, end));
}

class RunMark extends GutterMarker {
  constructor(private readonly edges: string) {
    super();
  }

  eq(other: RunMark): boolean {
    return other.edges === this.edges;
  }

  toDOM(): HTMLElement {
    const element = document.createElement("div");
    element.className = `lk-run-mark ${this.edges}`.trim();
    return element;
  }
}

const MARK = {
  only: new RunMark("lk-run-mark-start lk-run-mark-end"),
  start: new RunMark("lk-run-mark-start"),
  middle: new RunMark(""),
  end: new RunMark("lk-run-mark-end"),
};

/** One marker per line of each target statement; rounded only at the ends. */
function buildMarks(state: EditorState): RangeSet<GutterMarker> {
  const byLine = new Map<number, RunMark>();
  for (const { start, end } of runTargets(state)) {
    const first = state.doc.lineAt(start).number;
    const last = state.doc.lineAt(end).number;
    for (let n = first; n <= last; n += 1) {
      const mark =
        first === last ? MARK.only : n === first ? MARK.start : n === last ? MARK.end : MARK.middle;
      byLine.set(n, mark);
    }
  }
  const builder = new RangeSetBuilder<GutterMarker>();
  for (const n of [...byLine.keys()].sort((a, b) => a - b)) {
    const { from } = state.doc.line(n);
    builder.add(from, from, byLine.get(n) as RunMark);
  }
  return builder.finish();
}

const runMarks = StateField.define<RangeSet<GutterMarker>>({
  create: buildMarks,
  update: (marks, transaction) =>
    transaction.docChanged || transaction.selection ? buildMarks(transaction.state) : marks,
});

/** The bar itself: a dedicated gutter fed by the state field above. */
export const runGutter = [
  runMarks,
  gutter({
    class: "lk-run-gutter",
    markers: (view) => view.state.field(runMarks),
  }),
];
