import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import { EditorView, keymap, placeholder } from "@codemirror/view";
import { EditorState, Prec } from "@codemirror/state";
import { basicSetup } from "codemirror";
import { sql } from "@codemirror/lang-sql";
import { syntaxHighlighting } from "@codemirror/language";
import { classHighlighter } from "@lezer/highlight";

export interface SqlEditorHandle {
  getValue: () => string;
  setValue: (value: string) => void;
  insertAtCursor: (text: string) => void;
  focus: () => void;
}

interface SqlEditorProps {
  /** Invoked on Mod-Enter. Read through a ref so the binding never goes stale. */
  onRun: () => void;
}

export const SqlEditor = forwardRef<SqlEditorHandle, SqlEditorProps>(function SqlEditor(
  { onRun },
  ref,
) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onRunRef = useRef(onRun);
  onRunRef.current = onRun;

  useEffect(() => {
    const state = EditorState.create({
      doc: "",
      extensions: [
        basicSetup,
        sql({ upperCaseKeywords: true }),
        syntaxHighlighting(classHighlighter),
        placeholder("select * from catalog.schema.table limit 10"),
        Prec.highest(
          keymap.of([
            {
              key: "Mod-Enter",
              run: () => {
                onRunRef.current();
                return true;
              },
            },
          ]),
        ),
      ],
    });
    const view = new EditorView({ state, parent: containerRef.current! });
    viewRef.current = view;
    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, []);

  useImperativeHandle(ref, () => ({
    getValue: () => viewRef.current?.state.doc.toString() ?? "",
    setValue: (value: string) => {
      const view = viewRef.current;
      if (!view) return;
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: value },
      });
    },
    insertAtCursor: (text: string) => {
      const view = viewRef.current;
      if (!view) return;
      const { from, to } = view.state.selection.main;
      view.dispatch({
        changes: { from, to, insert: text },
        selection: { anchor: from + text.length },
      });
      view.focus();
    },
    focus: () => viewRef.current?.focus(),
  }));

  return <div ref={containerRef} className="h-full min-h-0 overflow-auto" />;
});
