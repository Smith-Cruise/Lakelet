import { useEffect, useRef } from "react";
import { EditorState, Prec } from "@codemirror/state";
import {
  EditorView,
  drawSelection,
  highlightActiveLine,
  highlightActiveLineGutter,
  keymap,
  lineNumbers,
} from "@codemirror/view";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { autocompletion, completionKeymap } from "@codemirror/autocomplete";
import { PostgreSQL, sql } from "@codemirror/lang-sql";
import { LoaderCircle, Play, Plus, X } from "lucide-react";
import { ScopePicker } from "./ScopePicker";
import { sqlKeywords } from "../editor/keywords";
import { runGutter, runStatements } from "../editor/runMarks";
import { editorTheme, sqlHighlight } from "../editor/theme";
import { useApp, type EditorTab } from "../store";

interface Props {
  tab: EditorTab;
  running: boolean;
  onRun: (statements: string[]) => void;
}

export function SqlEditor({ tab: active, running, onRun }: Props) {
  const { tabs, activeTabId, setActiveTab, addTab, closeTab, updateSql } = useApp();
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<EditorView | null>(null);
  // One EditorState per tab, so each keeps its own undo history and cursor.
  const states = useRef(new Map<string, EditorState>());
  const shownIdRef = useRef<string | null>(null);
  // Refs so the editor, created once, always sees the current tab and handler.
  const runRef = useRef(onRun);
  runRef.current = onRun;
  const activeIdRef = useRef(active.id);
  activeIdRef.current = active.id;
  const makeStateRef = useRef<(doc: string) => EditorState>(() => EditorState.create({ doc: "" }));

  const runCurrent = () => {
    const editor = view.current;
    if (!editor) {
      return;
    }
    const statements = runStatements(editor.state);
    if (statements.length > 0) {
      runRef.current(statements);
    }
  };

  useEffect(() => {
    if (!host.current) {
      return;
    }
    const extensions = [
      lineNumbers(),
      runGutter,
      highlightActiveLineGutter(),
      highlightActiveLine(),
      history(),
      drawSelection(),
      // Postgres is the closest dialect for highlighting; completion comes
      // from our own short keyword list, so `override` sidelines the
      // dialect's full table.
      sql({ dialect: PostgreSQL }),
      autocompletion({
        override: [sqlKeywords],
        activateOnTyping: true,
        icons: false,
        maxRenderedOptions: 12,
      }),
      editorTheme,
      sqlHighlight,
      Prec.highest(
        keymap.of([
          {
            key: "Mod-Enter",
            run: () => {
              runCurrent();
              return true;
            },
          },
        ]),
      ),
      keymap.of([...completionKeymap, ...defaultKeymap, ...historyKeymap, indentWithTab]),
      EditorView.updateListener.of((update) => {
        if (update.docChanged) {
          updateSql(activeIdRef.current, update.state.doc.toString());
        }
      }),
        ];
    const makeState = (doc: string) => EditorState.create({ doc, extensions });
    const initial = useApp.getState().tabs.find((tab) => tab.id === activeIdRef.current);
    const editor = new EditorView({
      parent: host.current,
      state: makeState(initial?.sql ?? ""),
    });
    states.current.set(activeIdRef.current, editor.state);
    shownIdRef.current = activeIdRef.current;
    makeStateRef.current = makeState;
    view.current = editor;
    return () => {
      editor.destroy();
      view.current = null;
    };
    // Mount once; tab changes are applied by the effect below.
  }, []);

  // Switching tabs swaps the whole editor state: the leaving tab's state is
  // parked (undo history, selection and all) and the entering tab's restored
  // or created. A store change to the shown tab's SQL from outside (the
  // explorer's template) is applied as an edit; the listener above echoes the
  // same text back into the store, which is a no-op.
  useEffect(() => {
    const editor = view.current;
    if (!editor) {
      return;
    }
    if (shownIdRef.current !== active.id) {
      if (shownIdRef.current !== null) {
        states.current.set(shownIdRef.current, editor.state);
      }
      const live = useApp.getState().tabs.map((tab) => tab.id);
      for (const id of states.current.keys()) {
        if (!live.includes(id)) {
          states.current.delete(id);
        }
      }
      editor.setState(states.current.get(active.id) ?? makeStateRef.current(active.sql));
      shownIdRef.current = active.id;
    }
    const current = editor.state.doc.toString();
    if (current !== active.sql) {
      editor.dispatch({
        changes: { from: 0, to: current.length, insert: active.sql },
        selection: { anchor: active.sql.length },
      });
    }
  }, [active.id, active.sql]);

  return (
    <section className="flex h-full min-h-0 flex-col">
      {/* Tab strip: the open statements, newest last. */}
      <div className="flex h-[30px] shrink-0 items-stretch overflow-x-auto border-b border-line-strong bg-page [scrollbar-width:thin]">
        {tabs.map((tab) => {
          const on = tab.id === activeTabId;
          return (
            <div
              key={tab.id}
              className={`group relative flex shrink-0 items-center gap-1.5 border-r border-line pr-1.5 pl-3 font-mono text-[11.5px] ${
                on ? "bg-panel font-bold text-fg" : "text-fg-faint hover:text-fg"
              }`}
            >
              <button type="button" className="whitespace-nowrap" onClick={() => setActiveTab(tab.id)}>
                {tab.name}
              </button>
              <button
                type="button"
                aria-label={`Close ${tab.name}`}
                disabled={tabs.length === 1}
                onClick={() => closeTab(tab.id)}
                className="flex h-5 w-5 items-center justify-center text-fg-faint opacity-0 group-hover:opacity-100 hover:bg-hover hover:text-fg focus-visible:opacity-100 disabled:hidden"
              >
                <X size={12} />
              </button>
              {on ? <span className="absolute inset-x-0 top-0 h-0.5 bg-accent" /> : null}
            </div>
          );
        })}
        <button
          type="button"
          aria-label="New query tab"
          className="flex w-8 shrink-0 items-center justify-center text-fg-faint hover:bg-hover hover:text-fg"
          onClick={addTab}
        >
          <Plus size={13} />
        </button>
      </div>

      {/* Toolbar: where this tab's names resolve, and the Run button. */}
      <div className="flex h-[38px] shrink-0 items-center gap-1.5 border-b border-line px-2.5">
        <ScopePicker tab={active} />
        <div className="flex-1" />
        <button
          type="button"
          onClick={runCurrent}
          disabled={running}
          className="flex h-7 items-center gap-2 border border-line-strong bg-accent px-3.5 font-mono text-[11.5px] font-extrabold tracking-[.12em] text-accent-contrast shadow-press transition-[transform,box-shadow] hover:bg-accent-deep active:translate-x-px active:translate-y-px active:shadow-none disabled:opacity-60 disabled:shadow-press"
        >
          {running ? <LoaderCircle size={12} className="animate-spin" /> : <Play size={12} fill="currentColor" />}
          {running ? "RUNNING" : "RUN"}
          <kbd className="font-mono text-[11px] font-medium tracking-normal opacity-60">⌘↵</kbd>
        </button>
      </div>

      <div ref={host} className="min-h-0 flex-1 overflow-hidden" />
    </section>
  );
}
