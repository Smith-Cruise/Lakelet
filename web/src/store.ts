import { create } from "zustand";
import type { ResultSet } from "./lib/result";

/** One editor tab. Each carries the catalog and schema its statements resolve against. */
export interface EditorTab {
  id: string;
  name: string;
  sql: string;
  catalog: string;
  schema?: string;
}

export interface TableRef {
  catalog: string;
  schema: string;
  table: string;
}

interface RunState {
  status: "idle" | "running" | "done" | "error";
  result?: ResultSet;
  error?: string;
  streamedRows: number;
}

interface AppState {
  /** The table last picked in the explorer; drives its highlight. */
  activeTable?: TableRef;
  setActiveTable: (table: TableRef) => void;

  tabs: EditorTab[];
  activeTabId: string;
  setActiveTab: (id: string) => void;
  addTab: () => void;
  closeTab: (id: string) => void;
  updateSql: (id: string, sql: string) => void;
  setTabScope: (id: string, catalog: string, schema?: string) => void;

  run: RunState;
  setRun: (run: RunState) => void;
}

const TABS_KEY = "lakelet.tabs";
/** Lakebed has a single theme; drop the key earlier versions wrote. */
const RETIRED_THEME_KEY = "lakelet.theme";

/** Mirrors the server's own defaults, so an unqualified name resolves the
 * same way from a fresh tab as from a client that sends no scope at all. */
const DEFAULT_CATALOG = "internal";
const DEFAULT_SCHEMA = "information_schema";

function readStorage(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    // Private mode and blocked storage both land here; defaults are fine.
    return null;
  }
}

function writeStorage(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // Not being able to remember a choice should not break making it.
  }
}

try {
  localStorage.removeItem(RETIRED_THEME_KEY);
} catch {
  // Nothing to clean up when storage is unavailable.
}

/** Tabs are named after the moment they were opened, e.g. `2026/9/19 14:12`. */
function tabName(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${now.getFullYear()}/${now.getMonth() + 1}/${now.getDate()} ${pad(now.getHours())}:${pad(now.getMinutes())}`;
}

function newId(): string {
  return typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `tab-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function newTab(): EditorTab {
  return { id: newId(), name: tabName(), sql: "", catalog: DEFAULT_CATALOG, schema: DEFAULT_SCHEMA };
}

interface StoredTabs {
  v: 1;
  tabs: EditorTab[];
  activeTabId: string;
}

function isTab(value: unknown): value is EditorTab {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const tab = value as Record<string, unknown>;
  return (
    typeof tab.id === "string" &&
    typeof tab.name === "string" &&
    typeof tab.sql === "string" &&
    typeof tab.catalog === "string" &&
    (tab.schema === undefined || typeof tab.schema === "string")
  );
}

/** Restores the tabs from the last visit; anything malformed falls back to one fresh tab. */
function storedTabs(): Pick<AppState, "tabs" | "activeTabId"> {
  const fresh = () => {
    const tab = newTab();
    return { tabs: [tab], activeTabId: tab.id };
  };
  const raw = readStorage(TABS_KEY);
  if (!raw) {
    return fresh();
  }
  try {
    const parsed = JSON.parse(raw) as Partial<StoredTabs>;
    if (parsed.v !== 1 || !Array.isArray(parsed.tabs)) {
      return fresh();
    }
    const tabs = parsed.tabs.filter(isTab);
    if (tabs.length === 0) {
      return fresh();
    }
    const activeTabId = tabs.some((tab) => tab.id === parsed.activeTabId)
      ? (parsed.activeTabId as string)
      : tabs[0].id;
    return { tabs, activeTabId };
  } catch {
    return fresh();
  }
}

export const useApp = create<AppState>((set, get) => ({
  setActiveTable: (activeTable) => set({ activeTable }),

  ...storedTabs(),
  setActiveTab: (id) => set({ activeTabId: id }),
  addTab: () => {
    const tab = newTab();
    set((state) => ({ tabs: [...state.tabs, tab], activeTabId: tab.id }));
  },
  closeTab: (id) => {
    const { tabs, activeTabId } = get();
    if (tabs.length === 1) {
      return;
    }
    const remaining = tabs.filter((tab) => tab.id !== id);
    set({
      tabs: remaining,
      activeTabId: activeTabId === id ? remaining[remaining.length - 1].id : activeTabId,
    });
  },
  updateSql: (id, sql) =>
    set((state) => ({
      tabs: state.tabs.map((tab) => (tab.id === id ? { ...tab, sql } : tab)),
    })),
  setTabScope: (id, catalog, schema) =>
    set((state) => ({
      tabs: state.tabs.map((tab) => (tab.id === id ? { ...tab, catalog, schema } : tab)),
    })),

  run: { status: "idle", streamedRows: 0 },
  setRun: (run) => set({ run }),
}));

// Every change to the tabs reaches storage, so a reload — or the next visit —
// opens exactly what was there. Typing changes the store per keystroke, so
// writes are coalesced and flushed when the page goes away. Results are not kept.
const PERSIST_DELAY_MS = 300;
let persistTimer: ReturnType<typeof setTimeout> | undefined;

function persistTabs(): void {
  persistTimer = undefined;
  const { tabs, activeTabId } = useApp.getState();
  const stored: StoredTabs = { v: 1, tabs, activeTabId };
  writeStorage(TABS_KEY, JSON.stringify(stored));
}

useApp.subscribe((state, previous) => {
  if (state.tabs !== previous.tabs || state.activeTabId !== previous.activeTabId) {
    clearTimeout(persistTimer);
    persistTimer = setTimeout(persistTabs, PERSIST_DELAY_MS);
  }
});

window.addEventListener("pagehide", () => {
  if (persistTimer !== undefined) {
    clearTimeout(persistTimer);
    persistTabs();
  }
});
