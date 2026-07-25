export interface HistoryEntry {
  sql: string;
  at: number;
  durationMs: number;
  rows: number;
  status: "ok" | "error" | "cancelled";
}

const HISTORY_KEY = "lakelet.web-ui.history.v1";
const MAX_ENTRIES = 200;

export function loadHistory(): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function pushHistory(entry: HistoryEntry): HistoryEntry[] {
  const entries = [entry, ...loadHistory()].slice(0, MAX_ENTRIES);
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(entries));
  } catch {
    // Best effort: a full or unavailable localStorage never breaks queries.
  }
  return entries;
}
