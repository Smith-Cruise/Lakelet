import { create } from "zustand";
import type { RecordBatch, Schema } from "apache-arrow";
import { QueryError, runQuery } from "@/lib/api";
import { applyLimitGuard, normalizeSql } from "@/lib/limit";
import { loadHistory, pushHistory, type HistoryEntry } from "@/lib/history";

interface QueryState {
  running: boolean;
  schema: Schema | null;
  batches: RecordBatch[];
  rowCount: number;
  error: string | null;
  cancelled: boolean;
  startedAt: number | null;
  finishedAt: number | null;
  limitGuard: boolean;
  history: HistoryEntry[];
  run: (sql: string) => Promise<void>;
  stop: () => void;
  toggleLimitGuard: () => void;
}

let abortController: AbortController | null = null;

export const useQuery = create<QueryState>((set, get) => ({
  running: false,
  schema: null,
  batches: [],
  rowCount: 0,
  error: null,
  cancelled: false,
  startedAt: null,
  finishedAt: null,
  limitGuard: true,
  history: loadHistory(),

  run: async (sql: string) => {
    const statement = get().limitGuard ? applyLimitGuard(sql) : normalizeSql(sql);
    if (!statement) {
      return;
    }
    abortController?.abort();
    const controller = new AbortController();
    abortController = controller;

    const startedAt = Date.now();
    set({
      running: true,
      schema: null,
      batches: [],
      rowCount: 0,
      error: null,
      cancelled: false,
      startedAt,
      finishedAt: null,
    });

    // A run superseded by a newer one must not write to the shared state:
    // its rejection and in-flight batches arrive after the new run has
    // already reset everything.
    const owns = () => abortController === controller;

    let status: HistoryEntry["status"] = "ok";
    try {
      await runQuery(
        statement,
        {},
        controller.signal,
        (schema) => owns() && set({ schema }),
        (batch) =>
          owns() &&
          set((state) => ({
            batches: [...state.batches, batch],
            rowCount: state.rowCount + batch.numRows,
          })),
      );
    } catch (err) {
      if (!owns()) {
        return;
      }
      if (controller.signal.aborted) {
        status = "cancelled";
        set({ cancelled: true });
      } else {
        status = "error";
        const message =
          err instanceof QueryError
            ? `${err.message} (${err.code})`
            : err instanceof Error
              ? err.message
              : String(err);
        set({ error: message });
      }
    } finally {
      // A newer run may already own the state; only the owner finalizes it.
      if (owns()) {
        const finishedAt = Date.now();
        const history = pushHistory({
          sql: statement,
          at: startedAt,
          durationMs: finishedAt - startedAt,
          rows: get().rowCount,
          status,
        });
        set({ running: false, finishedAt, history });
        abortController = null;
      }
    }
  },

  stop: () => {
    abortController?.abort();
  },

  toggleLimitGuard: () => set((state) => ({ limitGuard: !state.limitGuard })),
}));
