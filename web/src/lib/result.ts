/** Turns decoded Arrow batches into the rows the UI renders. */

import type { RecordBatch } from "apache-arrow";
import { describeColumn, formatValue, type ColumnMeta } from "./values";

/** Formatted cell text, or null so the grid can tell a NULL from an empty string. */
export type Row = Record<string, string | null>;

export interface ResultSet {
  columns: ColumnMeta[];
  rows: Row[];
  rowCount: number;
  elapsedMs: number;
  batchCount: number;
  truncated: boolean;
}

export function buildResultSet(
  batches: RecordBatch[],
  limit: number,
  elapsedMs: number,
  truncated: boolean,
): ResultSet {
  const first = batches[0];
  const columns = first ? first.schema.fields.map(describeColumn) : [];

  const rows: Row[] = [];
  outer: for (const batch of batches) {
    for (let index = 0; index < batch.numRows; index += 1) {
      if (rows.length >= limit) {
        break outer;
      }
      const row: Row = {};
      for (const column of columns) {
        const value = batch.getChild(column.name)?.get(index) ?? null;
        row[column.name] = value === null ? null : formatValue(value, column);
      }
      rows.push(row);
    }
  }

  return {
    columns,
    rows,
    rowCount: rows.length,
    elapsedMs,
    batchCount: batches.length,
    truncated,
  };
}
