/** Turns decoded Arrow batches into the rows the UI renders. */

import type { RecordBatch } from "apache-arrow";
import { describeColumn, formatValue, type ColumnMeta } from "./values";

/** Formatted cell text, or null so the grid can tell a NULL from an empty string. */
export type Row = Record<string, string | null>;

/**
 * A result column, identified by its position rather than its name. SQL
 * happily returns two columns with the same name — `select * from a join b`,
 * or `select 1 as x, 2 as x` — and keying rows by name silently drops one of
 * them and shows the other twice.
 */
export interface ResultColumn extends ColumnMeta {
  /** Unique within one result: the row key and the grid's field. */
  key: string;
}

export interface ResultSet {
  columns: ResultColumn[];
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
  const columns: ResultColumn[] = first
    ? first.schema.fields.map((field, position) => ({
        ...describeColumn(field),
        key: `c${position}`,
      }))
    : [];

  const rows: Row[] = [];
  outer: for (const batch of batches) {
    for (let index = 0; index < batch.numRows; index += 1) {
      if (rows.length >= limit) {
        break outer;
      }
      const row: Row = {};
      // By position: `getChild(name)` resolves to the first match, so a
      // duplicate name would read the same vector twice.
      columns.forEach((column, position) => {
        const value = batch.getChildAt(position)?.get(index) ?? null;
        row[column.key] = value === null ? null : formatValue(value, column);
      });
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
