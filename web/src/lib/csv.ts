/** CSV export of the rows currently in the browser. */

import type { ResultSet } from "./result";

/** Quotes a field only when it needs it: a comma, a quote or a line break. */
function csvField(value: string): string {
  return /[",\r\n]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

/**
 * Header row plus one line per row, CRLF-terminated as RFC 4180 has it. Null
 * cells come through as empty fields because the rows already format them so.
 */
export function toCsv(result: ResultSet): string {
  const names = result.columns.map((column) => column.name);
  const lines = [names.map(csvField).join(",")];
  for (const row of result.rows) {
    lines.push(names.map((name) => csvField(row[name] ?? "")).join(","));
  }
  return `${lines.join("\r\n")}\r\n`;
}

/** Saves the result as `lakelet-result-<timestamp>.csv` through the browser. */
export function downloadCsv(result: ResultSet): void {
  // The byte-order mark makes Excel read the file as UTF-8 instead of guessing.
  const blob = new Blob([`﻿${toCsv(result)}`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `lakelet-result-${stamp}.csv`;
  anchor.click();
  // Some browsers start the download after click() returns; revoking at once
  // would hand them a dead URL.
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}
