import type { RecordBatch, Schema } from "apache-arrow";
import { formatValue } from "./format";

function escapeCsv(value: string): string {
  return /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

export function exportCsv(schema: Schema, batches: RecordBatch[]): void {
  const lines: string[] = [];
  lines.push(schema.fields.map((f) => escapeCsv(f.name)).join(","));
  for (const batch of batches) {
    for (let row = 0; row < batch.numRows; row++) {
      const cells: string[] = [];
      for (let col = 0; col < batch.numCols; col++) {
        cells.push(escapeCsv(formatValue(batch.getChildAt(col)?.get(row))));
      }
      lines.push(cells.join(","));
    }
  }
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `lakelet-result-${new Date().toISOString().replace(/[:.]/g, "-")}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}
