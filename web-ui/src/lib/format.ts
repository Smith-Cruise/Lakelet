/** Convert an Arrow cell value to a display string. */
export function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof Uint8Array) {
    return Array.from(value, (b) => b.toString(16).padStart(2, "0")).join("");
  }
  if (typeof value === "object") {
    return JSON.stringify(value, (_key, v) => (typeof v === "bigint" ? v.toString() : v));
  }
  return String(value);
}
