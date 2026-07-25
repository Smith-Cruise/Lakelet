export const DEFAULT_ROW_LIMIT = 1000;

export function normalizeSql(sql: string): string {
  return sql.trim().replace(/;+\s*$/, "");
}

/**
 * A trailing `-- ...` comment, matched only when nothing between the start of
 * its line and the `--` is quoted. That keeps `where s = 'a--b'` intact at the
 * cost of missing comments on lines that also contain a string literal, which
 * only makes the guard more conservative.
 */
const TRAILING_LINE_COMMENT = /(^|\n)([^\n'"]*?)--[^\n]*$/;

/** The statement with a trailing line comment dropped, for inspection only. */
function withoutTrailingComment(sql: string): string {
  return sql.replace(TRAILING_LINE_COMMENT, "$1$2").trimEnd();
}

/**
 * Append a LIMIT to bare SELECT/WITH/VALUES statements that don't already
 * bound their row count. Deliberately conservative: no subquery wrapping, so
 * a statement that already ends in LIMIT and/or OFFSET is left untouched.
 */
export function applyLimitGuard(sql: string, limit: number = DEFAULT_ROW_LIMIT): string {
  const normalized = normalizeSql(sql);
  if (!/^(select|with|values)\b/i.test(normalized)) {
    return normalized;
  }
  const bounded = /\b(limit\s+\d+(\s+offset\s+\d+)?|offset\s+\d+(\s+rows?)?)\s*$/i;
  if (bounded.test(withoutTrailingComment(normalized))) {
    return normalized;
  }
  // On its own line: appending after a trailing `-- ...` comment would
  // otherwise bury the LIMIT inside it and run the query unbounded.
  return `${normalized}\nLIMIT ${limit}`;
}
