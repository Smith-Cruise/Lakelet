export const DEFAULT_ROW_LIMIT = 1000;

export function normalizeSql(sql: string): string {
  return sql.trim().replace(/;+\s*$/, "");
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
  if (bounded.test(normalized)) {
    return normalized;
  }
  return `${normalized} LIMIT ${limit}`;
}
