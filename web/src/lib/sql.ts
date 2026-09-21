/**
 * Splits editor text into statements so Run can execute exactly one of them.
 *
 * Offsets index into the original text. `end` is the position of the closing
 * semicolon (or the text length for an unterminated last statement), so
 * `text.slice(start, end)` is the statement without its terminator.
 */
export interface StatementRange {
  start: number;
  end: number;
}

/**
 * Finds every statement, splitting on semicolons that sit outside string
 * literals, quoted identifiers and comments. Whitespace-only segments are
 * dropped, so `;;` and a trailing semicolon produce no empty statements.
 */
export function splitStatements(text: string): StatementRange[] {
  const ranges: StatementRange[] = [];
  let segmentStart = 0;
  let i = 0;
  const n = text.length;

  const push = (end: number) => {
    const range = trimRange(text, segmentStart, end);
    if (range) {
      ranges.push(range);
    }
  };

  while (i < n) {
    const ch = text[i];
    const next = text[i + 1];
    if (ch === "'" || ch === '"' || ch === "`") {
      // A doubled quote inside the literal is an escaped quote, not the end.
      i += 1;
      while (i < n) {
        if (text[i] === ch) {
          if (text[i + 1] === ch) {
            i += 2;
            continue;
          }
          break;
        }
        i += 1;
      }
      i += 1;
    } else if (ch === "-" && next === "-") {
      const eol = text.indexOf("\n", i);
      i = eol === -1 ? n : eol + 1;
    } else if (ch === "/" && next === "*") {
      const close = text.indexOf("*/", i + 2);
      i = close === -1 ? n : close + 2;
    } else if (ch === ";") {
      push(i);
      i += 1;
      segmentStart = i;
    } else {
      i += 1;
    }
  }
  push(n);
  return ranges;
}

/**
 * The statement Run should execute for a caret at `offset`.
 *
 * Inside a statement, that statement. In the gap after a semicolon, the
 * statement just finished while the caret is still on its line; once the
 * caret moves to a later line the next statement is the one meant. After the
 * last statement, the last statement.
 */
export function statementAt(text: string, offset: number): StatementRange | undefined {
  const ranges = splitStatements(text);
  for (let k = 0; k < ranges.length; k += 1) {
    const range = ranges[k];
    if (offset < range.start) {
      const previous = ranges[k - 1];
      if (previous && !text.slice(previous.end, offset).includes("\n")) {
        return previous;
      }
      return range;
    }
    if (offset <= range.end) {
      return range;
    }
  }
  return ranges[ranges.length - 1];
}

function trimRange(text: string, start: number, end: number): StatementRange | undefined {
  while (start < end && /\s/.test(text[start])) {
    start += 1;
  }
  while (end > start && /\s/.test(text[end - 1])) {
    end -= 1;
  }
  return start < end ? { start, end } : undefined;
}

/** Double quotes an identifier unless it is already a plain lowercase name. */
export function quoteIdent(name: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`;
}

/** A dotted path - catalog.schema.table - with each part quoted as needed. */
export function qualifiedName(...parts: string[]): string {
  return parts.map(quoteIdent).join(".");
}
