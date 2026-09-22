import { useMemo } from "react";
import { AgGridReact } from "ag-grid-react";
import {
  CellStyleModule,
  ClientSideRowModelModule,
  ModuleRegistry,
  ValidationModule,
  themeQuartz,
  type CellClassParams,
  type ColDef,
  type ValueFormatterParams,
} from "ag-grid-community";
import { ColumnHeader } from "./ColumnHeader";
import type { ResultSet, Row } from "../lib/result";
import type { ColumnMeta } from "../lib/values";

// Only the features the grid actually uses, instead of AllCommunityModule:
// filters, editors, export, pagination and the rest stay out of the bundle.
// Sorting, column resizing and pinning live in the core every module pulls in.
// Validation explains a missing module or a bad option in the console; it
// is only worth its weight while developing.
ModuleRegistry.registerModules([
  ClientSideRowModelModule,
  CellStyleModule,
  ...(import.meta.env.DEV ? [ValidationModule] : []),
]);

/**
 * The grid reads the same tokens as the rest of the app. The look is a printed
 * data listing: every cell monospaced so digits line up down a column, the
 * header sitting a shade back from the rows over one full-strength rule, and
 * the finest possible lines between cells so the numbers carry the grid rather
 * than the borders.
 */
const gridTheme = themeQuartz.withParams({
  backgroundColor: "var(--lk-panel-alt)",
  foregroundColor: "var(--lk-fg)",
  borderColor: "var(--lk-border-soft)",
  headerBackgroundColor: "var(--lk-page)",
  headerTextColor: "var(--lk-fg-muted)",
  headerFontWeight: 700,
  headerRowBorder: { style: "solid", width: 1, color: "var(--lk-border-strong)" },
  headerColumnBorder: { style: "solid", width: 1, color: "var(--lk-border)" },
  headerColumnResizeHandleColor: "transparent",
  columnBorder: { style: "solid", width: 1, color: "var(--lk-border-soft)" },
  rowBorder: { style: "solid", width: 1, color: "var(--lk-border-soft)" },
  oddRowBackgroundColor: "var(--lk-panel-alt)",
  rowHoverColor: "var(--lk-hover)",
  selectedRowBackgroundColor: "var(--lk-accent-bg)",
  fontFamily: "var(--lk-mono)",
  fontSize: "12px",
  headerFontFamily: "var(--lk-mono)",
  headerFontSize: "10.5px",
  cellHorizontalPadding: "11px",
  rowHeight: "27px",
  headerHeight: "30px",
  wrapperBorderRadius: "0px",
  wrapperBorder: false,
  borderRadius: "0px",
});

const SAMPLE_ROWS = 200;
const MIN_WIDTH = 160;
const MAX_WIDTH = 380;
/** What a NULL cell shows; an empty string stays empty, so the two never look alike. */
const NULL_TEXT = "null";

/**
 * Estimates a column width from what it actually holds, instead of the grid's
 * flat 200px default. Deterministic and computed once per result, so there is
 * no measure-and-relayout flicker.
 */
function estimateWidth(meta: ColumnMeta, rows: Row[]): number {
  let longest = meta.name.length;
  for (const row of rows.slice(0, SAMPLE_ROWS)) {
    const length = row[meta.name]?.length ?? NULL_TEXT.length;
    if (length > longest) {
      longest = length;
    }
  }
  const perChar = meta.kind === "numeric" ? 8 : 7.2;
  return Math.round(Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, longest * perChar + 28)));
}

/**
 * Orders numeric cells by value even though they are stored formatted. Whole
 * numbers compare as bigints so 64-bit extremes keep their order; anything
 * else (decimals, floats) goes through Number. Nulls sort last either way:
 * the grid hands them to a custom comparator rather than placing them itself.
 */
function compareNumeric(
  a: string | null,
  b: string | null,
  _nodeA: unknown,
  _nodeB: unknown,
  descending: boolean,
): number {
  if (a === null || b === null) {
    if (a === b) {
      return 0;
    }
    const nullLast = a === null ? 1 : -1;
    return descending ? -nullLast : nullLast;
  }
  if (/^-?\d+$/.test(a) && /^-?\d+$/.test(b)) {
    const x = BigInt(a);
    const y = BigInt(b);
    return x < y ? -1 : x > y ? 1 : 0;
  }
  // NaN sorts after every number so the comparator stays consistent.
  const x = Number(a);
  const y = Number(b);
  if (Number.isNaN(x) || Number.isNaN(y)) {
    return Number.isNaN(x) ? (Number.isNaN(y) ? 0 : 1) : -1;
  }
  return x < y ? -1 : x > y ? 1 : 0;
}

export function TableView({ result }: { result: ResultSet }) {
  const columns = useMemo<ColDef[]>(
    () => [
      // A plain pinned column rather than the grid's own row numbers, which
      // the community build does not render. It shows the row's position in
      // the result, not on screen, so sorting never renumbers it and the
      // number matches the CSV line.
      {
        colId: "__row",
        headerName: "",
        pinned: "left",
        width: 52,
        sortable: false,
        resizable: false,
        suppressMovable: true,
        cellClass: "text-right text-[11px] text-fg-faint",
        valueGetter: (params) => (params.node?.sourceRowIndex ?? 0) + 1,
      },
      ...result.columns.map((meta) => ({
        field: meta.name,
        headerName: meta.name,
        width: estimateWidth(meta, result.rows),
        minWidth: MIN_WIDTH,
        headerComponent: ColumnHeader,
        headerComponentParams: { meta },
        valueFormatter: (params: ValueFormatterParams<Row, string | null>) =>
          params.value ?? NULL_TEXT,
        cellClass: meta.kind === "numeric" ? "text-right tabular-nums" : undefined,
        comparator: meta.kind === "numeric" ? compareNumeric : undefined,
        cellClassRules: { "lk-null": (params: CellClassParams<Row>) => params.value === null },
      })),
    ],
    [result],
  );

  return (
    <div className="h-full w-full">
      <AgGridReact
        theme={gridTheme}
        rowData={result.rows}
        columnDefs={columns}
        defaultColDef={{ resizable: true, sortable: true }}
        suppressCellFocus
      />
    </div>
  );
}
