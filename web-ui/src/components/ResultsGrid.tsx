import { useCallback, useMemo, useState } from "react";
import {
  DataEditor,
  GridCellKind,
  type GridCell,
  type GridColumn,
  type Item,
  type Theme,
} from "@glideapps/glide-data-grid";
import { AlertCircle, Terminal } from "lucide-react";
import { useQuery } from "@/stores/query";
import { useTheme } from "@/stores/theme";
import { formatValue } from "@/lib/format";

const MONO_FONT = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";

const LIGHT_THEME: Partial<Theme> = {
  accentColor: "#3b82f6",
  accentLight: "rgba(59, 130, 246, 0.12)",
  textDark: "#18181b",
  textMedium: "#52525b",
  textLight: "#a1a1aa",
  bgCell: "#ffffff",
  bgCellMedium: "#fafafa",
  bgHeader: "#fafafa",
  bgHeaderHasFocus: "#f4f4f5",
  bgHeaderHovered: "#f4f4f5",
  textHeader: "#52525b",
  borderColor: "#e4e4e7",
  headerFontStyle: "600 12px",
  baseFontStyle: "12.5px",
  fontFamily: MONO_FONT,
};

const DARK_THEME: Partial<Theme> = {
  accentColor: "#60a5fa",
  accentLight: "rgba(96, 165, 250, 0.15)",
  textDark: "#fafafa",
  textMedium: "#a1a1aa",
  textLight: "#71717a",
  bgCell: "#09090b",
  bgCellMedium: "#18181b",
  bgHeader: "#18181b",
  bgHeaderHasFocus: "#27272a",
  bgHeaderHovered: "#27272a",
  textHeader: "#a1a1aa",
  borderColor: "#27272a",
  headerFontStyle: "600 12px",
  baseFontStyle: "12.5px",
  fontFamily: MONO_FONT,
};

export function ResultsGrid() {
  const schema = useQuery((state) => state.schema);
  const batches = useQuery((state) => state.batches);
  const rowCount = useQuery((state) => state.rowCount);
  const error = useQuery((state) => state.error);
  const running = useQuery((state) => state.running);
  const dark = useTheme((state) => state.dark);

  const [widthOverrides, setWidthOverrides] = useState<Record<string, number>>({});

  const firstBatch = batches.length > 0 ? batches[0] : null;

  // Fit each column to its header plus a sample of the first batch's rows.
  const fittedWidths = useMemo(() => {
    if (!schema) {
      return {};
    }
    const context = document.createElement("canvas").getContext("2d")!;
    const widths: Record<string, number> = {};
    const sampleRows = Math.min(firstBatch?.numRows ?? 0, 100);
    schema.fields.forEach((field, index) => {
      context.font = `600 12px ${MONO_FONT}`;
      let width = context.measureText(`${field.name} · ${String(field.type)}`).width;
      context.font = `12.5px ${MONO_FONT}`;
      const column = firstBatch?.getChildAt(index);
      for (let row = 0; row < sampleRows; row++) {
        width = Math.max(width, context.measureText(formatValue(column?.get(row))).width);
      }
      widths[field.name] = Math.ceil(Math.min(Math.max(width + 24, 80), 480));
    });
    return widths;
  }, [schema, firstBatch]);

  const columns = useMemo<GridColumn[]>(() => {
    if (!schema) {
      return [];
    }
    return schema.fields.map((field) => {
      const override = widthOverrides[field.name];
      return {
        id: field.name,
        title: `${field.name} · ${String(field.type).toUpperCase()}`,
        width: override ?? fittedWidths[field.name] ?? 160,
        // Fill the full grid width; a manually resized column keeps its size.
        grow: override === undefined ? 1 : 0,
      };
    });
  }, [schema, widthOverrides, fittedWidths]);

  // Cumulative row offsets per batch, so a global row index maps to
  // (batch, local row) with a binary search.
  const offsets = useMemo(() => {
    const result: number[] = new Array(batches.length);
    let total = 0;
    for (let i = 0; i < batches.length; i++) {
      result[i] = total;
      total += batches[i].numRows;
    }
    return result;
  }, [batches]);

  const getCellContent = useCallback(
    ([col, row]: Item): GridCell => {
      let lo = 0;
      let hi = offsets.length - 1;
      while (lo < hi) {
        const mid = (lo + hi + 1) >> 1;
        if (offsets[mid] <= row) {
          lo = mid;
        } else {
          hi = mid - 1;
        }
      }
      const batch = batches[lo];
      const value: unknown = batch?.getChildAt(col)?.get(row - offsets[lo]);
      if (value === null || value === undefined) {
        return {
          kind: GridCellKind.Text,
          data: "",
          displayData: "NULL",
          allowOverlay: false,
          themeOverride: { textDark: dark ? "#52525b" : "#a1a1aa" },
        };
      }
      const display = formatValue(value);
      return {
        kind: GridCellKind.Text,
        data: display,
        displayData: display,
        allowOverlay: true,
        readonly: true,
      };
    },
    [batches, offsets, dark],
  );

  const onColumnResize = useCallback((column: GridColumn, newSize: number) => {
    if (column.id) {
      setWidthOverrides((prev) => ({ ...prev, [column.id!]: newSize }));
    }
  }, []);

  if (error) {
    return (
      <div className="flex h-full items-start gap-2 overflow-auto p-4 text-sm text-destructive">
        <AlertCircle className="mt-0.5 size-4 shrink-0" />
        <pre className="whitespace-pre-wrap font-mono text-xs">{error}</pre>
      </div>
    );
  }

  if (!schema) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-2 text-muted-foreground">
        <Terminal className="size-6" />
        <p className="text-sm">{running ? "Running…" : "Run a query to see results"}</p>
        <p className="text-xs">
          Press <kbd className="rounded border bg-muted px-1 py-0.5 font-mono">⌘⏎</kbd> to run
        </p>
      </div>
    );
  }

  return (
    <DataEditor
      columns={columns}
      rows={rowCount}
      getCellContent={getCellContent}
      onColumnResize={onColumnResize}
      getCellsForSelection={true}
      rowMarkers="number"
      smoothScrollX
      smoothScrollY
      theme={dark ? DARK_THEME : LIGHT_THEME}
      width="100%"
      height="100%"
    />
  );
}
