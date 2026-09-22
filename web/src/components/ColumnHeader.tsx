import { useEffect, useState } from "react";
import type { IHeaderParams } from "ag-grid-community";
import { ChevronDown, ChevronUp, ChevronsUpDown } from "lucide-react";
import { Tip } from "./Tip";
import type { ColumnMeta } from "../lib/values";

export interface HeaderContext {
  meta: ColumnMeta;
}

type Sort = "asc" | "desc" | null | undefined;

/** What one more click does, following the grid's none → asc → desc cycle. */
const NEXT_SORT: Record<"asc" | "desc" | "none", string> = {
  none: "Sort ascending",
  asc: "Sort descending",
  desc: "Clear sort",
};

/**
 * Just the column name, aligned like its cells, with a small sort chevron
 * that appears on hover and stays while the column is sorted. The type shows
 * on hover instead of taking up space.
 *
 * A custom header component takes over the whole cell, including sorting, so
 * the click-to-sort that the default header provides has to be wired here.
 */
export function ColumnHeader(params: IHeaderParams & HeaderContext) {
  const { meta, column, enableSorting, progressSort } = params;
  const [sort, setSort] = useState<Sort>(() => column.getSort());

  useEffect(() => {
    const onSortChanged = () => setSort(column.getSort());
    column.addEventListener("sortChanged", onSortChanged);
    return () => column.removeEventListener("sortChanged", onSortChanged);
  }, [column]);

  const numeric = meta.kind === "numeric";
  const sorted = sort === "asc" || sort === "desc";
  const next = NEXT_SORT[sort ?? "none"];

  const name = (
    <Tip label={meta.typeLabel}>
      <span className="min-w-0 truncate text-[10.5px] font-bold tracking-[.12em] text-fg-muted uppercase" title={meta.name}>
        {meta.name}
      </span>
    </Tip>
  );
  const chevron = enableSorting ? (
    <Tip label={next}>
      <button
        type="button"
        aria-label={next}
        onClick={(event) => progressSort(event.shiftKey)}
        className={`flex h-6 w-6 shrink-0 items-center justify-center text-fg-faint hover:text-fg ${
          sorted ? "opacity-100" : "opacity-0 group-hover:opacity-100 focus-visible:opacity-100"
        }`}
      >
        {sort === "asc" ? (
          <ChevronUp size={12} />
        ) : sort === "desc" ? (
          <ChevronDown size={12} />
        ) : (
          <ChevronsUpDown size={12} />
        )}
      </button>
    </Tip>
  ) : null;

  // Numbers sit on the right like their cells, so the chevron goes on the
  // outer side in both cases rather than between the name and the edge.
  return (
    <div className={`group flex h-full w-full items-center gap-1 ${numeric ? "justify-end" : ""}`}>
      {numeric ? chevron : null}
      {name}
      {numeric ? null : chevron}
    </div>
  );
}
