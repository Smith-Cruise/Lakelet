import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ChevronDown,
  ChevronRight,
  Database,
  Folder,
  RectangleVertical,
  RefreshCw,
  Search,
  Table2,
} from "lucide-react";
import { describeError, describeTable, listSchemas, listTables } from "../flight/client";
import { useApp } from "../store";

interface Props {
  catalogs: string[];
  catalogsError?: string;
  loadingCatalogs: boolean;
  onRefresh: () => void;
  onPickTable: (catalog: string, schema: string, table: string) => void;
}

/** Open-state key for a tree node; NUL cannot appear in a name, unlike a dot. */
function nodeKey(...parts: string[]): string {
  return parts.join("\u0000");
}

const ROW =
  "flex h-[26px] w-full items-center gap-1.5 px-2.5 text-left font-mono text-[12.5px] whitespace-nowrap hover:bg-hover";

export function CatalogTree({
  catalogs,
  catalogsError,
  loadingCatalogs,
  onRefresh,
  onPickTable,
}: Props) {
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState<Record<string, boolean>>({});

  const toggle = (key: string) => setOpen((state) => ({ ...state, [key]: !state[key] }));
  const matches = (name: string) =>
    filter.trim() === "" || name.toLowerCase().includes(filter.trim().toLowerCase());

  return (
    <aside className="flex h-full w-full flex-col bg-panel">
      <div className="flex h-[30px] shrink-0 items-center border-b border-line px-2.5">
        <span className="truncate font-mono text-[10px] font-bold tracking-[.16em] text-fg-faint uppercase">
          Data explorer
        </span>
        <button
          type="button"
          aria-label="Refresh catalogs"
          onClick={onRefresh}
          className="ml-auto flex h-6 w-6 items-center justify-center text-fg-faint hover:bg-hover hover:text-fg"
        >
          <RefreshCw size={13} className={loadingCatalogs ? "animate-spin" : undefined} />
        </button>
      </div>

      <div className="m-2 flex h-[30px] shrink-0 items-center gap-2 border border-line-strong bg-panel-alt px-2.5 focus-within:shadow-[0_0_0_2px_var(--lk-accent)]">
        <Search size={13} className="text-fg-faint" />
        <input
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="search tables"
          className="w-full bg-transparent font-mono text-[12px] outline-none placeholder:text-fg-faint"
        />
      </div>

      <div className="min-h-0 flex-1 overflow-auto pb-2">
        {catalogsError ? (
          <p className="mx-2 bg-danger-bg px-2.5 py-2 text-[12px] text-danger-fg">
            {catalogsError}
          </p>
        ) : null}

        {catalogs.map((catalog) => (
          <CatalogNode
            key={catalog}
            catalog={catalog}
            expanded={open[catalog] ?? false}
            onToggle={() => toggle(catalog)}
            openState={open}
            onToggleSchema={toggle}
            matches={matches}
            onPickTable={onPickTable}
          />
        ))}
      </div>
    </aside>
  );
}

interface CatalogNodeProps {
  catalog: string;
  expanded: boolean;
  onToggle: () => void;
  openState: Record<string, boolean>;
  onToggleSchema: (key: string) => void;
  matches: (name: string) => boolean;
  onPickTable: (catalog: string, schema: string, table: string) => void;
}

function CatalogNode({
  catalog,
  expanded,
  onToggle,
  openState,
  onToggleSchema,
  matches,
  onPickTable,
}: CatalogNodeProps) {
  // Each level loads on expand and caches independently, so one unreachable
  // metastore cannot stop the rest of the tree from working.
  const schemas = useQuery({
    queryKey: ["schemas", catalog],
    queryFn: () => listSchemas(catalog),
    enabled: expanded,
  });

  return (
    <div>
      <button type="button" className={ROW} onClick={onToggle}>
        <Chevron expanded={expanded} />
        <Database size={14} className="shrink-0 text-fg-faint" />
        <span className="truncate">{catalog}</span>
      </button>

      {expanded ? (
        <NodeStatus
          pending={schemas.isPending}
          error={schemas.error ? describeError(schemas.error) : undefined}
          indent={24}
        >
          {(schemas.data ?? []).map((schema) => {
            const key = nodeKey(catalog, schema);
            return (
              <SchemaNode
                key={key}
                catalog={catalog}
                schema={schema}
                expanded={openState[key] ?? false}
                onToggle={() => onToggleSchema(key)}
                openState={openState}
                onToggleTable={onToggleSchema}
                matches={matches}
                onPickTable={onPickTable}
              />
            );
          })}
        </NodeStatus>
      ) : null}
    </div>
  );
}

interface SchemaNodeProps {
  catalog: string;
  schema: string;
  expanded: boolean;
  onToggle: () => void;
  openState: Record<string, boolean>;
  onToggleTable: (key: string) => void;
  matches: (name: string) => boolean;
  onPickTable: (catalog: string, schema: string, table: string) => void;
}

function SchemaNode({
  catalog,
  schema,
  expanded,
  onToggle,
  openState,
  onToggleTable,
  matches,
  onPickTable,
}: SchemaNodeProps) {
  const tables = useQuery({
    queryKey: ["tables", catalog, schema],
    queryFn: () => listTables(catalog, schema),
    enabled: expanded,
  });
  const active = useApp((state) => state.activeTable);

  return (
    <div>
      <button type="button" className={`${ROW} pl-6`} onClick={onToggle}>
        <Chevron expanded={expanded} />
        <Folder size={14} className="shrink-0 text-fg-faint" />
        <span className="truncate">{schema}</span>
      </button>

      {expanded ? (
        <NodeStatus
          pending={tables.isPending}
          error={tables.error ? describeError(tables.error) : undefined}
          indent={44}
        >
          {(tables.data ?? []).filter(matches).map((table) => (
            <TableNode
              key={table}
              catalog={catalog}
              schema={schema}
              table={table}
              expanded={openState[nodeKey(catalog, schema, table)] ?? false}
              onToggle={() => onToggleTable(nodeKey(catalog, schema, table))}
              selected={
                active?.catalog === catalog && active.schema === schema && active.table === table
              }
              onPickTable={onPickTable}
            />
          ))}
        </NodeStatus>
      ) : null}
    </div>
  );
}

interface TableNodeProps {
  catalog: string;
  schema: string;
  table: string;
  expanded: boolean;
  onToggle: () => void;
  selected: boolean;
  onPickTable: (catalog: string, schema: string, table: string) => void;
}

/**
 * A table row that opens into its columns. The schema comes from planning a
 * `select * ... limit 0`, so the type labels match the result grid's.
 */
function TableNode({ catalog, schema, table, expanded, onToggle, selected, onPickTable }: TableNodeProps) {
  const columns = useQuery({
    queryKey: ["columns", catalog, schema, table],
    queryFn: () => describeTable(catalog, schema, table),
    enabled: expanded,
  });

  return (
    <div>
      <button
        type="button"
        onClick={onToggle}
        onDoubleClick={() => onPickTable(catalog, schema, table)}
        className={`${ROW} pl-10 ${
          selected
            ? "bg-accent-bg font-medium text-fg shadow-[inset_3px_0_0_var(--lk-accent)] hover:bg-accent-bg"
            : ""
        }`}
      >
        <Chevron expanded={expanded} />
        <Table2 size={14} className={`shrink-0 ${selected ? "text-fg" : "text-fg-faint"}`} />
        <span className="truncate">{table}</span>
      </button>

      {expanded ? (
        <NodeStatus
          pending={columns.isPending}
          error={columns.error ? describeError(columns.error) : undefined}
          indent={64}
        >
          {(columns.data ?? []).map((column) => (
            <div key={column.name} className={`${ROW} cursor-default pl-16 hover:bg-transparent`}>
              <RectangleVertical size={12} className="shrink-0 text-fg-faint" />
              <span className="truncate" title={column.name}>
                {column.name}
              </span>
              <span
                className="ml-auto shrink-0 pl-3 text-[11px] text-fg-faint"
                title={column.typeLabel}
              >
                {column.typeLabel}
              </span>
            </div>
          ))}
          {columns.data && columns.data.length === 0 ? (
            <p className="h-[26px] pl-[72px] font-mono text-[12px] leading-[26px] text-fg-faint">no columns</p>
          ) : null}
        </NodeStatus>
      ) : null}
    </div>
  );
}

function Chevron({ expanded }: { expanded: boolean }) {
  const Icon = expanded ? ChevronDown : ChevronRight;
  return <Icon size={12} className="w-3 shrink-0 text-fg-faint" />;
}

function NodeStatus({
  pending,
  error,
  indent,
  children,
}: {
  pending: boolean;
  error?: string;
  indent: number;
  children: React.ReactNode;
}) {
  if (pending) {
    return (
      <p
        className="h-[26px] font-mono text-[12px] leading-[26px] text-fg-faint"
        style={{ paddingLeft: indent + 8 }}
      >
        loading…
      </p>
    );
  }
  if (error) {
    return (
      <p
        className="my-1 mr-2 bg-danger-bg py-1.5 pr-2.5 text-[12px] text-danger-fg"
        style={{ paddingLeft: indent + 8 }}
      >
        {error}
      </p>
    );
  }
  return <>{children}</>;
}
