import { useCallback, useEffect, useState } from "react";
import { ChevronRight, Database, FolderOpen, Loader2, RefreshCw, Table2 } from "lucide-react";
import { runQueryFirstColumn } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

/** Quote an SQL identifier, doubling embedded quotes. */
function q(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

interface SchemaNode {
  name: string;
  expanded: boolean;
  loading: boolean;
  error: string | null;
  tables: string[] | null;
}

interface CatalogNode {
  name: string;
  expanded: boolean;
  loading: boolean;
  error: string | null;
  schemas: SchemaNode[] | null;
}

interface CatalogTreeProps {
  /** Insert a fully qualified table name into the editor. */
  onPickTable: (qualifiedName: string) => void;
}

export function CatalogTree({ onPickTable }: CatalogTreeProps) {
  const [catalogs, setCatalogs] = useState<CatalogNode[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadCatalogs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const names = await runQueryFirstColumn("show catalogs;");
      setCatalogs(
        names.map((name) => ({
          name,
          expanded: false,
          loading: false,
          error: null,
          schemas: null,
        })),
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setCatalogs(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadCatalogs();
  }, [loadCatalogs]);

  const patchCatalog = (name: string, patch: Partial<CatalogNode>) => {
    setCatalogs((prev) => prev?.map((c) => (c.name === name ? { ...c, ...patch } : c)) ?? prev);
  };

  const patchSchema = (catalog: string, schema: string, patch: Partial<SchemaNode>) => {
    setCatalogs(
      (prev) =>
        prev?.map((c) =>
          c.name === catalog
            ? {
                ...c,
                schemas:
                  c.schemas?.map((s) => (s.name === schema ? { ...s, ...patch } : s)) ?? c.schemas,
              }
            : c,
        ) ?? prev,
    );
  };

  const toggleCatalog = async (node: CatalogNode) => {
    if (node.expanded) {
      patchCatalog(node.name, { expanded: false });
      return;
    }
    patchCatalog(node.name, { expanded: true });
    if (node.schemas !== null) {
      return;
    }
    patchCatalog(node.name, { loading: true, error: null });
    try {
      const names = await runQueryFirstColumn(`show schemas from ${q(node.name)};`);
      patchCatalog(node.name, {
        loading: false,
        schemas: names.map((name) => ({
          name,
          expanded: false,
          loading: false,
          error: null,
          tables: null,
        })),
      });
    } catch (err) {
      patchCatalog(node.name, {
        loading: false,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  const toggleSchema = async (catalog: string, node: SchemaNode) => {
    if (node.expanded) {
      patchSchema(catalog, node.name, { expanded: false });
      return;
    }
    patchSchema(catalog, node.name, { expanded: true });
    if (node.tables !== null) {
      return;
    }
    patchSchema(catalog, node.name, { loading: true, error: null });
    try {
      const tables = await runQueryFirstColumn(`show tables from ${q(catalog)}.${q(node.name)};`);
      patchSchema(catalog, node.name, { loading: false, tables });
    } catch (err) {
      patchSchema(catalog, node.name, {
        loading: false,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between px-3 py-2">
        <span className="text-xs font-medium text-muted-foreground">Catalogs</span>
        <Button
          variant="ghost"
          size="icon"
          className="size-6"
          onClick={() => void loadCatalogs()}
          disabled={loading}
          title="Refresh catalogs"
        >
          <RefreshCw className={cn("size-3.5", loading && "animate-spin")} />
        </Button>
      </div>
      <div className="min-h-0 flex-1 overflow-auto px-1.5 pb-2 text-[13px]">
        {loading && !catalogs && (
          <div className="flex items-center gap-2 px-2 py-1.5 text-muted-foreground">
            <Loader2 className="size-3.5 animate-spin" /> Loading…
          </div>
        )}
        {error && <div className="px-2 py-1.5 break-words text-destructive">{error}</div>}
        {catalogs?.length === 0 && (
          <div className="px-2 py-1.5 text-muted-foreground">No catalogs</div>
        )}
        {catalogs?.map((catalog) => (
          <div key={catalog.name}>
            <TreeRow
              depth={0}
              expanded={catalog.expanded}
              loading={catalog.loading}
              icon={<Database className="size-3.5 shrink-0 text-muted-foreground" />}
              label={catalog.name}
              onClick={() => void toggleCatalog(catalog)}
            />
            {catalog.expanded && catalog.error && (
              <div className="py-1 pr-2 pl-8 break-words text-xs text-destructive">
                {catalog.error}
              </div>
            )}
            {catalog.expanded && catalog.schemas?.length === 0 && (
              <div className="py-1 pl-8 text-xs text-muted-foreground">No schemas</div>
            )}
            {catalog.expanded &&
              catalog.schemas?.map((schema) => (
                <div key={schema.name}>
                  <TreeRow
                    depth={1}
                    expanded={schema.expanded}
                    loading={schema.loading}
                    icon={<FolderOpen className="size-3.5 shrink-0 text-muted-foreground" />}
                    label={schema.name}
                    onClick={() => void toggleSchema(catalog.name, schema)}
                  />
                  {schema.expanded && schema.error && (
                    <div className="py-1 pr-2 pl-12 break-words text-xs text-destructive">
                      {schema.error}
                    </div>
                  )}
                  {schema.expanded && schema.tables?.length === 0 && (
                    <div className="py-1 pl-12 text-xs text-muted-foreground">No tables</div>
                  )}
                  {schema.expanded &&
                    schema.tables?.map((table) => (
                      <button
                        key={table}
                        className="flex w-full items-center gap-1.5 rounded-md py-1 pr-2 pl-12 text-left hover:bg-accent"
                        onClick={() =>
                          onPickTable(`${q(catalog.name)}.${q(schema.name)}.${q(table)}`)
                        }
                        title={`Insert ${catalog.name}.${schema.name}.${table}`}
                      >
                        <Table2 className="size-3.5 shrink-0 text-muted-foreground" />
                        <span className="truncate">{table}</span>
                      </button>
                    ))}
                </div>
              ))}
          </div>
        ))}
      </div>
    </div>
  );
}

interface TreeRowProps {
  depth: number;
  expanded: boolean;
  loading: boolean;
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
}

function TreeRow({ depth, expanded, loading, icon, label, onClick }: TreeRowProps) {
  return (
    <button
      className="flex w-full items-center gap-1 rounded-md py-1 pr-2 text-left hover:bg-accent"
      style={{ paddingLeft: `${depth * 16 + 8}px` }}
      onClick={onClick}
    >
      {loading ? (
        <Loader2 className="size-3.5 shrink-0 animate-spin text-muted-foreground" />
      ) : (
        <ChevronRight
          className={cn(
            "size-3.5 shrink-0 text-muted-foreground transition-transform",
            expanded && "rotate-90",
          )}
        />
      )}
      {icon}
      <span className="truncate">{label}</span>
    </button>
  );
}
