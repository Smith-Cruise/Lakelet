import { useCallback } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Group, Panel, Separator } from "react-resizable-panels";
import { TopBar } from "./components/TopBar";
import { CatalogTree } from "./components/CatalogTree";
import { SqlEditor } from "./components/SqlEditor";
import { ResultPanel } from "./components/ResultPanel";
import { StatusBar } from "./components/StatusBar";
import { describeError, listCatalogs, runQuery, ROW_LIMIT } from "./flight/client";
import { buildResultSet } from "./lib/result";
import { qualifiedName } from "./lib/sql";
import { useApp } from "./store";

/** A hairline between panels with an 8px grab area; it goes full ink while hovered or dragged. */
function Gutter({ direction }: { direction: "horizontal" | "vertical" }) {
  const horizontal = direction === "horizontal";
  return (
    <Separator
      className={`group relative outline-none ${horizontal ? "w-px" : "h-px"} bg-line data-[separator=active]:bg-line-strong data-[separator=hover]:bg-line-strong`}
    >
      <div className={`absolute ${horizontal ? "inset-y-0 -left-1 w-2" : "inset-x-0 -top-1 h-2"}`} />
    </Separator>
  );
}

export function App() {
  const queryClient = useQueryClient();
  const { tabs, activeTabId, updateSql, setTabScope, setActiveTable, run, setRun } = useApp();
  const active = tabs.find((tab) => tab.id === activeTabId) ?? tabs[0];

  const catalogs = useQuery({ queryKey: ["catalogs"], queryFn: listCatalogs });

  // Statements arrive one per array entry, already split by the editor, and
  // run in order: Flight SQL takes a single statement per request. The result
  // panel follows along and ends on the last one, or on the first failure.
  const onRun = useCallback(
    async (statements: string[]) => {
      // The tab's chips are what unqualified names resolve against, so they
      // have to reach the server rather than only steer the tree.
      const scope = { catalog: active.catalog, schema: active.schema };
      for (const statement of statements) {
        const sql = statement.trim();
        if (sql === "") {
          continue;
        }
        setRun({ status: "running", streamedRows: 0 });
        try {
          const outcome = await runQuery(sql, scope, (rows) =>
            setRun({ status: "running", streamedRows: rows }),
          );
          setRun({
            status: "done",
            streamedRows: outcome.rowCount,
            result: buildResultSet(
              outcome.batches,
              ROW_LIMIT,
              outcome.elapsedMs,
              outcome.truncated,
            ),
          });
        } catch (error) {
          // The server already classifies failures (invalid SQL, unimplemented,
          // resources exhausted); showing its message beats re-deriving one here.
          setRun({ status: "error", streamedRows: 0, error: describeError(error) });
          return;
        }
      }
    },
    [active.catalog, active.schema, setRun],
  );

  const onPickTable = useCallback(
    (pickedCatalog: string, pickedSchema: string, table: string) => {
      const target = qualifiedName(pickedCatalog, pickedSchema, table);
      setTabScope(activeTabId, pickedCatalog, pickedSchema);
      setActiveTable({ catalog: pickedCatalog, schema: pickedSchema, table });
      updateSql(activeTabId, `select *\nfrom ${target}\nlimit 100;`);
    },
    [activeTabId, setActiveTable, setTabScope, updateSql],
  );

  return (
    <div className="flex h-full flex-col bg-panel">
      <TopBar />

      <Group orientation="horizontal" className="min-h-0 flex-1">
        <Panel defaultSize="18" minSize="12" maxSize="40">
          <CatalogTree
            catalogs={catalogs.data ?? []}
            catalogsError={catalogs.error ? describeError(catalogs.error) : undefined}
            loadingCatalogs={catalogs.isFetching}
            onRefresh={() => queryClient.invalidateQueries()}
            onPickTable={onPickTable}
          />
        </Panel>
        <Gutter direction="horizontal" />
        <Panel minSize="40">
          <Group orientation="vertical">
            <Panel defaultSize="40" minSize="15">
              <SqlEditor tab={active} running={run.status === "running"} onRun={onRun} />
            </Panel>
            <Gutter direction="vertical" />
            <Panel minSize="20">
              <ResultPanel />
            </Panel>
          </Group>
        </Panel>
      </Group>

      <StatusBar connected={catalogs.isSuccess} busy={catalogs.isFetching} />
    </div>
  );
}
