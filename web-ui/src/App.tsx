import { useCallback, useEffect, useRef } from "react";
import { Download, History, Play, Square, TableProperties } from "lucide-react";
import { TopBar } from "@/components/TopBar";
import { ConnectScreen } from "@/components/ConnectScreen";
import { CatalogTree } from "@/components/CatalogTree";
import { HistoryList } from "@/components/HistoryList";
import { SqlEditor, type SqlEditorHandle } from "@/components/SqlEditor";
import { ResultsGrid } from "@/components/ResultsGrid";
import { StatusBar } from "@/components/StatusBar";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useConnection } from "@/stores/connection";
import { useQuery } from "@/stores/query";
import { exportCsv } from "@/lib/csv";
import { DEFAULT_ROW_LIMIT } from "@/lib/limit";

export default function App() {
  const status = useConnection((state) => state.status);
  const connect = useConnection((state) => state.connect);
  const port = useConnection((state) => state.port);

  // Probe the saved/default port once on load; failure shows the connect form.
  useEffect(() => {
    void connect(port);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="flex h-full flex-col">
      <TopBar />
      {status === "connected" ? <Workspace /> : <ConnectScreen />}
    </div>
  );
}

function Workspace() {
  const running = useQuery((state) => state.running);
  const limitGuard = useQuery((state) => state.limitGuard);
  const toggleLimitGuard = useQuery((state) => state.toggleLimitGuard);
  const run = useQuery((state) => state.run);
  const stop = useQuery((state) => state.stop);
  const schema = useQuery((state) => state.schema);
  const batches = useQuery((state) => state.batches);
  const rowCount = useQuery((state) => state.rowCount);
  const error = useQuery((state) => state.error);

  const editorRef = useRef<SqlEditorHandle>(null);

  const runCurrent = useCallback(() => {
    const sql = editorRef.current?.getValue() ?? "";
    if (sql.trim()) {
      void run(sql);
    }
  }, [run]);

  const pickFromHistory = useCallback((sql: string) => {
    editorRef.current?.setValue(sql);
    editorRef.current?.focus();
  }, []);

  const insertTable = useCallback((qualifiedName: string) => {
    editorRef.current?.insertAtCursor(qualifiedName);
  }, []);

  return (
    // The right gutter keeps the content column off the browser edge,
    // mirroring the breathing room the sidebar gives on the left.
    <div className="flex min-h-0 flex-1 pr-2">
      <aside className="flex w-64 shrink-0 flex-col border-r">
        <Tabs defaultValue="catalog" className="flex min-h-0 flex-1 flex-col">
          <TabsList className="mx-2 mt-2 grid grid-cols-2 self-stretch">
            <TabsTrigger value="catalog">
              <TableProperties className="size-3.5" /> Catalog
            </TabsTrigger>
            <TabsTrigger value="history">
              <History className="size-3.5" /> History
            </TabsTrigger>
          </TabsList>
          <TabsContent value="catalog" className="flex min-h-0 flex-col">
            <CatalogTree onPickTable={insertTable} />
          </TabsContent>
          <TabsContent value="history" className="flex min-h-0 flex-col">
            <HistoryList onPick={pickFromHistory} />
          </TabsContent>
        </Tabs>
      </aside>

      <main className="flex min-w-0 flex-1 flex-col border-r">
        <div className="flex h-2/5 min-h-36 flex-col border-b">
          <div className="flex shrink-0 items-center gap-3 border-b px-3 py-2">
            {running ? (
              <Button variant="destructive" size="sm" onClick={stop}>
                <Square className="size-3.5" /> Stop
              </Button>
            ) : (
              <Button size="sm" onClick={runCurrent}>
                <Play className="size-3.5" /> Run
              </Button>
            )}
            <label className="flex cursor-pointer items-center gap-1.5 text-xs text-muted-foreground select-none">
              <Checkbox checked={limitGuard} onCheckedChange={toggleLimitGuard} />
              LIMIT {DEFAULT_ROW_LIMIT}
            </label>
          </div>
          <div className="min-h-0 flex-1">
            <SqlEditor ref={editorRef} onRun={runCurrent} />
          </div>
        </div>
        <div className="relative min-h-0 flex-1">
          <ResultsGrid />
          {schema && rowCount > 0 && !running && !error && (
            <Button
              variant="outline"
              size="sm"
              className="absolute right-4 bottom-3 z-10 bg-background/90 shadow-md backdrop-blur"
              onClick={() => exportCsv(schema, batches)}
            >
              <Download className="size-3.5" /> CSV
            </Button>
          )}
        </div>
        <StatusBar />
      </main>
    </div>
  );
}
