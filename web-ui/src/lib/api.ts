import { RecordBatchReader, type RecordBatch, type Schema } from "apache-arrow";

export interface ServerInfo {
  version: string;
  default_catalog: string | null;
  default_schema: string | null;
}

export class QueryError extends Error {
  code: string;

  constructor(message: string, code: string) {
    super(message);
    this.name = "QueryError";
    this.code = code;
  }
}

// All requests are same-origin relative: this page is served by
// `lakelet --ui`, which mounts the API routes next to the UI proxy.

async function errorFromResponse(resp: Response): Promise<QueryError> {
  try {
    const body = await resp.json();
    return new QueryError(String(body.error ?? resp.statusText), String(body.code ?? "internal"));
  } catch {
    return new QueryError(`HTTP ${resp.status} ${resp.statusText}`, "internal");
  }
}

export async function getInfo(signal?: AbortSignal): Promise<ServerInfo> {
  const resp = await fetch("/api/info", { signal });
  if (!resp.ok) {
    throw await errorFromResponse(resp);
  }
  return resp.json();
}

export interface QueryContext {
  catalog?: string;
  schema?: string;
}

/** Run a query and stream Arrow record batches to `onBatch` as they arrive. */
export async function runQuery(
  sql: string,
  context: QueryContext,
  signal: AbortSignal,
  onSchema: (schema: Schema) => void,
  onBatch: (batch: RecordBatch) => void,
): Promise<void> {
  const resp = await fetch("/api/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql, ...context }),
    signal,
  });
  if (!resp.ok) {
    throw await errorFromResponse(resp);
  }
  const reader = await RecordBatchReader.from(resp.body!);
  await reader.open();
  onSchema(reader.schema);
  for await (const batch of reader) {
    onBatch(batch);
  }
}

/** Run a query and collect the first column as strings (metadata helpers). */
export async function runQueryFirstColumn(sql: string, signal?: AbortSignal): Promise<string[]> {
  const values: string[] = [];
  await runQuery(
    sql,
    {},
    signal ?? new AbortController().signal,
    () => {},
    (batch) => {
      const column = batch.getChildAt(0);
      if (!column) {
        return;
      }
      for (const value of column) {
        values.push(String(value));
      }
    },
  );
  return values;
}
