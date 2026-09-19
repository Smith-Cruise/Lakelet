/**
 * The only module that talks to `@sparrowflight/js`.
 *
 * Everything else in the app goes through the four functions below, so a
 * breaking change in that 0.x dependency lands here and nowhere else.
 *
 * sparrowJS earns its place by doing two things this app would otherwise have
 * to implement: it reassembles FlightData frames into a real Arrow IPC stream,
 * and it transcodes Utf8View/BinaryView — which DataFusion emits for every
 * parquet-backed string column — into types Arrow JS can actually decode.
 */

import { connect, type FlightClient } from "@sparrowflight/js";
import type { RecordBatch, Table } from "apache-arrow";
import { decodeIpcSchema } from "../lib/ipc";
import { describeColumn, type ColumnMeta } from "../lib/values";
import {
  commandGetCatalogs,
  commandGetDbSchemas,
  commandGetTables,
} from "./commands";

/** Rows kept in the browser. Past this the stream is cancelled. */
export const ROW_LIMIT = 10_000;

/** The catalog and schema unqualified names in a statement resolve against. */
export interface Scope {
  catalog?: string;
  schema?: string;
}

export interface QueryOutcome {
  batches: RecordBatch[];
  rowCount: number;
  elapsedMs: number;
  truncated: boolean;
}

const clients = new Map<string, Promise<FlightClient>>();

function scopeKey(scope?: Scope): string {
  return scope?.catalog ? `${scope.catalog}\u0000${scope.schema ?? ""}` : "";
}

/**
 * One client per scope.
 *
 * The server reads `default-catalog` and `default-schema` from the request
 * metadata, but sparrowJS only sets headers at connect time, so switching
 * scope means a new connection. They are cached because that costs a bootstrap
 * round trip, and the empty-scope client serves metadata calls, which carry
 * their catalog inside the command instead.
 */
function client(scope?: Scope): Promise<FlightClient> {
  const key = scopeKey(scope);
  const existing = clients.get(key);
  if (existing) {
    return existing;
  }

  const headers: Record<string, string> = {};
  if (scope?.catalog) {
    headers["default-catalog"] = scope.catalog;
  }
  if (scope?.schema) {
    headers["default-schema"] = scope.schema;
  }

  // A rejected connection must not stay cached, or every later query in this
  // scope replays the original failure instead of retrying.
  const pending = connect({ endpoint: window.location.origin, headers }).catch((error) => {
    clients.delete(key);
    throw error;
  });
  clients.set(key, pending);
  return pending;
}

/** Turns a transport or gRPC failure into the message the UI shows. */
export function describeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

/**
 * Runs a metadata command through the escape hatch.
 *
 * sparrowJS's own `tables()` sends an unfiltered GetTables, which Lakelet
 * rejects: it requires both a catalog and a schema pattern so that one click
 * on a tree cannot fan out across every configured metastore.
 */
async function metadataTable(cmd: Uint8Array): Promise<Table> {
  const flight = await client();
  const info = await flight.getFlightInfo({ cmd });
  const ticket = info.endpoint[0]?.ticket;
  if (!ticket) {
    throw new Error("server returned no endpoint for the metadata request");
  }
  const { table } = await flight.doGet(ticket);
  return table;
}

async function metadataColumn(cmd: Uint8Array, column: string): Promise<string[]> {
  return readStrings(await metadataTable(cmd), column);
}

function readStrings(table: Table, column: string): string[] {
  const vector = table.getChild(column);
  if (!vector) {
    throw new Error(`metadata result is missing the ${column} column`);
  }
  const values: string[] = [];
  for (const value of vector) {
    if (value !== null && value !== undefined) {
      values.push(String(value));
    }
  }
  return values;
}

export function listCatalogs(): Promise<string[]> {
  return metadataColumn(commandGetCatalogs(), "catalog_name");
}

export function listSchemas(catalog: string): Promise<string[]> {
  return metadataColumn(commandGetDbSchemas(catalog), "db_schema_name");
}

export function listTables(catalog: string, dbSchema: string): Promise<string[]> {
  return metadataColumn(commandGetTables(catalog, dbSchema), "table_name");
}

/**
 * The columns of one table, through GetTables with `include_schema`: the
 * standard Flight SQL way to read a table's schema without planning a query.
 * The name filter is a LIKE pattern, so the row is matched by exact name.
 */
export async function describeTable(
  catalog: string,
  dbSchema: string,
  table: string,
): Promise<ColumnMeta[]> {
  const result = await metadataTable(
    commandGetTables(catalog, dbSchema, { table, includeSchema: true }),
  );
  const names = result.getChild("table_name");
  const schemas = result.getChild("table_schema");
  if (!names || !schemas) {
    throw new Error("metadata result is missing the table_schema column");
  }
  for (let index = 0; index < result.numRows; index += 1) {
    if (names.get(index) === table) {
      const bytes = schemas.get(index) as Uint8Array | null;
      if (!bytes) {
        break;
      }
      return decodeIpcSchema(bytes).fields.map(describeColumn);
    }
  }
  throw new Error(`table ${table} was not found`);
}

/**
 * Streams a statement, keeping at most ROW_LIMIT rows.
 *
 * Batches are handed back as they decode so the grid can paint before the
 * whole result has landed; once the cap is reached the stream is cancelled
 * rather than drained, which is what keeps an unbounded query from taking the
 * tab down with it.
 */
export async function runQuery(
  sql: string,
  scope?: Scope,
  onProgress?: (rowCount: number) => void,
): Promise<QueryOutcome> {
  const flight = await client(scope);
  const startedAt = performance.now();
  const stream = flight.query(sql);

  const batches: RecordBatch[] = [];
  let rowCount = 0;
  let truncated = false;

  for await (const batch of stream) {
    batches.push(batch);
    rowCount += batch.numRows;
    onProgress?.(Math.min(rowCount, ROW_LIMIT));
    if (rowCount >= ROW_LIMIT) {
      truncated = rowCount > ROW_LIMIT;
      stream.cancel();
      break;
    }
  }

  return {
    batches,
    rowCount: Math.min(rowCount, ROW_LIMIT),
    elapsedMs: performance.now() - startedAt,
    truncated,
  };
}
