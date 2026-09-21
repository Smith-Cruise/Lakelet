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
import { qualifiedName } from "../lib/sql";
import {
  commandGetCatalogs,
  commandGetDbSchemas,
  commandGetTables,
  commandStatementQuery,
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
 * The columns of one table, read from the FlightInfo of a statement that is
 * planned but never run.
 *
 * GetFlightInfo answers a statement with the Arrow schema its result would
 * have, so a `limit 0` select is the cheapest way to ask what a table looks
 * like: one round trip, no DoGet, no rows. It also means the type labels here
 * come from the same schema the result grid renders.
 *
 * The ticket that comes back is dropped. Lakelet's statement ticket is the
 * SQL text itself, so not redeeming it leaves nothing behind on the server.
 */
export async function describeTable(
  catalog: string,
  dbSchema: string,
  table: string,
): Promise<ColumnMeta[]> {
  const flight = await client();
  const sql = `select * from ${qualifiedName(catalog, dbSchema, table)} limit 0`;
  const info = await flight.getFlightInfo({ cmd: commandStatementQuery(sql) });
  if (info.schema.byteLength === 0) {
    throw new Error("server returned no schema for the statement");
  }
  return decodeIpcSchema(info.schema).fields.map(describeColumn);
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
