/**
 * Flight SQL metadata commands, encoded by hand.
 *
 * These three messages are small and stable, so encoding them directly costs
 * far less than adding protobuf codegen to the build. Field numbers and the
 * type URL prefix match arrow-flight 58.4.0, which is what the server runs:
 * see `arrow-flight/src/sql/arrow.flight.protocol.sql.rs` for the tags and
 * `arrow-flight/src/sql/mod.rs` for the `type.googleapis.com/...` prefix.
 */

const TYPE_URL_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql.";

const encoder = new TextEncoder();

function varint(value: number): number[] {
  const out: number[] = [];
  let rest = value;
  while (rest > 0x7f) {
    out.push((rest & 0x7f) | 0x80);
    rest >>>= 7;
  }
  out.push(rest);
  return out;
}

/** A length-delimited field: tag byte, length, then the bytes themselves. */
function lengthDelimited(fieldNumber: number, payload: Uint8Array): number[] {
  return [
    (fieldNumber << 3) | 2,
    ...varint(payload.length),
    ...payload,
  ];
}

function stringField(fieldNumber: number, value: string): number[] {
  return lengthDelimited(fieldNumber, encoder.encode(value));
}

/** A varint field holding a bool: tag byte with wire type 0, then 0 or 1. */
function boolField(fieldNumber: number, value: boolean): number[] {
  return [fieldNumber << 3, value ? 1 : 0];
}

/** google.protobuf.Any: type_url = 1, value = 2. */
function wrapInAny(messageName: string, body: Uint8Array): Uint8Array {
  return new Uint8Array([
    ...stringField(1, TYPE_URL_PREFIX + messageName),
    ...lengthDelimited(2, body),
  ]);
}

/** CommandGetCatalogs has no fields. */
export function commandGetCatalogs(): Uint8Array {
  return wrapInAny("CommandGetCatalogs", new Uint8Array());
}

/**
 * CommandGetDbSchemas: catalog = 1, db_schema_filter_pattern = 2.
 *
 * The pattern is left unset on purpose. Lakelet hands it to the Flight SQL
 * builder, which treats an absent pattern as "every schema" — exactly what
 * expanding a catalog wants — while `GetTables` below requires one.
 */
export function commandGetDbSchemas(catalog: string): Uint8Array {
  return wrapInAny(
    "CommandGetDbSchemas",
    new Uint8Array(stringField(1, catalog)),
  );
}

/**
 * CommandGetTables: catalog = 1, db_schema_filter_pattern = 2,
 * table_name_filter_pattern = 3, table_types = 4, include_schema = 5.
 *
 * Both name filters are LIKE patterns, so exact names are escaped. Lakelet
 * only honours `include_schema` together with a table name filter, since
 * each table's schema costs a metastore round trip.
 */
export function commandGetTables(
  catalog: string,
  dbSchema: string,
  options: { table?: string; includeSchema?: boolean } = {},
): Uint8Array {
  return wrapInAny(
    "CommandGetTables",
    new Uint8Array([
      ...stringField(1, catalog),
      ...stringField(2, escapeLikePattern(dbSchema)),
      ...(options.table === undefined ? [] : stringField(3, escapeLikePattern(options.table))),
      ...(options.includeSchema ? boolField(5, true) : []),
    ]),
  );
}

/**
 * Turns an exact schema name into a LIKE pattern that matches only itself.
 *
 * The server filters schemas with SQL LIKE, where `_` matches any single
 * character — and `_` is in most real schema names. Without this, expanding
 * `user_events` would also pull in the tables of `userXevents`.
 */
export function escapeLikePattern(name: string): string {
  return name.replace(/([\\%_])/g, "\\$1");
}
