/** Turning Arrow values into something a grid cell and a histogram can use. */

import { DataType, Type, type DataType as ArrowType, type Field } from "apache-arrow";

/** How the column's values are summarised. */
export type ColumnKind = "numeric" | "temporal" | "categorical";

/** What the header icon says about the column; finer than `kind`. */
export type ColumnFamily =
  | "number"
  | "text"
  | "boolean"
  | "temporal"
  | "struct"
  | "list"
  | "binary"
  | "other";

export interface ColumnMeta {
  name: string;
  /** SQL-ish label shown under the column name. */
  typeLabel: string;
  kind: ColumnKind;
  family: ColumnFamily;
  /** Decimal scale, needed to place the point when decoding the raw words. */
  scale?: number;
}

function unwrapDictionary(type: ArrowType): ArrowType {
  return DataType.isDictionary(type) ? type.dictionary : type;
}

export function describeColumn(field: Field): ColumnMeta {
  const type = unwrapDictionary(field.type);
  const base = { name: field.name };

  switch (type.typeId) {
    case Type.Int: {
      const bits = (type as { bitWidth?: number }).bitWidth ?? 32;
      const label = bits === 64 ? "bigint" : bits === 16 ? "smallint" : bits === 8 ? "tinyint" : "int";
      return { ...base, typeLabel: label, kind: "numeric", family: "number" };
    }
    case Type.Float:
      return { ...base, typeLabel: "double", kind: "numeric", family: "number" };
    case Type.Decimal: {
      const { precision, scale } = type as { precision?: number; scale?: number };
      return {
        ...base,
        typeLabel: precision !== undefined ? `decimal(${precision},${scale ?? 0})` : "decimal",
        kind: "numeric",
        family: "number",
        scale: scale ?? 0,
      };
    }
    case Type.Bool:
      return { ...base, typeLabel: "boolean", kind: "categorical", family: "boolean" };
    case Type.Date:
      return { ...base, typeLabel: "date", kind: "temporal", family: "temporal" };
    case Type.Time:
      return { ...base, typeLabel: "time", kind: "temporal", family: "temporal" };
    case Type.Timestamp:
      return { ...base, typeLabel: "timestamp", kind: "temporal", family: "temporal" };
    case Type.Utf8:
    case Type.LargeUtf8:
      return { ...base, typeLabel: "varchar", kind: "categorical", family: "text" };
    case Type.Binary:
    case Type.LargeBinary:
      return { ...base, typeLabel: "binary", kind: "categorical", family: "binary" };
    case Type.List:
    case Type.FixedSizeList:
    case Type.LargeList:
      return { ...base, typeLabel: "array", kind: "categorical", family: "list" };
    case Type.Struct:
      return { ...base, typeLabel: "struct", kind: "categorical", family: "struct" };
    case Type.Map:
      return { ...base, typeLabel: "map", kind: "categorical", family: "struct" };
    case Type.Null:
      return { ...base, typeLabel: "null", kind: "categorical", family: "other" };
    default: {
      // Utf8View/BinaryView arrive with their own type ids that the enum above
      // predates; classify them by name so string columns still read as text.
      const name = String(type);
      if (/utf8/i.test(name)) {
        return { ...base, typeLabel: "varchar", kind: "categorical", family: "text" };
      }
      if (/binary/i.test(name)) {
        return { ...base, typeLabel: "binary", kind: "categorical", family: "binary" };
      }
      return { ...base, typeLabel: name, kind: "categorical", family: "other" };
    }
  }
}

/**
 * Decodes a decimal from the four little-endian 32-bit words Arrow JS hands
 * back. Without this a decimal column renders as "1,0,0,0".
 */
function decimalToString(words: Uint32Array, scale: number): string {
  let magnitude = 0n;
  for (let i = words.length - 1; i >= 0; i -= 1) {
    magnitude = (magnitude << 32n) | BigInt(words[i] >>> 0);
  }

  // Two's complement: the top bit of the highest word is the sign.
  const width = BigInt(words.length * 32);
  const signBit = 1n << (width - 1n);
  if (magnitude >= signBit) {
    magnitude -= 1n << width;
  }

  if (scale <= 0) {
    return magnitude.toString();
  }

  const negative = magnitude < 0n;
  const digits = (negative ? -magnitude : magnitude).toString().padStart(scale + 1, "0");
  const whole = digits.slice(0, digits.length - scale);
  const fraction = digits.slice(digits.length - scale);
  return `${negative ? "-" : ""}${whole}.${fraction}`;
}

const TIMESTAMP = new Intl.DateTimeFormat("sv-SE", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

export function formatValue(value: unknown, meta: ColumnMeta): string {
  if (value === null || value === undefined) {
    return "";
  }
  // Arrow JS hands timestamps back as epoch milliseconds, not Date objects, so
  // the column's own type is what decides whether a number is a point in time.
  if (meta.kind === "temporal") {
    const epochMs = typeof value === "bigint" ? Number(value) : value;
    if (typeof epochMs === "number" && Number.isFinite(epochMs)) {
      return TIMESTAMP.format(new Date(epochMs));
    }
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return TIMESTAMP.format(value);
  }
  if (value instanceof Uint32Array) {
    return decimalToString(value, meta.scale ?? 0);
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value, (_key, item) =>
        typeof item === "bigint" ? item.toString() : item,
      );
    } catch {
      return String(value);
    }
  }
  return String(value);
}
