import { completeFromList, type CompletionSource } from "@codemirror/autocomplete";

/**
 * The words worth completing when writing DataFusion SQL. Deliberately a
 * short list: the stock Postgres table offers `serial4` and `selective` for
 * `sel`, which is noise for an analytics workbench.
 */
const KEYWORDS = [
  "select", "from", "where", "group by", "having", "order by", "limit", "offset",
  "join", "inner join", "left join", "right join", "full join", "cross join", "on", "using",
  "as", "and", "or", "not", "in", "is", "null", "distinct", "all",
  "union", "union all", "except", "intersect",
  "case", "when", "then", "else", "end", "cast", "with", "recursive",
  "insert into", "values", "overwrite", "create table", "drop table", "external", "location",
  "show tables", "show catalogs", "show schemas", "show columns", "describe", "explain", "analyze",
  "between", "like", "ilike", "exists", "asc", "desc", "nulls first", "nulls last",
  "true", "false", "interval", "over", "partition by", "rows", "range",
  "unbounded preceding", "unbounded following", "current row", "filter", "qualify", "unnest", "lateral",
  "set", "copy",
  // Type names, for CAST and column definitions.
  "int", "integer", "bigint", "smallint", "tinyint", "varchar", "text", "boolean",
  "double", "float", "decimal", "date", "time", "timestamp",
];

export const sqlKeywords: CompletionSource = completeFromList(
  KEYWORDS.map((label) => ({ label, type: "keyword" })),
);
