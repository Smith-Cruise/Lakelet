# INSERT

## Synopsis

```text
INSERT INTO table_name [ (column_name [, column_name ...]) ] source
INSERT OVERWRITE table_name [ (column_name [, column_name ...]) ] source

source = VALUES (expression [, expression ...]) [, (expression [, expression ...]) ...]
       | query
```

## Description

Write rows to an existing table from a `VALUES` list or a query. The target
column list specifies which column receives each input value. Without a
column list, values follow the table's column order. The result includes a
`count` of the rows written.

On [Paimon tables](../table-formats/paimon.md#writing-tables), `INSERT INTO`
writes rows using the table's configured merge behavior. `INSERT OVERWRITE`
replaces all rows in an unpartitioned table. In a partitioned table, it
replaces the entire contents of each partition present in the input and
preserves the other partitions.

## Examples

These two orders go into different partitions of the [example table](create-table.md).

```sql
INSERT INTO paimon_local.sales.orders (id, order_day)
VALUES (1, '2026-01-01'), (2, '2026-01-02');
```

Replace the January 1 partition with one row from a query. The January 2
order is still in the table.

```sql
INSERT OVERWRITE paimon_local.sales.orders (id, order_day)
SELECT 3, '2026-01-01';
```
