---
icon: lucide/database
---

# TPC-H

Use `[[catalog.tpch]]` to query generated TPC-H data without an external
metastore or data files. Data is generated when queried; the catalog is read-only.

## Configuration

```toml
[[catalog.tpch]]
name = "tpch"
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used in SQL. |

## Schemas

The schema name selects the scale factor.

| Schema | Scale factor |
| --- | --- |
| `sf1` | 1 |
| `sf10` | 10 |
| `sf100` | 100 |
| `sf300` | 300 |
| `sf1000` | 1000 |

## Query the data

```sql
SHOW SCHEMAS FROM tpch;
SHOW TABLES FROM tpch.sf1;
DESC tpch.sf1.nation;
SELECT n_nationkey, n_name FROM tpch.sf1.nation LIMIT 10;

USE tpch.sf1;
SELECT * FROM region;
```

## Tables

| Table |
| --- |
| `customer` |
| `lineitem` |
| `nation` |
| `orders` |
| `part` |
| `partsupp` |
| `region` |
| `supplier` |
