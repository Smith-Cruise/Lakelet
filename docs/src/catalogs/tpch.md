---
icon: lucide/database
---

# TPC-H

Configure `[[catalog.tpch]]` to query read-only TPC-H tables. Lakelet
generates the data on demand.

## Configuration

```toml
[[catalog.tpch]]
name = "tpch"
```

| Option | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | String | Yes | Unique catalog name used in SQL. |

## Schemas

The schema name selects the scale factor.

| Schema | Scale factor |
| --- | --- |
| `sf1` | 1 |
| `sf10` | 10 |
| `sf100` | 100 |
| `sf300` | 300 |
| `sf1000` | 1000 |

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
