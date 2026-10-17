---
icon: lucide/database
---

# TPC-DS

Use `[[catalog.tpcds]]` to query generated TPC-DS data without an external
metastore or data files. Data is generated when queried; the catalog is read-only.

## Configuration

```toml
[[catalog.tpcds]]
name = "tpcds"
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
SHOW SCHEMAS FROM tpcds;
SHOW TABLES FROM tpcds.sf1;
DESC tpcds.sf1.date_dim;
SELECT d_date_sk, d_date FROM tpcds.sf1.date_dim LIMIT 10;

USE tpcds.sf1;
SELECT * FROM income_band LIMIT 10;
```

## Tables

| Table |
| --- |
| `call_center` |
| `catalog_page` |
| `catalog_returns` |
| `catalog_sales` |
| `customer` |
| `customer_address` |
| `customer_demographics` |
| `date_dim` |
| `household_demographics` |
| `income_band` |
| `inventory` |
| `item` |
| `promotion` |
| `reason` |
| `ship_mode` |
| `store` |
| `store_returns` |
| `store_sales` |
| `time_dim` |
| `warehouse` |
| `web_page` |
| `web_returns` |
| `web_sales` |
| `web_site` |
