# SHOW CATALOGS

## Synopsis

```text
SHOW CATALOGS [ LIKE 'pattern' ]
```

## Description

List the configured catalogs and the built-in `internal` catalog. The result
contains `catalog_name` and `catalog_type`, sorted by name.
`LIKE` filters the names. In the pattern, `%` matches any sequence of
characters and `_` matches one character.

## Examples

```sql
SHOW CATALOGS;
```

Find catalogs whose names start with `tp`, such as `tpch` and `tpcds`.

```sql
SHOW CATALOGS LIKE 'tp%';
```
