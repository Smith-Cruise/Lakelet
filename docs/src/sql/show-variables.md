# SHOW VARIABLES

## Synopsis

```text
SHOW VARIABLES [ LIKE 'pattern' ]
```

## Description

List the DataFusion settings for the current session, sorted by name. The
`name` column identifies the setting, `value` shows its current value, and
`description` explains what it controls. `LIKE` filters on the setting name.

## Examples

```sql
SHOW VARIABLES;
```

Look up the session's target number of execution partitions.

```sql
SHOW VARIABLES LIKE 'datafusion.execution.target_partitions';
```
