# oracle_scanner

`oracle_scanner` is a read-only DuckDB storage extension for querying Oracle through an Oracle ODBC driver.

## Scope

- Read-only `ATTACH`
- Oracle owner, table, view, and materialized-view discovery
- Projection, filter, and `FETCH FIRST` limit pushdown
- `oracle_query`, `oracle_metadata`, and `oracle_clear_cache`
- Oracle `DATE`, `TIMESTAMP`, `NUMBER`, character, RAW, BLOB, CLOB, ROWID, JSON, XMLTYPE, and interval mappings

DDL, INSERT, UPDATE, DELETE, and arbitrary statements through `oracle_query` are intentionally unsupported.

## Runtime Requirements

Install Oracle Instant Client Basic and ODBC packages, then register the driver with unixODBC. The driver name is version-specific and must match `odbcinst -q -d`.

## Build

```bash
make release
make format-check
make test_release
```

The project is built in the shared development container with:

```bash
docker exec fedqueryBuilder bash -lc 'cd /workspace/oracle_scanner && make release'
```

## Usage

```sql
LOAD oracle_scanner;

ATTACH 'Driver={Oracle 23 ODBC driver};DBQ=//db.example:1521/service;UID=app;PWD=secret;'
AS oracle_app (TYPE oracle_scanner, READ_ONLY, SCHEMA 'APP');

SELECT *
FROM oracle_app.APP.ORDERS
WHERE ORDER_TIME >= TIMESTAMP '2026-01-01 00:00:00'
LIMIT 100;

SELECT *
FROM oracle_query('oracle_app', 'SELECT CURRENT_TIMESTAMP FROM DUAL');
```

`SCHEMA` is optional. When omitted, the scanner uses the upper-case ODBC `UID`, then falls back to Oracle `CURRENT_SCHEMA`.

### Oracle owner identifiers

`SCHEMA` uses Oracle identifier syntax. An unquoted owner is normalized to uppercase, while a double-quoted owner is
matched exactly. This prevents an attach with `SCHEMA 'linweilong'` from silently missing the ordinary Oracle owner
`LINWEILONG`.

```sql
-- Matches owner LINWEILONG
ATTACH '' AS oracle_app (TYPE oracle_scanner, READ_ONLY, SECRET oracle_secret, SCHEMA 'linweilong');

-- Matches the case-sensitive owner "MixedCaseOwner"
ATTACH '' AS oracle_quoted (TYPE oracle_scanner, READ_ONLY, SECRET oracle_secret, SCHEMA '"MixedCaseOwner"');
```

The optional schema argument to `oracle_metadata` follows the same rule.

## Initial Type Policy

- `NUMBER(p,s)` with supported precision and non-negative scale becomes DuckDB `DECIMAL`.
- Unconstrained `NUMBER` and negative-scale `NUMBER` become `DOUBLE`.
- Oracle `DATE` becomes DuckDB `TIMESTAMP` because Oracle `DATE` includes time-of-day.
- `TIMESTAMP WITH TIME ZONE` is exposed as `VARCHAR` until driver-specific timezone preservation is verified.
- LOB values use ODBC chunk retrieval and require live validation with the selected Oracle driver.
