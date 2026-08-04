# Build Guide

## Dependencies

- CMake
- C++17 compiler
- Make
- unixODBC development files on Linux or the Windows ODBC SDK
- vcpkg at `VCPKG_ROOT`, `../vcpkg`, or `./vcpkg`

The Oracle client is not linked into the extension. Oracle Instant Client ODBC is loaded dynamically by the ODBC driver manager when an Oracle database is attached.

## Development Container

```bash
docker exec fedqueryBuilder bash -lc 'cd /workspace/oracle_scanner && make release'
docker exec fedqueryBuilder bash -lc 'cd /workspace/oracle_scanner && make format-check'
docker exec fedqueryBuilder bash -lc 'cd /workspace/oracle_scanner && make test_release'
```

The release artifact is:

```text
build/release/extension/oracle_scanner/oracle_scanner.duckdb_extension
```

## Live Oracle Validation

Before live tests, verify the driver registration:

```bash
odbcinst -q -d
```

Use an Oracle account with read access to the configured owner. The first live validation must cover table and view discovery, quoted identifiers, NUMBER variants, DATE/TIMESTAMP values, UTF-8 text, CLOB, BLOB, filter pushdown, and LIMIT pushdown.
