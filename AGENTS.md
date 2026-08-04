# AGENTS.md - Development Guide for oracle_scanner

This document provides guidance for AI agents working on the oracle_scanner DuckDB extension.

## Project Overview

oracle_scanner is a read-only DuckDB storage extension that queries Oracle through an Oracle ODBC driver. It implements Oracle-specific catalog discovery, SQL generation, and type mapping on top of an internal ODBC transport layer.

## Technology Stack

- **Language**: C++ (C++17)
- **Build System**: CMake + Makefile (uses DuckDB extension template)
- **Dependencies**: DuckDB (submodule), ODBC libraries, vcpkg
- **Formatting**: clang-format (LLVM base style with customizations)

## Build Commands

```bash
# Build release version (default)
make release

# Build debug version
make debug

# Build with assertions
make relassert

# Build release with debug info
make reldebug

# Clean build artifacts
make clean

# Update DuckDB submodule
make update

# Format code (apply clang-format)
make format

# Check formatting without applying
make format-check

# Run clang-tidy for static analysis
make tidy-check
```

## Test Commands

```bash
# Run all tests (default - runs release tests)
make test

# Run tests for specific build types
make test_release    # Test release build
make test_debug      # Test debug build
make test_reldebug   # Test relwithdebinfo build

# Run a single test (using unittest pattern)
./build/release/test/unittest "test_name_pattern"

# Examples of single test patterns:
./build/release/test/unittest "test/sql/oracle_scanner*"
./build/release/test/unittest "*connection*"        # Tests with "connection" in name

# Skip tests (for CI outside docker)
SKIP_TESTS=1 make release
```

## Code Style Guidelines

### Formatting

- **Format Tool**: clang-format (configuration in `.clang-format`)
- **Check formatting**: `make format-check`
- **Apply formatting**: `make format` or `make format-fix`
- **Configuration**: Uses LLVM base style with customizations:
  - Tab width: 4
  - Column limit: 120
  - Pointer alignment: Right (`char* ptr`)
  - Use tabs for indentation
  - Sort includes: Regroup

### Naming Conventions

- **Classes**: PascalCase (e.g., `ODBCScanFunction`, `ODBCConnection`)
- **Functions**: PascalCase (e.g., `ParseConnectionParameters`)
- **Variables**: camelCase (e.g., `connectionString`, `result`)
- **Member variables**: camelCase or underscore-prefixed (e.g., `table` or `_table`)
- **Constants**: PascalCase or UPPER_SNAKE_CASE
- **Namespaces**: lowercase (e.g., `duckdb`)

### Code Structure

#### File Organization
- Header files in `src/include/`
- Implementation in `src/`
- Storage-related code in `src/storage/`
- Each header file should have DuckDB license header:
```cpp
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// oracle_scanner.hpp
//
//
//===----------------------------------------------------------------------===//
```

#### Include Order (clang-format will reorganize)
1. Primary headers (duckdb.hpp)
2. Extension headers
3. Standard library
4. System headers

#### Header Guards
- Use `#pragma once` instead of traditional guards

### Error Handling

Use appropriate exception types from DuckDB:

- `throw InvalidInputException("message with %s", var)` - For invalid user input
- `throw IOException("message")` - For I/O errors
- `throw BinderException("message")` - For binding/parameter errors
- `throw NotImplementedException("message")` - For unimplemented features
- `throw InternalException("message")` - For internal errors
- `throw NotImplementedException("message")` - For serialization not supported

### Memory Management

- Use DuckDB smart pointers: `make_uniq<T>()`, `make_shared<T>()`
- Avoid raw `new`/`delete`
- Use `unique_ptr` and `shared_ptr` from DuckDB namespace

### Types

- Use DuckDB types: `idx_t` (unsigned index), `string`, `Value`, `Vector`, `DataChunk`
- Use `LogicalType` for type system
- Avoid C types where DuckDB equivalents exist

### Common Patterns

#### Table Function Implementation
```cpp
// Bind function
static unique_ptr<FunctionData> ODBCBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    // Implementation
}

// Global state
struct ODBCGlobalState : public GlobalTableFunctionState {
    unique_ptr<ODBCResult> result;
    idx_t MaxThreads() const override { return 1; }
};

// Scan function
static void ODBCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    // Implementation
}
```

#### ODBC Resource Management
```cpp
// Use custom deleters for ODBC handles
using ODBCStatementPtr = duckdb::unique_ptr<void, void (*)(SQLHSTMT)>;

inline void ODBCStatementDelete(SQLHSTMT stmt) {
    if (stmt) {
        SQLFreeStmt(stmt, SQL_CLOSE);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
}
```

### Testing

- Tests should be in `test/` directory
- Use DuckDB's test framework
- Test patterns follow DuckDB conventions
- Registration and read-only tests do not require an Oracle driver
- Live database tests require Oracle Instant Client ODBC and an Oracle instance

### Git/Development Workflow

- Commit messages should be descriptive
- Update DuckDB submodule with `make update`
- Format code before committing (`make format`)
- Build and test before submitting

### Key Files

- `CMakeLists.txt` - Main CMake configuration
- `Makefile` - Extension build targets
- `extension_config.cmake` - DuckDB extension config
- `.clang-format` - Code formatting rules
- `src/odbc_scanner.cpp` - Oracle scan and query table functions
- `src/oracle_extension.cpp` - Extension registration and public settings
- `src/storage/odbc_schema_set.cpp` - Oracle owner discovery
- `src/storage/odbc_table_set.cpp` - Oracle table/view discovery
- `src/odbc_types.cpp` - Oracle type mapping

### Agent Workflow Requirements

**VERY IMPORTANT**: When you have completed a task, you MUST run the lint and typecheck commands with Bash if they were provided to you to ensure your code is correct. If you are unable to find the correct command, ask the user for the command to run and if they supply it, proactively suggest writing it to AGENTS.md so that you will know to run it next time.

**NEVER commit changes unless the user explicitly asks you to**. It is VERY IMPORTANT to only commit when explicitly asked, otherwise the user will feel that you are being too proactive.

### Code References

When referencing specific functions or pieces of code include the pattern `file_path:line_number` to allow the user to easily navigate to the source code location.

<example>
user: Where are errors from the client handled?
assistant: Clients are marked as failed in the `connectToServer` function in src/services/process.ts:712.
</example>

### Important Notes

1. This is a DuckDB loadable extension - it links against DuckDB
2. The public catalog is Oracle-specific; ODBC is an internal transport detail
3. Build requires ODBC development libraries (`unixODBC-dev`)
4. The extension only accepts `ATTACH ... (READ_ONLY)`
5. The DuckDB submodule must be initialized: `git submodule update --init --recursive`
