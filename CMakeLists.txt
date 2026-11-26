cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME oracle_scanner)
project(${TARGET_NAME})

# Platform-specific dependency handling (Windows and Linux)
if(WIN32)
    # Windows: Use system ODBC SDK (odbc32.lib)
    set(ODBC_LIBRARY odbc32)
    set(ODBC_INCLUDE_DIR "")
    message(STATUS "ODBC: Using Windows ODBC SDK")
else()
    # Linux: unixODBC (libodbc.so); vcpkg or system package provides headers and -lodbc
    set(ODBC_LIBRARY odbc)
    if(NOT ODBC_INCLUDE_DIR)
        set(ODBC_INCLUDE_DIR "")
    endif()
    message(STATUS "ODBC: Using unixODBC (Linux)")
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)

# Include directories
if(ODBC_INCLUDE_DIR)
    include_directories(BEFORE ${ODBC_INCLUDE_DIR})
endif()

option(ORACLE_GUARDED_CRUD "Enable restricted scanner-owned Oracle CRUD" OFF)
if(ORACLE_GUARDED_CRUD)
 add_compile_definitions(ORACLE_GUARDED_CRUD)
endif()

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

# Loadable binary - include directories
target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE include)

if(ODBC_INCLUDE_DIR)
    target_include_directories(${TARGET_NAME}_loadable_extension
                               PRIVATE ${ODBC_INCLUDE_DIR})
endif()

# Link libraries
target_link_libraries(${TARGET_NAME}_loadable_extension 
                      ${ODBC_LIBRARY})

option(SCANNER_BUILD_CAPABILITY_TESTS "Build scanner capability contract tests" OFF)
if(SCANNER_BUILD_CAPABILITY_TESTS)
    add_executable(oracle_sparse_metadata_test test/oracle_sparse_metadata_test.cpp)
    target_include_directories(oracle_sparse_metadata_test PRIVATE src/include)
    target_link_libraries(oracle_sparse_metadata_test duckdb)
    add_test(NAME oracle_sparse_metadata COMMAND oracle_sparse_metadata_test)
    add_executable(guarded_odbc_source_error_test test/guarded_odbc_source_error_test.cpp)
    target_include_directories(guarded_odbc_source_error_test PRIVATE src/include)
    target_link_libraries(guarded_odbc_source_error_test duckdb)
    add_test(NAME guarded_odbc_source_errors COMMAND guarded_odbc_source_error_test)
    add_executable(odbc_exact_numeric_test test/odbc_exact_numeric_test.cpp src/odbc_types.cpp)
    target_include_directories(odbc_exact_numeric_test PRIVATE src/include)
    target_link_libraries(odbc_exact_numeric_test duckdb)
    add_test(NAME odbc_exact_numeric COMMAND odbc_exact_numeric_test)
    target_compile_definitions(odbc_exact_numeric_test PRIVATE EXACT_NUMERIC_ORACLE)
    add_executable(odbc_query_metrics_test test/odbc_query_metrics_test.cpp ${ALL_OBJECT_FILES})
    target_include_directories(odbc_query_metrics_test PRIVATE src/include)
    target_link_libraries(odbc_query_metrics_test duckdb ${ODBC_LIBRARY})
    add_test(NAME odbc_query_metrics COMMAND odbc_query_metrics_test)
    add_executable(oracle_metadata_diagnostics_test test/oracle_metadata_diagnostics_test.cpp)
    target_include_directories(oracle_metadata_diagnostics_test PRIVATE src/include)
    target_link_libraries(oracle_metadata_diagnostics_test duckdb)
    add_test(NAME oracle_metadata_diagnostics COMMAND oracle_metadata_diagnostics_test)
    # Test-only module lets the actual Go SQL driver exercise the same binder.
    build_loadable_extension(oracle_diagnostics_test "-no-warnings" test/oracle_metadata_diagnostics_test.cpp)
    target_include_directories(oracle_diagnostics_test_loadable_extension PRIVATE src/include)
    target_compile_definitions(oracle_diagnostics_test_loadable_extension PRIVATE ORACLE_DIAGNOSTICS_TEST_EXTENSION)

    add_executable(oracle_temporal_test
                   test/oracle_temporal_test.cpp
                   src/odbc_utils.cpp
                   src/odbc_filter_pushdown.cpp
                   src/odbc_parameter.cpp
                   src/odbc_types.cpp)
    target_include_directories(oracle_temporal_test PRIVATE src/include)
    target_link_libraries(oracle_temporal_test duckdb ${ODBC_LIBRARY})
    add_test(NAME oracle_temporal COMMAND oracle_temporal_test)
endif()
