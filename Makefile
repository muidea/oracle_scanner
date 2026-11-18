PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=oracle_scanner
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
# The fedquery runtime loads DuckDB v1.5.3 extensions. Keep the default
# metadata/version in sync so a plain `make release` cannot emit v0.0.1.
OVERRIDE_GIT_DESCRIBE ?= v2.0.0

# ====== vcpkg Configuration ======
# Auto-detect vcpkg installation path
# Priority: 1. VCPKG_ROOT env var, 2. ../vcpkg, 3. ./vcpkg

# Check if VCPKG_ROOT is set in environment
ifdef VCPKG_ROOT
    VCPKG_ROOT_PATH := $(VCPKG_ROOT)
else
    # Try to detect vcpkg in relative paths
    VCPKG_RELATIVE := $(wildcard ../vcpkg)
    ifeq ($(VCPKG_RELATIVE),)
        VCPKG_RELATIVE := $(wildcard ./vcpkg)
    endif
    VCPKG_ROOT_PATH := $(VCPKG_RELATIVE)
endif

ifneq ($(VCPKG_ROOT_PATH),)
    # Convert to absolute path and set toolchain
    VCPKG_ROOT_PATH := $(abspath $(VCPKG_ROOT_PATH))
    VCPKG_TOOLCHAIN_PATH := $(VCPKG_ROOT_PATH)/scripts/buildsystems/vcpkg.cmake
    export VCPKG_TOOLCHAIN_PATH
    VCPKG_ROOT := $(VCPKG_ROOT_PATH)
    export VCPKG_ROOT
    
    # Platform-specific triplet
    ifeq ($(OS),Windows_NT)
        VCPKG_TARGET_TRIPLET ?= x64-windows-static
    else
        VCPKG_TARGET_TRIPLET ?= x64-linux
    endif
    export VCPKG_TARGET_TRIPLET
    
    $(info Detected vcpkg at: $(VCPKG_ROOT_PATH))
    $(info Using triplet: $(VCPKG_TARGET_TRIPLET))
else
    $(warning vcpkg not found. Set VCPKG_ROOT or install vcpkg in ../vcpkg or ./vcpkg)
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# ====== Additional Helper Targets ======

.PHONY: help init-vcpkg check-deps test-connection-cleanup test-unicode-input

# Prove Unicode statement encoding and single-send errors without a source DB.
test-unicode-input:
	@set -eu; \
	unicode_test_binary=$$(mktemp "$${TMPDIR:-/tmp}/oracle-unicode-input.XXXXXX"); \
	trap 'rm -f "$$unicode_test_binary"' EXIT; \
	$(CXX) -std=c++17 -O2 -UNDEBUG -Wall -Wextra -Werror \
	  -Isrc/include test/odbc_unicode_input.cpp -o "$$unicode_test_binary"; \
	"$$unicode_test_binary"; \
	printf 'Oracle ODBC Unicode input regression PASS\n'

# Native ODBC cleanup regression with mock handles; opens no database.
# Discard unused DuckDB header functions so this test needs neither libduckdb
# nor libodbc. Keep assertions enabled even when the extension is Release.
test-connection-cleanup:
	@set -eu; \
	cleanup_test_binary=$$(mktemp "$${TMPDIR:-/tmp}/oracle-connection-cleanup.XXXXXX"); \
	trap 'rm -f "$$cleanup_test_binary"' EXIT; \
	$(CXX) -std=c++17 -O2 -UNDEBUG -ffunction-sections -fdata-sections -Wl,--gc-sections \
	  -Isrc/include -Iduckdb/src/include test/odbc_connection_cleanup.cpp -o "$$cleanup_test_binary"; \
	"$$cleanup_test_binary"; \
	printf 'Oracle ODBC connection cleanup regression PASS\n'

help:
	@echo "oracle_scanner - DuckDB Oracle scanner extension"
	@echo ""
	@echo "Available targets:"
	@echo "  make release       - Build release version"
	@echo "  make debug         - Build debug version"
	@echo "  make test          - Run tests"
	@echo "  make test-connection-cleanup - Run native cleanup regression (no source DB)"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make format        - Format source code"
	@echo "  make init-vcpkg    - Initialize vcpkg installation"
	@echo "  make check-deps    - Check dependencies"
	@echo ""
	@echo "Environment variables:"
	@echo "  VCPKG_ROOT            - vcpkg installation path (current: $(if $(VCPKG_ROOT),$(VCPKG_ROOT),not set))"
	@echo "  VCPKG_TARGET_TRIPLET  - Target triplet (current: $(if $(VCPKG_TARGET_TRIPLET),$(VCPKG_TARGET_TRIPLET),not set))"
	@echo ""
	@echo "Examples:"
	@echo "  make release                    # Build with auto-detected vcpkg"
	@echo "  VCPKG_ROOT=~/vcpkg make release # Build with specific vcpkg"
	@echo "  GEN=ninja make release          # Build with Ninja generator"

init-vcpkg:
	@if [ -z "$(VCPKG_ROOT)" ]; then \
		echo "Installing vcpkg to ../vcpkg..."; \
		git clone https://github.com/microsoft/vcpkg.git ../vcpkg; \
		cd ../vcpkg; \
		if [ "$(OS)" = "Windows_NT" ]; then \
			./bootstrap-vcpkg.bat; \
		else \
			./bootstrap-vcpkg.sh; \
		fi; \
		echo ""; \
		echo "vcpkg installed successfully at $(abspath ../vcpkg)"; \
		echo "Now you can run: make release"; \
	else \
		echo "vcpkg already installed at $(VCPKG_ROOT)"; \
	fi

check-deps:
	@echo "Checking dependencies..."
	@echo ""
	@echo "vcpkg: $(if $(VCPKG_ROOT),Found ($(VCPKG_ROOT)),Not found)"
	@echo "ODBC: $(if $(OS),Windows_NT,System SDK,vcpkg unixodbc)"
	@echo ""
	@echo "Build configuration:"
	@echo "  Platform: $(if $(OS),Windows_NT,Windows,Linux)"
	@echo "  Triplet: $(if $(VCPKG_TARGET_TRIPLET),$(VCPKG_TARGET_TRIPLET),not set)"
	@echo ""
	@echo "Ready to build: $(if $(VCPKG_ROOT),Yes,No - run 'make init-vcpkg' first)"
