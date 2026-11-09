// Exercise the production transport with mock ODBC handles, without a source DB.
#include "odbc_connection.hpp"
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

using namespace duckdb;
namespace {
int direct_calls = 0, prepare_calls = 0, parameter_calls = 0, bind_calls = 0, execute_calls = 0;
int allocated = 0, freed = 0;
bool fail_direct = false, fail_attributes = false, return_rows = false;
int next_row = 0;
void RequireAt(bool ok, int line) {
	if (!ok)
		throw std::runtime_error("ODBC query metric contract failed at line " + std::to_string(line));
}
} // namespace
#define Require(ok) RequireAt((ok), __LINE__)

extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER, SQLINTEGER *) {
	if (attr == SQL_ATTR_CONNECTION_DEAD)
		*static_cast<SQLINTEGER *>(value) = SQL_CD_FALSE;
	else
		*static_cast<SQLULEN *>(value) = SQL_AUTOCOMMIT_ON;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE, SQLHANDLE *value) {
	Require(type == SQL_HANDLE_STMT);
	*value = reinterpret_cast<SQLHANDLE>(static_cast<uintptr_t>(++allocated));
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE) {
	if (type == SQL_HANDLE_STMT)
		++freed;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT) {
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLDisconnect(SQLHDBC) {
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT key, SQLPOINTER value, SQLSMALLINT, SQLSMALLINT *length) {
	if (fail_attributes)
		return SQL_ERROR;
	const char *text = key == SQL_DRIVER_NAME ? "mock_oracle.so" : "23.1";
	std::strcpy(static_cast<char *>(value), text);
	*length = static_cast<SQLSMALLINT>(std::strlen(text));
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER key, SQLPOINTER value, SQLINTEGER, SQLINTEGER *) {
	if (fail_attributes)
		return SQL_ERROR;
	*static_cast<SQLULEN *>(value) = key == SQL_ATTR_ROW_ARRAY_SIZE  ? 1
	                                 : key == SQL_ATTR_QUERY_TIMEOUT ? 5
	                                                                 : SQL_CURSOR_FORWARD_ONLY;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLDescribeColW(SQLHSTMT, SQLUSMALLINT, SQLWCHAR *name, SQLSMALLINT, SQLSMALLINT *length,
                                             SQLSMALLINT *type, SQLULEN *size, SQLSMALLINT *digits,
                                             SQLSMALLINT *nullable) {
	name[0] = 'i';
	name[1] = 0;
	*length = 1;
	*type = SQL_INTEGER;
	*size = 10;
	*digits = 0;
	*nullable = SQL_NO_NULLS;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLColAttribute(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER value, SQLSMALLINT,
                                             SQLSMALLINT *length, SQLLEN *) {
	std::strcpy(static_cast<char *>(value), "INT");
	*length = 3;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT) {
	if (next_row >= 3)
		return SQL_NO_DATA;
	std::this_thread::sleep_for(std::chrono::milliseconds(next_row == 0 ? 2 : next_row == 1 ? 12 : 105));
	++next_row;
	return next_row == 2 ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT type, SQLPOINTER value, SQLLEN,
                                        SQLLEN *length) {
	Require(type == SQL_C_SLONG);
	*static_cast<SQLINTEGER *>(value) = next_row;
	*length = sizeof(SQLINTEGER);
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER) {
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT, SQLWCHAR *, SQLINTEGER) {
	++direct_calls;
	std::this_thread::sleep_for(std::chrono::milliseconds(2));
	return fail_direct ? SQL_ERROR : SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLPrepareW(SQLHSTMT, SQLWCHAR *, SQLINTEGER) {
	++prepare_calls;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLNumParams(SQLHSTMT, SQLSMALLINT *value) {
	++parameter_calls;
	*value = 1;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT, SQLSMALLINT, SQLULEN,
                                              SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *) {
	++bind_calls;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLExecute(SQLHSTMT) {
	++execute_calls;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLRowCount(SQLHSTMT, SQLLEN *value) {
	*value = return_rows ? -1 : 0;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *value) {
	*value = return_rows ? 1 : 0;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT record, SQLCHAR *state, SQLINTEGER *code,
                                           SQLCHAR *message, SQLSMALLINT, SQLSMALLINT *length) {
	if (record != 1)
		return SQL_NO_DATA;
	std::memcpy(state, "HY000", 6);
	*code = 42;
	const char *text = "direct failure sentinel";
	std::strcpy(reinterpret_cast<char *>(message), text);
	*length = static_cast<SQLSMALLINT>(std::strlen(text));
	return SQL_SUCCESS;
}

int main() {
	auto owned = make_shared_ptr<OwnedODBCConnection>(reinterpret_cast<SQLHDBC>(1));
	auto connection = make_shared_ptr<ODBCConnection>(owned, "Driver=mock;FBS=65536", ODBCTypeConfig {});
	ODBCQueryMetrics first, second, failed;
	connection->QueryWithTimeout("SELECT 1", ODBCResultStreaming::FORCE_MATERIALIZATION, 5, &first);
	connection->QueryWithTimeout("SELECT 1", ODBCResultStreaming::FORCE_MATERIALIZATION, 5, &second);
	Require(first.direct_calls == 1 && first.direct_us > 0 && first.prepare_calls == 0 && first.parameter_calls == 0 &&
	        first.bind_calls == 0 && first.execute_calls == 0);
	Require(first.connection_id == owned->diagnostic_id && second.connection_id == first.connection_id &&
	        second.connection_query_sequence == first.connection_query_sequence + 1);
	fail_direct = true;
	bool rejected = false;
	try {
		connection->QueryWithTimeout("SELECT broken", ODBCResultStreaming::FORCE_MATERIALIZATION, 5, &failed);
	} catch (const std::exception &e) {
		rejected = string(e.what()).find("direct failure sentinel") != string::npos;
	}
	Require(rejected && failed.direct_calls == 1 && failed.direct_us > 0 && failed.prepare_calls == 0 &&
	        failed.connection_query_sequence == second.connection_query_sequence + 1);
	fail_direct = false;
	// A failed call releases the connection, and parameters retain their existing
	// prepare/bind/execute route. Diagnostics must never add a second execution.
	connection->Query("SELECT ?", vector<Value> {Value::INTEGER(7)}, ODBCResultStreaming::FORCE_MATERIALIZATION);
	Require(direct_calls == 3 && prepare_calls == 1 && parameter_calls == 1 && bind_calls == 1 && execute_calls == 1 &&
	        allocated == freed);
	Require(first.driver_name == "mock_oracle.so" && first.driver_version == "23.1" && first.row_array_size == 1 &&
	        first.query_timeout == 5 && first.configured_fetch_buffer == "65536" &&
	        first.statement_scope_us >= first.direct_us);
	return_rows = true;
	ODBCQueryMetrics fetched;
	auto rows = connection->QueryWithTimeout("SELECT i", ODBCResultStreaming::FORCE_MATERIALIZATION, 5, &fetched);
	Require(fetched.fetch_calls == 4 && fetched.rows == 3 && fetched.get_data_calls == 3 && fetched.fields == 1 &&
	        fetched.unbound_fields == 1 && fetched.long_fields == 0 && fetched.lob_fields == 0 &&
	        fetched.fetch_info == 1);
	Require(fetched.first_fetch_us >= 2000 && fetched.max_fetch_us >= 100000 && fetched.fetch_ge_1ms >= 3 &&
	        fetched.fetch_ge_10ms >= 2 && fetched.fetch_ge_100ms >= 1 &&
	        fetched.statement_scope_us >= fetched.fetch_us);
	for (int expected = 1; expected <= 3; expected++) {
		Require(rows->Next() && rows->GetInt32(0) == expected);
	}
	Require(!rows->Next() && allocated == freed);
	// Unsupported diagnostics must remain explicitly unknown and cannot alter
	// result rows, execute twice, leak a statement or suppress a source error.
	fail_attributes = true;
	next_row = 0;
	ODBCQueryMetrics unsupported;
	rows = connection->QueryWithTimeout("SELECT i", ODBCResultStreaming::FORCE_MATERIALIZATION, 5, &unsupported);
	Require(unsupported.driver_name_rc == SQL_ERROR && unsupported.driver_name.empty() &&
	        unsupported.row_array_rc == SQL_ERROR && unsupported.row_array_size == -1 &&
	        unsupported.cursor_type == -1 && unsupported.query_timeout == -1 && unsupported.rows == 3);
	Require(direct_calls == 5 && allocated == freed);
	OwnedODBCConnection another;
	Require(another.diagnostic_id != owned->diagnostic_id);
	std::cout << "PASS: direct versus prepared route, failure timing, original error, connection identity and "
	             "statement cleanup, fetch distribution, unsupported diagnostics and unchanged rows\n";
}
