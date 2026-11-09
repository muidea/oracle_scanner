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
bool fail_direct = false;
void Require(bool ok) {
	if (!ok)
		throw std::runtime_error("ODBC query metric contract failed");
}
} // namespace

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
	*value = 0;
	return SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *value) {
	*value = 0;
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
	auto connection = make_shared_ptr<ODBCConnection>(owned, "mock", ODBCTypeConfig {});
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
	OwnedODBCConnection another;
	Require(another.diagnostic_id != owned->diagnostic_id);
	std::cout << "PASS: direct versus prepared route, failure timing, original error, connection identity and "
	             "statement cleanup\n";
}
