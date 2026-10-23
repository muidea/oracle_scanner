// Standalone native cleanup regression; no source connection is opened.
// Build with the extension and DuckDB include directories, without -lodbc.
#include "odbc_connection.hpp"
#include <cassert>
#include <vector>

namespace {
std::vector<int> events;
SQLULEN mode = SQL_AUTOCOMMIT_OFF;
SQLRETURN mode_result = SQL_SUCCESS;
SQLRETURN rollback_result = SQL_SUCCESS;
bool transaction = true;
constexpr int READ_MODE = 1, ROLLBACK = 2, DISCONNECT = 3, FREE_DBC = 4, FREE_ENV = 5;
void Reset(SQLULEN next_mode = SQL_AUTOCOMMIT_OFF) {
	events.clear();
	mode = next_mode;
	mode_result = rollback_result = SQL_SUCCESS;
	transaction = mode == SQL_AUTOCOMMIT_OFF;
}
void Close() {
	duckdb::OwnedODBCConnection connection(reinterpret_cast<SQLHDBC>(1), reinterpret_cast<SQLHENV>(2));
}
} // namespace
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER, SQLINTEGER *) {
	assert(attr == SQL_ATTR_AUTOCOMMIT);
	events.push_back(READ_MODE);
	*static_cast<SQLULEN *>(value) = mode;
	return mode_result;
}
extern "C" SQLRETURN SQL_API SQLEndTran(SQLSMALLINT type, SQLHANDLE, SQLSMALLINT completion) {
	assert(type == SQL_HANDLE_DBC && completion == SQL_ROLLBACK);
	events.push_back(ROLLBACK);
	if (SQL_SUCCEEDED(rollback_result))
		transaction = false;
	return rollback_result;
}
extern "C" SQLRETURN SQL_API SQLDisconnect(SQLHDBC) {
	events.push_back(DISCONNECT);
	return transaction ? SQL_ERROR : SQL_SUCCESS;
}
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE) {
	events.push_back(type == SQL_HANDLE_DBC ? FREE_DBC : FREE_ENV);
	return SQL_SUCCESS;
}
int main() {
	Reset();
	Close();
	assert(!transaction && events == std::vector<int>({READ_MODE, ROLLBACK, DISCONNECT, FREE_DBC, FREE_ENV}));
	Reset(SQL_AUTOCOMMIT_ON);
	Close();
	assert(!transaction && events == std::vector<int>({READ_MODE, DISCONNECT, FREE_DBC, FREE_ENV}));
	Reset();
	mode_result = SQL_ERROR;
	Close();
	assert(!transaction && events == std::vector<int>({READ_MODE, ROLLBACK, DISCONNECT, FREE_DBC, FREE_ENV}));
	Reset();
	rollback_result = SQL_ERROR;
	Close();
	assert(events == std::vector<int>({READ_MODE, ROLLBACK, DISCONNECT, FREE_DBC, FREE_ENV}));
	Reset();
	{ duckdb::OwnedODBCConnection partial(nullptr, reinterpret_cast<SQLHENV>(2)); }
	assert(events == std::vector<int>({FREE_ENV}));
	Reset();
	{ duckdb::OwnedODBCConnection empty; }
	assert(events.empty());
}
