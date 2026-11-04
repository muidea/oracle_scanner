// Standalone Unicode transport regression. ODBC calls are mocked; no source
// connection, process-global setlocale, or Oracle client is required.
#include "oracle_odbc_unicode.hpp"
#include <cassert>

namespace {
std::u16string observed;
int direct_calls = 0, prepare_calls = 0;
SQLRETURN next_result = SQL_SUCCESS;
SQLRETURN Observe(SQLHSTMT stmt, SQLWCHAR *text, SQLINTEGER length) {
	assert(stmt == reinterpret_cast<SQLHSTMT>(1));
	assert(length >= 0 && text[length] == 0);
	observed.assign(text, text + length);
	return next_result;
}
} // namespace

extern "C" SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT stmt, SQLWCHAR *text, SQLINTEGER length) {
	direct_calls++;
	return Observe(stmt, text, length);
}
extern "C" SQLRETURN SQL_API SQLPrepareW(SQLHSTMT stmt, SQLWCHAR *text, SQLINTEGER length) {
	prepare_calls++;
	return Observe(stmt, text, length);
}

int main() {
	auto stmt = reinterpret_cast<SQLHSTMT>(1);
	const std::string sql = "SELECT '指标😀a''\\尾' FROM dual";
	assert(duckdb::OracleODBCExecDirectUTF8(stmt, sql) == SQL_SUCCESS);
	assert(observed == u"SELECT '指标😀a''\\尾' FROM dual" && direct_calls == 1);
	assert(duckdb::OracleODBCPrepareUTF8(stmt, sql) == SQL_SUCCESS);
	assert(observed == u"SELECT '指标😀a''\\尾' FROM dual" && prepare_calls == 1);
	next_result = SQL_ERROR;
	assert(duckdb::OracleODBCExecDirectUTF8(stmt, sql) == SQL_ERROR && direct_calls == 2);
	assert(duckdb::OracleODBCPrepareUTF8(stmt, sql) == SQL_ERROR && prepare_calls == 2);
	next_result = SQL_SUCCESS_WITH_INFO;
	assert(duckdb::OracleODBCExecDirectUTF8(stmt, "SELECT 1 FROM dual") == SQL_SUCCESS_WITH_INFO);
	assert(observed == u"SELECT 1 FROM dual" && direct_calls == 3);
	for (auto &invalid : std::vector<std::string> {"\xC0\xAF", "\xED\xA0\x80", "\xF4\x90\x80\x80", "\xE6\x8C",
	                                               std::string("SELECT 1\0SELECT 2", 17)}) {
		bool rejected = false;
		try {
			duckdb::OracleODBCExecDirectUTF8(stmt, invalid);
		} catch (const std::exception &) {
			rejected = true;
		}
		assert(rejected && direct_calls == 3);
	}
	assert(duckdb::OracleODBCUTF8ToWide("") == std::vector<SQLWCHAR> {0});
	assert(duckdb::OracleODBCUTF8ToWide("😀") == std::vector<SQLWCHAR>({0xD83D, 0xDE00, 0}));
	// Values use explicit byte lengths when bound, and retain embedded NULs.
	assert(duckdb::OracleODBCUTF8ToWide(std::string("a\0b", 3)) == std::vector<SQLWCHAR>({'a', 0, 'b', 0}));
}
