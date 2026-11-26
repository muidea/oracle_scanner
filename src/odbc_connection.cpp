#include "query_odbc_source_error.hpp"
#include <chrono>
#include "odbc_connection.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "odbc_parameter.hpp"
#include "odbc_types.hpp"
#include "oracle_odbc_unicode.hpp"

#include <cstdlib>
#include <mutex>
namespace duckdb {

static bool debug_odbc_print_queries = false;

// Statement-local timers include throwing calls and never outlive the caller.
class ODBCExecuteTimer {
public:
	explicit ODBCExecuteTimer(int64_t *value_p) : value(value_p) {
		if (value)
			started = std::chrono::steady_clock::now();
	}
	~ODBCExecuteTimer() {
		if (value)
			*value += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started)
			              .count();
	}

private:
	int64_t *value;
	std::chrono::steady_clock::time_point started;
};

ODBCConnection::ODBCConnection(shared_ptr<OwnedODBCConnection> connection_p, const std::string &dsn_p,
                               ODBCTypeConfig type_config_p)
    : connection(std::move(connection_p)), dsn(std::move(dsn_p)), type_config(std::move(type_config_p)) {
}

ODBCConnection::~ODBCConnection() {
	Close();
}

ODBCConnection::ODBCConnection(ODBCConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	std::swap(type_config, other.type_config);
}

ODBCConnection &ODBCConnection::operator=(ODBCConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	std::swap(type_config, other.type_config);
	return *this;
}

shared_ptr<ODBCConnection> ODBCConnection::Open(ODBCTypeConfig type_config, const string &connection_string) {
#ifndef _WIN32
	static std::once_flag oracle_client_encoding_once;
	std::call_once(oracle_client_encoding_once, []() {
		// Oracle's SQLGetData path falls back to the process ANSI character set
		// when NLS_LANG is absent, replacing non-ASCII identifiers and values
		// with '?'. Respect an explicit deployment setting, otherwise request
		// UTF-8 before the Oracle client is initialized by the first connection.
		if (!std::getenv("NLS_LANG")) {
			setenv("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8", 0);
		}
	});
#endif
	SQLHENV env = SQL_NULL_HENV;
	SQLHDBC dbc = ODBCUtils::Connect(connection_string, env);
	auto owned_connection = make_shared_ptr<OwnedODBCConnection>(dbc, env);
	{
		SQLHSTMT tz_stmt = SQL_NULL_HSTMT;
		if (SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &tz_stmt)) && tz_stmt != SQL_NULL_HSTMT) {
			(void)OracleODBCExecDirectUTF8(
			    tz_stmt, "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'");
			SQLFreeHandle(SQL_HANDLE_STMT, tz_stmt);
		}
	}
	return make_shared_ptr<ODBCConnection>(std::move(owned_connection), connection_string, std::move(type_config));
}

idx_t ODBCConnection::ODBCExecute(SQLHSTMT stmt, const string &query, vector<Value> params, bool streaming,
                                  uint32_t timeout_seconds, ODBCQueryMetrics *metrics) {
	if (ODBCConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	lock_guard<std::recursive_mutex> l(query_lock);

	SQLRETURN retcode;

	if (!stmt) { // basic interface
		auto hdbc = GetConn();
		SQLHSTMT hstmt = SQL_NULL_HSTMT;
		retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_DBC, hdbc, GetConn(), "source_prepare", query);
		}

		try {
			retcode = OracleODBCExecDirectUTF8(hstmt, query);
			if (!SQL_SUCCEEDED(retcode)) {
				ThrowQueryODBCError(SQL_HANDLE_STMT, hstmt, GetConn(), "source_execute", query);
			}
		} catch (const std::exception &) {
			// 确保语句句柄被清理
			if (hstmt != SQL_NULL_HSTMT) {
				SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
			}
			throw;
		}

		SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		return 0;
	}

	// prepared statement interface
	// 对于预编译语句接口，调用者负责清理语句句柄
	// 我们只需要确保在失败时抛出适当的异常

	if (timeout_seconds > 0) {
		retcode = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT,
		                         reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(timeout_seconds)), 0);
		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), "source_prepare", query);
		}
	}

	if (params.empty()) {
		ODBCExecuteTimer timer(metrics ? &metrics->direct_us : nullptr);
		if (metrics)
			metrics->direct_calls++;
		retcode = OracleODBCExecDirectUTF8(stmt, query);
	} else {
		ODBCExecuteTimer timer(metrics ? &metrics->prepare_us : nullptr);
		if (metrics)
			metrics->prepare_calls++;
		retcode = OracleODBCPrepareUTF8(stmt, query);
	}
	if (!SQL_SUCCEEDED(retcode)) {
		ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), params.empty() ? "source_execute" : "source_prepare",
		                    query);
	}

	vector<ODBCParameter> odbc_params;
	if (params.empty()) {
		SQLLEN affected_rows = 0;
		ODBCExecuteTimer timer(metrics ? &metrics->result_info_us : nullptr);
		retcode = SQLRowCount(stmt, &affected_rows);
		if (!SQL_SUCCEEDED(retcode)) {
			affected_rows = -1;
		}
		SQLSMALLINT num_cols = 0;
		SQLRETURN col_ret = SQLNumResultCols(stmt, &num_cols);
		if (SQL_SUCCEEDED(col_ret) && num_cols > 0) {
			affected_rows = -1;
		}
		return affected_rows >= 0 ? static_cast<idx_t>(affected_rows) : static_cast<idx_t>(-1);
	}
	if (params.size() > 0) {
		SQLSMALLINT param_count;
		{
			ODBCExecuteTimer timer(metrics ? &metrics->parameter_us : nullptr);
			if (metrics)
				metrics->parameter_calls++;
			retcode = SQLNumParams(stmt, &param_count);
		}
		if (!SQL_SUCCEEDED(retcode)) {
			ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), "source_describe", query);
		}

		if (param_count != params.size()) {
			throw IOException(
			    "Incorrect query parameters count specified, expected: %d, actual: %zu, ODBC query \"%s\"\n",
			    param_count, params.size(), query.c_str());
		}

		// ODBC retains pointers to parameter lengths as well as their data.
		// Preserve the existing reservation before binding any parameter.
		odbc_params.reserve(params.size());
		for (Value &dp : params) {
			ODBCParameter mp(query, std::move(dp));
			odbc_params.emplace_back(std::move(mp));
			ODBCParameter &mp_ref = odbc_params.back();
			SQLRETURN bind_ret;
			{
				ODBCExecuteTimer timer(metrics ? &metrics->bind_us : nullptr);
				if (metrics)
					metrics->bind_calls++;
				bind_ret = mp_ref.BindParameter(stmt, odbc_params.size());
			}
			if (!SQL_SUCCEEDED(bind_ret)) {
				ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), "source_bind", query);
			}
		}
	}

	{
		ODBCExecuteTimer timer(metrics ? &metrics->execute_us : nullptr);
		if (metrics)
			metrics->execute_calls++;
		retcode = SQLExecute(stmt);
	}
	if (!SQL_SUCCEEDED(retcode)) {
		ThrowQueryODBCError(SQL_HANDLE_STMT, stmt, GetConn(), "source_execute", query);
	}

	ODBCExecuteTimer result_info_timer(metrics ? &metrics->result_info_us : nullptr);
	SQLLEN affected_rows = 0;
	retcode = SQLRowCount(stmt, &affected_rows);
	if (!SQL_SUCCEEDED(retcode)) {
		// For SELECT queries, SQLRowCount should return -1
		affected_rows = -1;
	}

	// For ODBC, we need to be more careful:
	// - SELECT queries should return -1 (no affected rows)
	// - DML queries (INSERT, UPDATE, DELETE) return >= 0
	// But some ODBC drivers might return 0 for SELECT queries, so we need to check column count
	SQLSMALLINT num_cols = 0;
	SQLRETURN col_ret = SQLNumResultCols(stmt, &num_cols);
	if (SQL_SUCCEEDED(col_ret) && num_cols > 0) {
		// This is a SELECT query, force affected_rows to -1
		affected_rows = -1;
	}

	// Return -1 for SELECT queries (no affected rows), otherwise return the actual count
	return affected_rows >= 0 ? static_cast<idx_t>(affected_rows) : static_cast<idx_t>(-1);
}

// Observe only standard ODBC attributes; do not set driver options or guess
// Oracle-private attribute numbers. A DSN's FBS is configuration evidence,
// not proof of the driver's effective internal prefetch window.
static void ObserveMetadataDriver(SQLHDBC dbc, SQLHSTMT stmt, const string &dsn, ODBCQueryMetrics *metrics) noexcept {
	if (!metrics)
		return;
	ODBCExecuteTimer timer(&metrics->diagnostic_us);
	try {
		auto info = [&](SQLUSMALLINT key, string &value, int &rc) {
			SQLCHAR buffer[256] = {};
			SQLSMALLINT length = 0;
			rc = SQLGetInfo(dbc, key, buffer, sizeof(buffer), &length);
			if (SQL_SUCCEEDED(rc) && length >= 0)
				value.assign(reinterpret_cast<const char *>(buffer),
				             std::min<size_t>(static_cast<size_t>(length), sizeof(buffer) - 1));
		};
		info(SQL_DRIVER_NAME, metrics->driver_name, metrics->driver_name_rc);
		info(SQL_DRIVER_VER, metrics->driver_version, metrics->driver_version_rc);
		auto attr = [&](SQLINTEGER key, int64_t &value, int &rc) {
			SQLULEN actual = 0;
			rc = SQLGetStmtAttr(stmt, key, &actual, sizeof(actual), nullptr);
			if (SQL_SUCCEEDED(rc))
				value = static_cast<int64_t>(actual);
		};
		attr(SQL_ATTR_ROW_ARRAY_SIZE, metrics->row_array_size, metrics->row_array_rc);
		attr(SQL_ATTR_CURSOR_TYPE, metrics->cursor_type, metrics->cursor_type_rc);
		attr(SQL_ATTR_QUERY_TIMEOUT, metrics->query_timeout, metrics->query_timeout_rc);
		ODBCConnectionParameters config;
		unordered_set<string> options;
		std::tie(config, options) = ODBCUtils::ParseConnectionParameters(dsn);
		for (auto &param : config.extra_params) {
			auto equal = param.find('=');
			if (equal == string::npos)
				continue;
			auto key = StringUtil::Lower(param.substr(0, equal));
			auto value = param.substr(equal + 1);
			if ((key == "fbs" || key == "fetchbuffersize") && !value.empty() &&
			    value.find_first_not_of("0123456789") == string::npos)
				metrics->configured_fetch_buffer = value;
		}
	} catch (...) { /* Observability must not change source execution. */
	}
}

unique_ptr<ODBCResult> ODBCConnection::QueryInternal(const string &query, vector<Value> params,
                                                     ODBCResultStreaming streaming,
                                                     ODBCConnectorInterface con_interface, uint32_t timeout_seconds,
                                                     ODBCQueryMetrics *metrics) {

	// 检查连接是否繁忙
	bool expected = false;
	if (!connection_busy.compare_exchange_strong(expected, true)) {
		throw IOException("ODBC connection is busy executing another query \"%s\"\n", query.c_str());
	}

	// 检查连接健康状态
	if (!CheckConnectionHealth()) {
		ReleaseConnection();
		throw IOException("ODBC connection is not healthy for query \"%s\"\n", query.c_str());
	}

	// 注意：我们不使用ConnectionBusyGuard，因为连接需要保持繁忙直到ODBCResult被销毁
	// 连接繁忙状态由ODBCResult在析构时释放

	// 使用递归互斥锁保护连接操作，防止多个查询同时使用同一个连接
	// 使用 recursive_mutex 允许同一线程重入，避免死锁
	auto lock_started = metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	lock_guard<std::recursive_mutex> l(query_lock);
	if (metrics)
		metrics->query_lock_wait_us +=
		    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - lock_started)
		        .count();

	auto hdbc = GetConn();
	auto query_sequence = ++connection->query_sequence;
	if (metrics) {
		metrics->connection_id = connection->diagnostic_id;
		metrics->connection_query_sequence = query_sequence;
	}

	// 检查连接是否有效
	if (!hdbc) {
		ReleaseConnection();
		throw IOException("ODBC connection is not open for query \"%s\"\n", query.c_str());
	}

	// 检查连接状态
	SQLINTEGER conn_attr = SQL_CD_FALSE;
	SQLRETURN status_ret = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, &conn_attr, 0, nullptr);
	if (SQL_SUCCEEDED(status_ret) && conn_attr == SQL_CD_TRUE) {
		ReleaseConnection();
		throw IOException("ODBC connection is dead for query \"%s\"\n", query.c_str());
	}

	bool result_streaming = streaming == ODBCResultStreaming::ALLOW_STREAMING;
	bool basic_interface = con_interface == ODBCConnectorInterface::BASIC;

	if (basic_interface) {
		try {
			ODBCExecute(nullptr, query, params, result_streaming, timeout_seconds);
		} catch (...) {
			ReleaseConnection();
			throw;
		}
		// BASIC接口不创建ODBCResult，需要立即释放连接忙状态
		ReleaseConnection();
		return unique_ptr<ODBCResult>(nullptr);
	}

	SQLHSTMT hstmt = SQL_NULL_HSTMT;
	ODBCExecuteTimer statement_timer(metrics ? &metrics->statement_scope_us : nullptr);
	auto alloc_started = metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
	if (metrics)
		metrics->statement_alloc_us +=
		    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - alloc_started)
		        .count();
	if (!SQL_SUCCEEDED(retcode)) {
		// 释放连接忙状态
		ReleaseConnection();
		ThrowQueryODBCError(SQL_HANDLE_DBC, hdbc, GetConn(), "source_prepare", query);
	}

	idx_t affected_rows = 0;
	auto execute_started = metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	try {
		affected_rows = ODBCExecute(hstmt, query, params, result_streaming, timeout_seconds, metrics);
		if (metrics)
			metrics->execute_ms += std::chrono::duration_cast<std::chrono::milliseconds>(
			                           std::chrono::steady_clock::now() - execute_started)
			                           .count();
	} catch (const std::exception &) {
		if (metrics)
			metrics->execute_ms += std::chrono::duration_cast<std::chrono::milliseconds>(
			                           std::chrono::steady_clock::now() - execute_started)
			                           .count();
		// 执行失败，清理语句句柄
		if (hstmt != SQL_NULL_HSTMT) {
			SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		}
		// 释放连接忙状态
		ReleaseConnection();
		throw;
	}

	ObserveMetadataDriver(hdbc, hstmt, dsn, metrics);

	// Get number of result columns
	SQLSMALLINT num_cols = 0;
	retcode = SQLNumResultCols(hstmt, &num_cols);
	if (!SQL_SUCCEEDED(retcode)) {
		num_cols = 0;
	}

	// For SELECT queries, affected_rows will be -1, but we still need to process the result set
	if (affected_rows == static_cast<idx_t>(-1) && num_cols == 0) {
		// Try to get column count again - some drivers might not set it immediately
		retcode = SQLNumResultCols(hstmt, &num_cols);
	}

	auto materialize_started = metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	try {
		auto result = make_uniq<ODBCResult>(query, ODBCStatementPtr(hstmt, ODBCStatementDelete), type_config,
		                                    affected_rows, num_cols, shared_from_this(), streaming, metrics);
		if (metrics)
			metrics->materialize_ms += std::chrono::duration_cast<std::chrono::milliseconds>(
			                               std::chrono::steady_clock::now() - materialize_started)
			                               .count();
		return result;
	} catch (const std::exception &e) {
		if (metrics)
			metrics->materialize_ms += std::chrono::duration_cast<std::chrono::milliseconds>(
			                               std::chrono::steady_clock::now() - materialize_started)
			                               .count();
		// ODBCStatementPtr already owns and releases hstmt if construction
		// fails. Freeing the raw handle here would be a double free.
		// 释放连接忙状态
		ReleaseConnection();
		throw;
	}
}

unique_ptr<ODBCResult> ODBCConnection::Query(const string &query, ODBCResultStreaming streaming) {
	return Query(query, vector<Value>(), streaming);
}

unique_ptr<ODBCResult> ODBCConnection::Query(const string &query, vector<Value> params, ODBCResultStreaming streaming) {
	return QueryInternal(query, params, streaming, ODBCConnectorInterface::PREPARED_STATEMENT);
}

unique_ptr<ODBCResult> ODBCConnection::QueryWithTimeout(const string &query, ODBCResultStreaming streaming,
                                                        uint32_t timeout_seconds, ODBCQueryMetrics *metrics) {
	if (timeout_seconds == 0) {
		throw InvalidInputException("ODBC query timeout must be greater than zero");
	}
	return QueryInternal(query, vector<Value>(), streaming, ODBCConnectorInterface::PREPARED_STATEMENT, timeout_seconds,
	                     metrics);
}

void ODBCConnection::Execute(const string &query) {
	Execute(query, vector<Value>());
}

void ODBCConnection::Execute(const string &query, vector<Value> params) {
	ODBCConnectorInterface con_interface =
	    params.size() > 0 ? ODBCConnectorInterface::PREPARED_STATEMENT : ODBCConnectorInterface::BASIC;
	QueryInternal(query, std::move(params), ODBCResultStreaming::FORCE_MATERIALIZATION, con_interface);
}

bool ODBCConnection::IsOpen() {
	return connection.get();
}

void ODBCConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	connection = nullptr;
}

bool ODBCConnection::CheckConnectionHealth() {
	if (!IsOpen()) {
		return false;
	}

	auto hdbc = GetConn();
	SQLINTEGER conn_attr = SQL_CD_FALSE;
	SQLRETURN retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, &conn_attr, 0, nullptr);

	if (!SQL_SUCCEEDED(retcode)) {
		// SQL_ATTR_CONNECTION_DEAD is optional in some Oracle ODBC drivers.
		return true;
	}

	return (conn_attr == SQL_CD_FALSE);
}

void ODBCConnection::DebugSetPrintQueries(bool print) {
	debug_odbc_print_queries = print;
}

bool ODBCConnection::DebugPrintQueries() {
	return debug_odbc_print_queries;
}

void ODBCConnection::ReleaseConnection() {
	// 使用原子操作确保幂等性和线程安全
	bool expected = true;
	connection_busy.compare_exchange_strong(expected, false);
}

} // namespace duckdb
