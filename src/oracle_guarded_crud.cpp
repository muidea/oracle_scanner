//===----------------------------------------------------------------------===//
//                         DuckDB
//
// oracle_guarded_crud.cpp
//
// Restricted Oracle mutations on an isolated scanner-owned session.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "guarded_odbc_source_error.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "storage/odbc_catalog.hpp"
#include "oracle_odbc_unicode.hpp"

#include <atomic>
#include <cctype>
#include <cstdlib>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>

namespace duckdb {
namespace {
static void Reject() {
	throw GuardedODBCLocalRejection("guarded Oracle operation rejected");
}
static string JSON(const string &s) {
	string out = "\"";
	for (auto c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(c) < 32)
				Reject();
			out += c;
		}
	}
	return out + "\"";
}
static bool SimpleName(const string &s) {
	if (s.empty() || s.size() > 128)
		return false;
	for (auto c : s)
		if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '_' && c != '$' &&
		    c != '#')
			return false;
	return true;
}
static string Identifier(const string &s) {
	return ODBCUtils::WriteIdentifier(s);
}
static string Literal(const string &s) {
	return ODBCUtils::WriteLiteral(s);
}
static string UnwrapBalancedOuterParens(const string &raw) {
	string s = raw;
	StringUtil::Trim(s);
	while (s.size() >= 2 && s.front() == '(' && s.back() == ')') {
		bool in_str = false;
		int depth = 0;
		bool whole_wrapped = true;
		for (idx_t i = 0; i < s.size(); i++) {
			char c = s[i];
			if (in_str) {
				if (c == '\'') {
					if (i + 1 < s.size() && s[i + 1] == '\'')
						i++;
					else
						in_str = false;
				}
				continue;
			}
			if (c == '\'') {
				in_str = true;
			} else if (c == '(') {
				depth++;
			} else if (c == ')') {
				depth--;
				if (depth < 0)
					return raw;
				if (depth == 0 && i + 1 < s.size()) {
					whole_wrapped = false;
					break;
				}
			}
		}
		if (in_str || depth != 0 || !whole_wrapped)
			break;
		s = s.substr(1, s.size() - 2);
		StringUtil::Trim(s);
	}
	return s;
}
static bool IsDigitsRange(const string &s, idx_t pos, idx_t len) {
	if (pos + len > s.size())
		return false;
	for (idx_t i = 0; i < len; i++) {
		if (!std::isdigit(static_cast<unsigned char>(s[pos + i])))
			return false;
	}
	return true;
}
static bool IsSafeOracleDefault(const string &raw) {
	string s = UnwrapBalancedOuterParens(raw);
	if (s.empty() || s.size() > 1024)
		return false;
	const string upper = StringUtil::Upper(s);
	if (upper == "NULL")
		return true;
	// Exact Oracle string literal ('...' or N'...'), rejecting empty string '' / N''
	if (s[0] == '\'' || ((s[0] == 'N' || s[0] == 'n') && s.size() >= 2 && s[1] == '\'')) {
		idx_t start = s[0] == '\'' ? 0 : 1;
		if (s.size() - start <= 2 || s.size() > 1024)
			return false;
		for (idx_t pos = start + 1; pos < s.size(); pos++) {
			if (s[pos] == '\0')
				return false;
			if (s[pos] == '\'') {
				if (pos + 1 < s.size() && s[pos + 1] == '\'') {
					pos++;
					continue;
				}
				return pos == s.size() - 1;
			}
		}
		return false;
	}
	// Exact DATE 'YYYY-MM-DD'
	if (StringUtil::StartsWith(upper, "DATE") && s.size() > 5 &&
	    (s[4] == ' ' || s[4] == '\t' || s[4] == '\r' || s[4] == '\n')) {
		string rest = s.substr(4);
		StringUtil::Trim(rest);
		// Strictly 'YYYY-MM-DD'
		if (rest.size() == 12 && rest.front() == '\'' && rest.back() == '\'' && IsDigitsRange(rest, 1, 4) &&
		    rest[5] == '-' && IsDigitsRange(rest, 6, 2) && rest[8] == '-' && IsDigitsRange(rest, 9, 2))
			return true;
		return false;
	}
	// Exact TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff]'
	if (StringUtil::StartsWith(upper, "TIMESTAMP") && s.size() > 10 &&
	    (s[9] == ' ' || s[9] == '\t' || s[9] == '\r' || s[9] == '\n')) {
		string rest = s.substr(9);
		StringUtil::Trim(rest);
		if (rest.size() >= 21 && rest.front() == '\'' && rest.back() == '\'' && IsDigitsRange(rest, 1, 4) &&
		    rest[5] == '-' && IsDigitsRange(rest, 6, 2) && rest[8] == '-' && IsDigitsRange(rest, 9, 2) &&
		    rest[11] == ' ' && IsDigitsRange(rest, 12, 2) && rest[14] == ':' && IsDigitsRange(rest, 15, 2) &&
		    rest[17] == ':' && IsDigitsRange(rest, 18, 2)) {
			if (rest.size() == 21)
				return true;
			idx_t frac = rest.size() - 22;
			if (rest[20] == '.' && frac >= 1 && frac <= 6 && IsDigitsRange(rest, 21, frac))
				return true;
		}
		return false;
	}
	// Exact signed integer/decimal literal
	idx_t pos = 0;
	if (s[pos] == '+' || s[pos] == '-') {
		pos++;
		s = UnwrapBalancedOuterParens(s.substr(pos));
		pos = 0;
	}
	if (s.empty() || s.size() > 128)
		return false;
	bool seen_dot = false;
	bool seen_digit = false;
	for (; pos < s.size(); pos++) {
		char c = s[pos];
		if (std::isdigit(static_cast<unsigned char>(c))) {
			seen_digit = true;
		} else if (c == '.' && !seen_dot) {
			seen_dot = true;
		} else {
			return false;
		}
	}
	return seen_digit;
}
// One stable statement handle lives until the cancellation watcher is joined.
// Never free/reassign a handle while another thread can call SQLCancelHandle.
class Session {
public:
	Session(ClientContext &ctx_p, SQLHDBC dbc) : ctx(ctx_p), stmt(nullptr, ODBCStatementDelete) {
		SQLHSTMT h = nullptr;
		if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &h) != SQL_SUCCESS)
			Reject();
		stmt = ODBCStatementPtr(h, ODBCStatementDelete);
		// Oracle ODBC's SQL_ATTR_QUERY_TIMEOUT uses process-wide SIGALRM on
		// Unix. It can terminate an embedded Go/race host. Keep the timeout
		// local to this session and cancel the stable handle from our watcher.
		watcher = std::thread([this] {
			std::unique_lock<std::mutex> lock(mu);
			auto end = std::chrono::steady_clock::now() + std::chrono::seconds(30);
			while (!stopped) {
				auto now = std::chrono::steady_clock::now();
				if (!cancel_sent &&
				    ((!cleanup_mode && (ctx.IsInterrupted() || now >= end)) || now >= statement_deadline)) {
					cancelled.store(true);
					cancel_sent = true;
					SQLCancelHandle(SQL_HANDLE_STMT, stmt.get());
				}
				cv.wait_for(lock, std::chrono::milliseconds(25), [this] { return stopped; });
			}
		});
	}
	~Session() {
		{
			std::lock_guard<std::mutex> lock(mu);
			stopped = true;
		}
		cv.notify_all();
		if (watcher.joinable())
			watcher.join();
	}
	string error;
	GuardedODBCSourceError source;
	void Check(SQLRETURN rc) {
		if (rc == SQL_SUCCESS)
			return;
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "oracle");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, nullptr, 0, &len);
		switch (native) {
		case 1:
			error = "23505";
			break;
		case 1400:
		case 1407:
			error = "23502";
			break;
		case 1438:
		case 1426:
			error = "22003";
			break;
		case 12899:
		case 1461:
			error = "22001";
			break;
		case 1465:
		case 1722:
			error = "22P02";
			break;
		case 1830:
		case 1839:
		case 1840:
		case 1841:
		case 1843:
		case 1847:
		case 1858:
		case 1861:
		case 1873:
		case 1874:
		case 1877:
		case 1878:
		case 1890:
			error = "22007";
			break;
		case 2290:
			error = "23514";
			break;
		case 2291:
		case 2292:
			error = "23503";
			break;
		case 60:
			error = "40P01";
			break;
		case 54:
		case 30006:
			error = "55P03";
			break;
		case 1031:
			error = "42501";
			break;
		case 1013:
			error = "57014";
			break;
		default:
			if (cancelled.load() || ctx.IsInterrupted())
				error = "57014";
			else
				error = state[0] ? string(reinterpret_cast<char *>(state)) : "XX000";
		}
		Reject();
	}
	void Start(bool cleanup = false) {
		if (!cleanup && (cancelled.load() || ctx.IsInterrupted())) {
			error = "57014";
			Reject();
		}
		{
			std::lock_guard<std::mutex> lock(mu);
			cleanup_mode = cleanup;
			cancel_sent = false;
			statement_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		}
		cv.notify_all();
		Check(SQLFreeStmt(stmt.get(), SQL_CLOSE));
		Check(SQLFreeStmt(stmt.get(), SQL_UNBIND));
		Check(SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS));
	}
	void Exec(const string &sql, bool cleanup = false) {
		Start(cleanup);
		auto rc = OracleODBCExecDirectUTF8(stmt.get(), sql);
		if (rc == SQL_NO_DATA)
			return;
		Check(rc);
		while ((rc = SQLMoreResults(stmt.get())) != SQL_NO_DATA)
			Check(rc);
	}
	vector<vector<string>> Rows(const string &sql, bool cleanup = false) {
		return RowsInternal(sql, cleanup, false);
	}
	vector<vector<string>> RowsAllowNull(const string &sql, bool cleanup = false) {
		return RowsInternal(sql, cleanup, true);
	}
	vector<vector<string>> RowsInternal(const string &sql, bool cleanup, bool allow_null) {
		Start(cleanup);
		Check(OracleODBCExecDirectUTF8(stmt.get(), sql));
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols < 1 || cols > 64)
			Reject();
		vector<vector<string>> rows;
		idx_t bytes = 0;
		SQLRETURN rc;
		while ((rc = SQLFetch(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
			if (rows.size() >= 8192)
				Reject();
			vector<string> row;
			for (SQLUSMALLINT col = 1; col <= cols; col++) {
				char value[4096] = {0};
				SQLLEN len = 0;
				Check(SQLGetData(stmt.get(), col, SQL_C_CHAR, value, sizeof(value), &len));
				if (len == SQL_NULL_DATA && allow_null) {
					row.emplace_back("");
					continue;
				}
				if (len == SQL_NULL_DATA || len < 0 || len >= sizeof(value))
					Reject();
				row.emplace_back(value, static_cast<size_t>(len));
				bytes += row.back().size();
				if (bytes > 1024 * 1024)
					Reject();
			}
			rows.push_back(std::move(row));
		}
		// Metadata SELECTs must have exactly one result, fully consumed.
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
		return rows;
	}
	string Scalar(const string &sql, bool cleanup = false) {
		auto r = Rows(sql, cleanup);
		if (r.size() != 1 || r[0].size() != 1)
			Reject();
		return r[0][0];
	}
	struct BoundParams {
		std::vector<std::vector<SQLWCHAR>> values;
		vector<float> float_values;
		vector<double> double_values;
		vector<string> blobs;
		vector<SQLLEN> lengths;
	};
	void BindPreparedInputParams(const vector<Value> &params, BoundParams &b) {
		b.values.resize(params.size());
		b.float_values.assign(params.size(), 0.0f);
		b.double_values.assign(params.size(), 0.0);
		b.blobs.resize(params.size());
		b.lengths.assign(params.size(), 0);
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			auto tid = v.type().id();
			if (tid == LogicalTypeId::FLOAT) {
				if (v.IsNull())
					b.lengths[i] = SQL_NULL_DATA;
				else {
					b.float_values[i] = v.GetValue<float>();
					if (!std::isfinite(b.float_values[i]))
						Reject();
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_REAL, 7, 0,
				                       &b.float_values[i], sizeof(float), &b.lengths[i]));
				continue;
			}
			if (tid == LogicalTypeId::DOUBLE) {
				if (v.IsNull())
					b.lengths[i] = SQL_NULL_DATA;
				else {
					b.double_values[i] = v.GetValue<double>();
					if (!std::isfinite(b.double_values[i]))
						Reject();
				}
				Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 15, 0,
				                       &b.double_values[i], sizeof(double), &b.lengths[i]));
				continue;
			}
			if (tid == LogicalTypeId::BLOB) {
				if (v.IsNull()) {
					b.lengths[i] = SQL_NULL_DATA;
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, 1, 0,
					                       nullptr, 0, &b.lengths[i]));
				} else {
					b.blobs[i] = StringValue::Get(v);
					if (b.blobs[i].empty() || b.blobs[i].size() > 1048576)
						Reject();
					b.lengths[i] = static_cast<SQLLEN>(b.blobs[i].size());
					SQLSMALLINT sql_type = b.blobs[i].size() > 2000 ? SQL_LONGVARBINARY : SQL_VARBINARY;
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, sql_type,
					                       b.blobs[i].size(), 0, const_cast<char *>(b.blobs[i].data()),
					                       b.blobs[i].size(), &b.lengths[i]));
				}
				continue;
			}
			SQLSMALLINT sql_type = SQL_WVARCHAR;
			size_t units = 0;
			if (v.IsNull()) {
				b.lengths[i] = SQL_NULL_DATA;
			} else {
				if (tid != LogicalTypeId::VARCHAR)
					Reject();
				auto text = v.GetValue<string>();
				if (text.empty() || text.size() > 1048576 || text.find('\0') != string::npos)
					Reject();
				b.values[i] = OracleODBCUTF8ToWide(text);
				units = b.values[i].size() - 1;
				b.lengths[i] = static_cast<SQLLEN>(units * sizeof(SQLWCHAR));
				if (text.size() > 4000 || units > 4000)
					sql_type = SQL_WLONGVARCHAR;
			}
			Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_WCHAR, sql_type,
			                       std::max<size_t>(1, units), 0, b.values[i].empty() ? nullptr : b.values[i].data(),
			                       b.values[i].size() * sizeof(SQLWCHAR), &b.lengths[i]));
		}
	}
	int64_t Execute(const string &sql, const vector<Value> &params, bool &sent) {
		Start();
		Check(OracleODBCPrepareUTF8(stmt.get(), sql));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != params.size() || count > 2100)
			Reject();
		BoundParams b;
		BindPreparedInputParams(params, b);

		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
			Reject();
		}
		sent = true;
		auto rc = SQLExecute(stmt.get());
		if (rc != SQL_NO_DATA)
			Check(rc);
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols != 0)
			Reject();
		SQLLEN affected = -1;
		Check(SQLRowCount(stmt.get(), &affected));
		if (affected < 0)
			Reject();
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
		return affected;
	}
	int64_t ExecuteReturning(const string &sql, const vector<Value> &params, string &returned_rows, bool &sent) {
		Start();
		Check(OracleODBCPrepareUTF8(stmt.get(), sql));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != static_cast<SQLSMALLINT>(params.size() + 1) || count > 2100)
			Reject();
		BoundParams b;
		BindPreparedInputParams(params, b);
		char out_buf[128] = {0};
		SQLLEN out_ind = 0;
		Check(SQLBindParameter(stmt.get(), static_cast<SQLUSMALLINT>(params.size() + 1), SQL_PARAM_OUTPUT,
		                       SQL_C_CHAR, SQL_VARCHAR, 128, 0, out_buf, sizeof(out_buf), &out_ind));

		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
			Reject();
		}
		sent = true;
		auto rc = SQLExecute(stmt.get());
		if (rc != SQL_NO_DATA)
			Check(rc);
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols != 0)
			Reject();
		SQLLEN affected = -1;
		Check(SQLRowCount(stmt.get(), &affected));
		if (affected != 1)
			Reject();
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
		if (out_ind == SQL_NULL_DATA) {
			returned_rows = "[[null]]";
			return 1;
		}
		if (out_ind <= 0 || out_ind >= static_cast<SQLLEN>(sizeof(out_buf)))
			Reject();
		string val(out_buf, static_cast<size_t>(out_ind));
		while (!val.empty() && val.back() == '\0')
			val.pop_back();
		StringUtil::Trim(val);
		if (val.empty())
			Reject();
		idx_t pos = (val[0] == '+' || val[0] == '-') ? 1 : 0;
		if (pos >= val.size() || val.size() - pos > 38 ||
		    val.find_first_not_of("0123456789", pos) != string::npos)
			Reject();
		returned_rows = "[[" + JSON(val) + "]]";
		return 1;
	}

private:
	ClientContext &ctx;
	ODBCStatementPtr stmt;
	std::mutex mu;
	std::condition_variable cv;
	bool stopped = false, cleanup_mode = false, cancel_sent = true;
	std::chrono::steady_clock::time_point statement_deadline = std::chrono::steady_clock::time_point::max();
	std::atomic<bool> cancelled {false};
	std::thread watcher;
};
struct Data : TableFunctionData {
	ODBCCatalog &catalog;
	string schema, table, expected, version, sql;
	vector<Value> params;
	bool execute;
	bool diagnostics = false;
	bool returning = false;
	string returning_name;
	int64_t returning_oid = 0;
	Data(ODBCCatalog &c, bool exec) : catalog(c), execute(exec) {
	}
};
struct State : GlobalTableFunctionState {
	bool done = false;
};
static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<State>();
}
static unique_ptr<FunctionData> BindInternal(ClientContext &ctx, TableFunctionBindInput &in,
                                             vector<LogicalType> &types, vector<string> &names, bool is_returning) {
	for (auto &v : in.inputs)
		if (v.IsNull())
			Reject();
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, in.inputs[0].GetValue<string>());
	if (!db || db->GetCatalog().GetCatalogType() != "oracle")
		Reject();
	auto d = make_uniq<Data>(db->GetCatalog().Cast<ODBCCatalog>(), in.inputs.size() == 6);
	d->returning = is_returning;
	d->schema = in.inputs[1].GetValue<string>();
	d->table = in.inputs[2].GetValue<string>();
	if (!SimpleName(d->schema) || !SimpleName(d->table))
		Reject();
	if (d->execute) {
		d->expected = in.inputs[3].GetValue<string>();
		d->version = in.inputs[4].GetValue<string>();
		d->sql = in.inputs[5].GetValue<string>();
		if (d->expected.empty() || d->expected.size() > 1024 * 1024 || d->sql.empty() ||
		    d->sql.size() > 4 * 1024 * 1024)
			Reject();
		auto it = in.named_parameters.find("params");
		if (it != in.named_parameters.end()) {
			if (it->second.IsNull() || it->second.type().id() != LogicalTypeId::STRUCT)
				Reject();
			d->params = StructValue::GetChildren(it->second);
		}
		if (d->params.size() > 2100)
			Reject();
	}
	if (d->returning) {
		if (!d->execute)
			Reject();
		auto name_it = in.named_parameters.find("returning_name");
		auto oid_it = in.named_parameters.find("returning_oid");
		if (name_it == in.named_parameters.end() || name_it->second.IsNull() ||
		    name_it->second.type().id() != LogicalTypeId::VARCHAR || oid_it == in.named_parameters.end() ||
		    oid_it->second.IsNull() || oid_it->second.type().id() != LogicalTypeId::BIGINT)
			Reject();
		d->returning_name = name_it->second.GetValue<string>();
		d->returning_oid = oid_it->second.GetValue<int64_t>();
		if (!SimpleName(d->returning_name) || d->returning_oid != 1700)
			Reject();
	}
	auto diagnostic = in.named_parameters.find("source_diagnostics");
	if (diagnostic != in.named_parameters.end()) {
		if (diagnostic->second.IsNull())
			Reject();
		d->diagnostics = diagnostic->second.GetValue<bool>();
	}
	names = {"status", "affected_rows", "sqlstate", "stage", "version", "descriptor"};
	types = {LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::VARCHAR,
	         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	if (d->returning) {
		names.push_back("returned_rows");
		types.push_back(LogicalType::VARCHAR);
	}
	if (d->diagnostics) {
		names.push_back("source_error");
		types.push_back(LogicalType::VARCHAR);
	}
	return std::move(d);
}
static unique_ptr<FunctionData> Bind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                     vector<string> &names) {
	return BindInternal(ctx, in, types, names, false);
}
static unique_ptr<FunctionData> BindReturning(ClientContext &ctx, TableFunctionBindInput &in,
                                              vector<LogicalType> &types, vector<string> &names) {
	return BindInternal(ctx, in, types, names, true);
}
static string JSONRows(const vector<vector<string>> &rows) {
	string out = "[";
	bool first = true;
	for (auto &row : rows) {
		if (!first)
			out += ",";
		first = false;
		out += "[";
		bool cell = true;
		for (auto &s : row) {
			if (!cell)
				out += ",";
			cell = false;
			out += JSON(s);
		}
		out += "]";
		if (out.size() > 1024 * 1024)
			Reject();
	}
	return out + "]";
}
static string Metadata(Session &s, const Data &d, string &stage, std::unordered_set<string> *column_names = nullptr) {
	const auto owner = Literal(d.schema), table = Literal(d.table);
	const auto filter = "OWNER=" + owner + " AND TABLE_NAME=" + table;
	stage = "source_objects";
	auto objects = s.Rows("SELECT "
	                      "TO_CHAR(OBJECT_ID),COALESCE(TO_CHAR(DATA_OBJECT_ID),'0'),TO_CHAR(CREATED,'YYYYMMDDHH24MISS')"
	                      ",TO_CHAR(LAST_DDL_TIME,'YYYYMMDDHH24MISS'),STATUS FROM DBA_OBJECTS WHERE OWNER=" +
	                      owner + " AND OBJECT_NAME=" + table + " AND OBJECT_TYPE='TABLE' AND SUBOBJECT_NAME IS NULL");
	if (objects.size() != 1 || objects[0][4] != "VALID")
		Reject();
	auto sys_has_column = [&](const char *tbl, const char *col) {
		auto flag = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_TAB_COLUMNS WHERE OWNER='SYS' AND TABLE_NAME=" +
		                     Literal(tbl) + " AND COLUMN_NAME=" + Literal(col));
		if (flag != "0" && flag != "1")
			Reject();
		return flag == "1";
	};
	const bool has_con_id = sys_has_column("V_$LOCKED_OBJECT", "CON_ID");
	const bool has_external_col = sys_has_column("DBA_TABLES", "EXTERNAL");
	const bool has_identity_col = sys_has_column("DBA_TAB_COLS", "IDENTITY_COLUMN");
	const bool has_default_on_null = sys_has_column("DBA_TAB_COLS", "DEFAULT_ON_NULL");
	const bool has_search_condition_vc = sys_has_column("DBA_CONSTRAINTS", "SEARCH_CONDITION_VC");
	stage = "source_locks";
	string lock_query = "SELECT TO_CHAR(COUNT(*)) FROM V$LOCKED_OBJECT WHERE OBJECT_ID=" + objects[0][0] +
	                    " AND LOCKED_MODE=6 AND SESSION_ID=TO_NUMBER(SYS_CONTEXT('USERENV','SID'))";
	if (has_con_id)
		lock_query += " AND CON_ID=TO_NUMBER(SYS_CONTEXT('USERENV','CON_ID'))";
	if (s.Scalar(lock_query) == "0")
		Reject();
	stage = "source_table";
	string table_query = "SELECT TO_CHAR(COUNT(*)) FROM DBA_TABLES WHERE " + filter +
	                     " AND TEMPORARY='N' AND SECONDARY='N' AND NESTED='NO' AND PARTITIONED='NO' AND IOT_TYPE "
	                     "IS NULL AND CLUSTER_NAME IS NULL AND READ_ONLY='NO' AND DROPPED='NO'";
	if (has_external_col)
		table_query += " AND EXTERNAL='NO'";
	if (s.Scalar(table_query) != "1")
		Reject();
	if (!has_external_col && s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_EXTERNAL_TABLES WHERE " + filter) != "0")
		Reject();
	stage = "source_triggers";
	if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_TRIGGERS WHERE TABLE_OWNER=" + owner +
	             " AND TABLE_NAME=" + table) != "0")
		Reject();
	stage = "source_policies";
	if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_POLICIES WHERE OBJECT_OWNER=" + owner +
	             " AND OBJECT_NAME=" + table) != "0")
		Reject();
	stage = "source_mviews";
	if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_MVIEWS WHERE OWNER=" + owner + " AND MVIEW_NAME=" + table) != "0")
		Reject();
	if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_MVIEW_LOGS WHERE LOG_OWNER=" + owner + " AND MASTER=" + table) !=
	    "0")
		Reject();
	stage = "source_references";
	auto parent_keys =
	    s.Rows("SELECT CONSTRAINT_NAME FROM DBA_CONSTRAINTS WHERE " + filter + " AND CONSTRAINT_TYPE IN('P','U')");
	if (parent_keys.size() > 1024)
		Reject();
	for (auto &key : parent_keys)
		if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_CONSTRAINTS WHERE CONSTRAINT_TYPE='R' AND R_OWNER=" + owner +
		             " AND R_CONSTRAINT_NAME=" + Literal(key[0])) != "0")
			Reject();

	stage = "source_columns";
	string col_query =
	    "SELECT "
	    "COLUMN_NAME,DATA_TYPE,NULLABLE,COALESCE(TO_CHAR(COLUMN_ID),'0'),COALESCE(TO_CHAR(DATA_PRECISION),'0'),"
	    "COALESCE(TO_CHAR(DATA_SCALE),'0'),COALESCE(TO_CHAR(DATA_LENGTH),'0'),COALESCE(TO_CHAR(CHAR_LENGTH),'0'),"
	    "COALESCE(CHAR_USED,'-'),COALESCE(DATA_TYPE_OWNER,'-'),COALESCE(TO_CHAR(DEFAULT_LENGTH),'0')," +
	    string(has_identity_col ? "COALESCE(IDENTITY_COLUMN,'NO')" : "'NO'") +
	    ",DATA_DEFAULT "
	    "FROM DBA_TAB_COLS WHERE " +
	    filter + " AND HIDDEN_COLUMN='NO' AND VIRTUAL_COLUMN='NO'";
	if (has_default_on_null)
		col_query += " AND DEFAULT_ON_NULL='NO'";
	col_query += " ORDER BY COLUMN_ID";
	auto cols = s.RowsAllowNull(col_query);
	auto count = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_TAB_COLS WHERE " + filter);
	if (cols.empty() || cols.size() > 1024 || count != std::to_string(cols.size()))
		Reject();
	bool any_identity = false;
	for (const auto &c : cols) {
		if (c.size() != 13)
			Reject();
		for (idx_t idx = 0; idx < 12; idx++) {
			if (c[idx].empty())
				Reject();
		}
		if (c[11] == "YES")
			any_identity = true;
	}
	vector<vector<string>> identity_cols;
	std::unordered_set<string> identity_always_cols;
	if (any_identity) {
		identity_cols = s.Rows("SELECT COLUMN_NAME,GENERATION_TYPE FROM DBA_TAB_IDENTITY_COLS WHERE " + filter +
		                       " ORDER BY COLUMN_NAME");
		for (const auto &ic : identity_cols) {
			if (ic[1] == "ALWAYS")
				identity_always_cols.insert(ic[0]);
		}
	}
	string charset, ncharset;
	for (auto &c : cols) {
		if (c[9] != "-" || (c[1] != "VARCHAR2" && c[1] != "CHAR" && c[1] != "NVARCHAR2" && c[1] != "NCHAR" &&
		                    c[1] != "CLOB"))
			continue;
		stage = "source_charset";
		// Keep the existing lossless source/client charset contract for guarded
		// text writes while letting opaque columns remain unreferenced.
		const auto lang = std::getenv("NLS_LANG");
		auto db_charset = s.Scalar("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'");
		auto db_ncharset = s.Scalar("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_NCHAR_CHARACTERSET'");
		if (lang && StringUtil::EndsWith(StringUtil::Upper(lang), ".AL32UTF8")) {
			if (db_charset == "AL32UTF8")
				charset = db_charset;
			if (db_ncharset == "AL16UTF16" || db_ncharset == "UTF8")
				ncharset = db_ncharset;
		}
		break;
	}

	stage = "source_constraints";
	vector<vector<string>> constraints;
	if (has_search_condition_vc) {
		constraints = s.Rows(
		    "SELECT CONSTRAINT_NAME,CONSTRAINT_TYPE,DEFERRABLE,DEFERRED,STATUS,VALIDATED,COALESCE(SEARCH_CONDITION_VC,"
		    "' '),TO_CHAR(LAST_CHANGE,'YYYYMMDDHH24MISS') FROM DBA_CONSTRAINTS WHERE " +
		    filter + " ORDER BY CONSTRAINT_NAME");
	} else {
		auto raw_constraints = s.RowsAllowNull(
		    "SELECT CONSTRAINT_NAME,CONSTRAINT_TYPE,DEFERRABLE,DEFERRED,STATUS,VALIDATED,TO_CHAR(LAST_CHANGE,"
		    "'YYYYMMDDHH24MISS'),SEARCH_CONDITION FROM DBA_CONSTRAINTS WHERE " +
		    filter + " ORDER BY CONSTRAINT_NAME");
		for (auto &row : raw_constraints) {
			if (row.size() != 8)
				Reject();
			constraints.push_back({row[0], row[1], row[2], row[3], row[4], row[5], row[7].empty() ? " " : row[7], row[6]});
		}
	}
	for (auto &c : constraints) {
		if (c[2] != "NOT DEFERRABLE" || c[3] != "IMMEDIATE" || c[4] != "ENABLED" || c[5] != "VALIDATED")
			Reject();
		if (c[1] == "C") {
			bool notnull = false;
			for (auto &col : cols)
				if (col[2] == "N" && c[6] == Identifier(col[0]) + " IS NOT NULL")
					notnull = true;
			if (!notnull)
				Reject();
		} else if (c[1] != "P" && c[1] != "U")
			Reject();
	}
	stage = "source_indexes";
	auto indexes = s.Rows("SELECT INDEX_NAME,INDEX_TYPE,UNIQUENESS,STATUS,PARTITIONED,TEMPORARY,VISIBILITY FROM "
	                      "DBA_INDEXES WHERE TABLE_OWNER=" +
	                      owner + " AND TABLE_NAME=" + table + " ORDER BY INDEX_NAME");
	std::unordered_set<string> lob_indexes;
	bool need_lob_lookup = false;
	for (auto &i : indexes) {
		if (i[1] == "LOB") {
			need_lob_lookup = true;
			break;
		}
	}
	if (need_lob_lookup) {
		auto lobs = s.Rows("SELECT INDEX_NAME FROM DBA_LOBS WHERE " + filter);
		for (auto &l : lobs)
			lob_indexes.insert(l[0]);
	}
	for (auto &i : indexes) {
		const bool ok_type = i[1] == "NORMAL" || (i[1] == "LOB" && lob_indexes.count(i[0]) > 0);
		if (!ok_type || i[3] != "VALID" || i[4] != "NO" || i[5] != "N" || i[6] != "VISIBLE")
			Reject();
	}
	auto keys = s.Rows(
	    "SELECT INDEX_NAME,COLUMN_NAME,TO_CHAR(COLUMN_POSITION),DESCEND FROM DBA_IND_COLUMNS WHERE TABLE_OWNER=" +
	    owner + " AND TABLE_NAME=" + table + " ORDER BY INDEX_NAME,COLUMN_POSITION");
	auto conscols =
	    s.Rows("SELECT CONSTRAINT_NAME,COLUMN_NAME,COALESCE(TO_CHAR(POSITION),'0') FROM DBA_CONS_COLUMNS WHERE " +
	           filter + " ORDER BY CONSTRAINT_NAME,POSITION");
	string single_pk_column_name;
	for (auto &con : constraints) {
		if (con[1] != "P")
			continue;
		vector<string> pk_cols;
		for (auto &cc : conscols) {
			if (cc[0] == con[0])
				pk_cols.push_back(cc[1]);
		}
		if (pk_cols.size() == 1)
			single_pk_column_name = pk_cols[0];
		break;
	}
	stage = "source_identity";
	auto identity = s.Rows(string("SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),SYS_CONTEXT('USERENV','SERVICE_NAME'),") +
	                       (has_con_id ? "SYS_CONTEXT('USERENV','CON_ID')" : "'0'") + " FROM DUAL");
	auto revision = JSONRows(identity) + JSONRows(objects) + JSONRows(cols) + JSONRows(constraints) +
	                JSONRows(indexes) + JSONRows(keys) + JSONRows(conscols) + JSONRows(identity_cols);
	string out = "{\"database\":" + JSON(identity[0][1]) + ",\"schema\":" + JSON(d.schema) +
	             ",\"table\":" + JSON(d.table) + ",\"object_id\":" + JSON(objects[0][0]) +
	             ",\"revision\":" + JSON(revision) + ",\"columns\":[";
	auto valid_int = [](const string &v) {
		if (v.empty())
			return false;
		idx_t pos = v[0] == '-' ? 1 : 0;
		return pos < v.size() && v.find_first_not_of("0123456789", pos) == string::npos;
	};
	bool first = true;
	for (auto &c : cols) {
		if (!SimpleName(c[0]) || !valid_int(c[4]) || !valid_int(c[5]))
			Reject();
		if (column_names)
			column_names->insert(c[0]);
		if (!first)
			out += ",";
		first = false;
		auto type = c[9] != "-" ? (c[9] + "." + c[1]) : c[1];
		if (c[9] == "-" && type.size() == 12 && StringUtil::StartsWith(type, "TIMESTAMP(") && type[10] >= '0' &&
		    type[10] <= '6' && type[11] == ')')
			type = "TIMESTAMP";
		else if (c[9] == "-" && type.size() == 27 && StringUtil::StartsWith(type, "TIMESTAMP(") && type[10] >= '0' &&
		         type[10] <= '6' && type.substr(11) == ") WITH TIME ZONE")
			type = "TIMESTAMP WITH TIME ZONE";
		out += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(type) +
		       ",\"nullable\":" + (c[2] == "Y" ? "true" : "false") + ",\"precision\":" + c[4] + ",\"scale\":" + c[5];
		const bool is_identity = (c[11] == "YES");
		const bool identity_always = is_identity && identity_always_cols.count(c[0]) > 0;
		if (is_identity) {
			out += ",\"is_identity\":true";
			if (identity_always)
				out += ",\"identity_always\":true";
		} else {
			string def_trimmed = c[12];
			StringUtil::Trim(def_trimmed);
			const bool has_default = (c[10] != "0" || !def_trimmed.empty());
			if (has_default) {
				out += ",\"has_default\":true";
				if (IsSafeOracleDefault(def_trimmed))
					out += ",\"default_safe\":true";
			}
		}
		if (!single_pk_column_name.empty() && c[0] == single_pk_column_name)
			out += ",\"single_column_primary_key\":true";
		if (type == "VARCHAR2" || type == "CHAR") {
			if (!valid_int(c[6]) || !valid_int(c[7]))
				Reject();
			out += ",\"max_length\":" + c[6] + ",\"char_length\":" + c[7] + ",\"char_used\":" + JSON(c[8]) +
			       ",\"charset\":" + JSON(charset);
		} else if (type == "NVARCHAR2" || type == "NCHAR") {
			if (!valid_int(c[6]) || !valid_int(c[7]))
				Reject();
			out += ",\"max_length\":" + c[6] + ",\"char_length\":" + c[7] + ",\"char_used\":" + JSON(c[8]) +
			       ",\"charset\":" + JSON(ncharset);
		} else if (type == "CLOB") {
			out += ",\"charset\":" + JSON(charset);
		} else if (type == "RAW") {
			if (!valid_int(c[6]))
				Reject();
			out += ",\"max_length\":" + c[6];
		}
		out += "}";
	}
	out += "]}";
	if (out.size() > 1024 * 1024)
		Reject();
	return out;
}
// Version is an opaque source identity. Capability is proved by the live
// session/catalog checks below, never by a fixed release interval.
static bool CompatibleVersion(const string &v) {
	if (v.empty() || v.size() > 1024 || StringUtil::CharacterIsSpace(v.front()) ||
	    StringUtil::CharacterIsSpace(v.back()))
		return false;
	for (auto c : v)
		if (static_cast<unsigned char>(c) < 32 || static_cast<unsigned char>(c) == 127)
			return false;
	return true;
}

static void Writable(SQLHDBC dbc) {
	if (!SQL_SUCCEEDED(
	        SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0)))
		Reject();
	if (!SQL_SUCCEEDED(
	        SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0)))
		Reject();
	SQLULEN mode = SQL_AUTOCOMMIT_ON;
	if (!SQL_SUCCEEDED(SQLGetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, &mode, sizeof(mode), nullptr)) ||
	    mode != SQL_AUTOCOMMIT_OFF)
		Reject();
}
static string TransactionState(Session &s, bool cleanup = false) {
	return s.Scalar("SELECT CASE WHEN DBMS_TRANSACTION.LOCAL_TRANSACTION_ID IS NULL THEN '0' ELSE '1' END FROM DUAL",
	                cleanup);
}
static void Scan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<State>();
	if (state.done)
		return;
	state.done = true;
	auto &d = in.bind_data->Cast<Data>();
	string status = "CONFIRMED_NOT_EXECUTED", error, stage = "source_metadata", version, descriptor;
	string source_error, returned_rows;
	int64_t affected = -1;
	bool sent = false, commit_started = false, local_rejection = false;
	ODBCPoolConnection pooled;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<Session> session;
	try {
		if (ctx.IsInterrupted())
			throw InterruptException();
		if (!d.catalog.GetConnectionPool().TryGetConnection(pooled))
			Reject();
		connection = pooled.GetConnection();
		session = make_uniq<Session>(ctx, connection->GetConn());
		auto &s = *session;
		if (TransactionState(s) != "0")
			Reject();
		stage = "source_session";
		Writable(connection->GetConn());
		SQLCHAR dbms[128] = {0};
		SQLSMALLINT len = 0;
		if (!SQL_SUCCEEDED(SQLGetInfo(connection->GetConn(), SQL_DBMS_NAME, dbms, sizeof(dbms), &len)) ||
		    StringUtil::Lower(reinterpret_cast<char *>(dbms)) != "oracle")
			Reject();
		// VERSION_FULL is a catalog capability, not a release-number inference.
		const auto full_version = s.Scalar("SELECT COUNT(*) FROM ALL_TAB_COLUMNS WHERE OWNER='SYS' AND "
		                                   "TABLE_NAME='PRODUCT_COMPONENT_VERSION' AND COLUMN_NAME='VERSION_FULL'");
		if (full_version != "0" && full_version != "1")
			Reject();
		version = s.Scalar("SELECT " + string(full_version == "1" ? "VERSION_FULL" : "VERSION") +
		                   " FROM PRODUCT_COMPONENT_VERSION WHERE PRODUCT LIKE 'Oracle Database%'");
		if (!CompatibleVersion(version) || (d.execute && version != d.version))
			Reject();
		s.Exec("ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,'");
		s.Exec("ALTER SESSION SET NLS_CALENDAR='GREGORIAN'");
		s.Exec("ALTER SESSION SET TIME_ZONE='+00:00'");
		s.Exec("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='SYYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'");
		s.Exec("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='SYYYY-MM-DD HH24:MI:SS.FF6'");
		s.Exec("ALTER SESSION SET NLS_DATE_FORMAT='SYYYY-MM-DD HH24:MI:SS'");
		stage = "source_lock";
		s.Exec("LOCK TABLE " + Identifier(d.schema) + "." + Identifier(d.table) + " IN EXCLUSIVE MODE WAIT 5");

		std::unordered_set<string> column_names;
		descriptor = Metadata(s, d, stage, &column_names);
		if (d.execute) {
			stage = "source_descriptor";
			if (descriptor != d.expected || (d.returning && column_names.count(d.returning_name) != 1)) {
				error = "0A000";
				Reject();
			}
			stage = "source_execute";
			if (d.returning)
				affected = s.ExecuteReturning(d.sql, d.params, returned_rows, sent);
			else
				affected = s.Execute(d.sql, d.params, sent);
			stage = "source_commit";
			commit_started = true;
			status = "COMMIT_STATUS_UNKNOWN";
			s.Exec("COMMIT");
			if (TransactionState(s) != "0")
				Reject();
			status = "COMMITTED";
			stage.clear();
		} else {
			s.Exec("ROLLBACK");
			if (TransactionState(s) != "0")
				Reject();
			stage.clear();
		}
	} catch (...) {
		try {
			throw;
		} catch (const GuardedODBCLocalRejection &) {
			local_rejection = true;
		} catch (const GuardedODBCConnectionFailure &failure) {
			auto source = failure.diagnostic;
			source.product = "oracle";
			source_error = source.JSON();
			error = source.state;
			stage = "source_connection";
		} catch (...) {
		}
		// Snapshot before rollback/cancellation cleanup replaces handle diagnostics.
		if (session && source_error.empty())
			source_error = session->source.JSON();
		if (error.empty())
			error = session && !session->error.empty()
			            ? session->error
			            : (ctx.IsInterrupted() ? "57014" : (local_rejection ? "0A000" : "XX000"));
		if (session) {
			try {
				session->Exec("ROLLBACK", true);
				if (TransactionState(*session, true) != "0")
					Reject();
				if (sent && !commit_started)
					status = "CONFIRMED_ROLLED_BACK";
			} catch (...) {
				if (sent)
					status = "COMMIT_STATUS_UNKNOWN";
			}
		}
	}
	session.reset(); // join cancellation thread before disconnecting this session
	if (connection)
		connection->Close(); // isolated state is never returned for reuse
	out.SetCardinality(1);
	out.SetValue(0, 0, Value(status));
	out.SetValue(1, 0, Value::BIGINT(affected));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value(version));
	out.SetValue(5, 0, Value(descriptor));
	idx_t out_col = 6;
	if (d.returning)
		out.SetValue(out_col++, 0, Value(status == "COMMITTED" ? returned_rows : ""));
	if (d.diagnostics)
		out.SetValue(out_col++, 0, Value(source_error));
}

// Administrative DuckDB callers use an explicitly writable attachment.
// fedQuery attaches Oracle read-only and denies this function publicly.
struct AdminData : TableFunctionData {
	ODBCCatalog &catalog;
	string sql;
	AdminData(ODBCCatalog &c, string q) : catalog(c), sql(std::move(q)) {
	}
};
static unique_ptr<FunctionData> AdminBind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                          vector<string> &names) {
	if (in.inputs[0].IsNull() || in.inputs[1].IsNull())
		Reject();
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, in.inputs[0].GetValue<string>());
	if (!db || db->GetCatalog().GetCatalogType() != "oracle")
		Reject();
	auto &catalog = db->GetCatalog().Cast<ODBCCatalog>();
	auto sql = in.inputs[1].GetValue<string>();
	if (catalog.access_mode != AccessMode::READ_WRITE || sql.empty() || sql.size() > 4 * 1024 * 1024)
		Reject();
	types = {LogicalType::BOOLEAN};
	names = {"success"};
	return make_uniq<AdminData>(catalog, std::move(sql));
}
static void AdminScan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<State>();
	if (state.done)
		return;
	state.done = true;
	auto &d = in.bind_data->Cast<AdminData>();
	ODBCPoolConnection pooled;
	if (!d.catalog.GetConnectionPool().TryGetConnection(pooled))
		Reject();
	auto connection = pooled.GetConnection();
	try {
		Writable(connection->GetConn());
		Session session(ctx, connection->GetConn());
		session.Exec(d.sql);
		session.Exec("COMMIT");
	} catch (...) {
		connection->Close();
		Reject();
	}
	connection->Close();
	out.SetCardinality(1);
	out.SetValue(0, 0, Value::BOOLEAN(true));
}
} // namespace
void RegisterOracleGuardedCRUD(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("oracle_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AdminScan, AdminBind, Init));
	TableFunction metadata("oracle_crud_metadata_v1", vector<LogicalType>(3, LogicalType::VARCHAR), Scan, Bind, Init);
	metadata.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(metadata);
	TableFunction exec("oracle_guarded_mutation_v1", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind, Init);
	exec.named_parameters["params"] = LogicalType::ANY;
	exec.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec);
	TableFunction exec_returning("oracle_guarded_insert_returning_v1", vector<LogicalType>(6, LogicalType::VARCHAR),
	                             Scan, BindReturning, Init);
	exec_returning.named_parameters["params"] = LogicalType::ANY;
	exec_returning.named_parameters["returning_name"] = LogicalType::VARCHAR;
	exec_returning.named_parameters["returning_oid"] = LogicalType::BIGINT;
	exec_returning.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec_returning);
}
} // namespace duckdb
