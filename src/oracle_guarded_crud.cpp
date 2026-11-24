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
#include <cmath>
#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <thread>

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
			error = "22001";
			break;
		case 1839:
		case 1841:
		case 1843:
		case 1847:
		case 1861:
			error = "22007";
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
	void BindInputParams(const vector<Value> &params, std::vector<std::vector<SQLWCHAR>> &values, vector<string> &blobs,
	                     vector<SQLLEN> &lengths) {
		static char empty_blob_byte = 0;
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			if (v.type().id() == LogicalTypeId::BLOB) {
				if (v.IsNull()) {
					lengths[i] = SQL_NULL_DATA;
					Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_LONGVARBINARY, 1, 0,
					                       &empty_blob_byte, 0, &lengths[i]));
				} else {
					blobs[i] = StringValue::Get(v);
					if (blobs[i].size() > 4 * 1024 * 1024)
						Reject();
					lengths[i] = static_cast<SQLLEN>(blobs[i].size());
					Check(SQLBindParameter(
					    stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_LONGVARBINARY,
					    std::max<size_t>(1, blobs[i].size()), 0,
					    blobs[i].empty() ? &empty_blob_byte : const_cast<char *>(blobs[i].data()), blobs[i].size(),
					    &lengths[i]));
				}
				continue;
			}
			if (v.IsNull())
				lengths[i] = SQL_NULL_DATA;
			else {
				string text;
				switch (v.type().id()) {
				case LogicalTypeId::VARCHAR:
					text = v.GetValue<string>();
					if (text.empty() || text.size() > 4000 || text.find('\0') != string::npos)
						Reject();
					break;
				case LogicalTypeId::BOOLEAN:
					text = v.GetValue<bool>() ? "1" : "0";
					break;
				case LogicalTypeId::SMALLINT:
				case LogicalTypeId::INTEGER:
				case LogicalTypeId::BIGINT:
					text = std::to_string(v.GetValue<int64_t>());
					break;
				case LogicalTypeId::FLOAT: {
					float f = v.GetValue<float>();
					if (!std::isfinite(f))
						Reject();
					std::ostringstream oss;
					oss << std::setprecision(9) << f;
					text = oss.str();
					break;
				}
				case LogicalTypeId::DOUBLE: {
					double d = v.GetValue<double>();
					if (!std::isfinite(d))
						Reject();
					std::ostringstream oss;
					oss << std::setprecision(17) << d;
					text = oss.str();
					break;
				}
				default:
					Reject();
				}
				values[i] = OracleODBCUTF8ToWide(text);
				lengths[i] = (values[i].size() - 1) * sizeof(SQLWCHAR);
			}
			Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR,
			                       std::max<size_t>(1, values[i].empty() ? 0 : values[i].size() - 1), 0,
			                       values[i].empty() ? nullptr : values[i].data(), values[i].size() * sizeof(SQLWCHAR),
			                       &lengths[i]));
		}
	}
	int64_t Execute(const string &sql, const vector<Value> &params, bool &sent) {
		Start();
		Check(OracleODBCPrepareUTF8(stmt.get(), sql));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != params.size() || count > 2100)
			Reject();
		std::vector<std::vector<SQLWCHAR>> values(params.size());
		vector<string> blobs(params.size());
		vector<SQLLEN> lengths(params.size(), 0);
		BindInputParams(params, values, blobs, lengths);

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
	int64_t ExecuteReturning(const string &sql, const vector<Value> &params, bool &sent, string &returned_rows) {
		Start();
		Check(OracleODBCPrepareUTF8(stmt.get(), sql));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != static_cast<SQLSMALLINT>(params.size() + 1) || count > 2100)
			Reject();
		std::vector<std::vector<SQLWCHAR>> values(params.size());
		vector<string> blobs(params.size());
		vector<SQLLEN> lengths(params.size(), 0);
		BindInputParams(params, values, blobs, lengths);
		SQLWCHAR out_buf[128] = {};
		SQLLEN out_len = 0;
		Check(SQLBindParameter(stmt.get(), static_cast<SQLUSMALLINT>(params.size() + 1), SQL_PARAM_OUTPUT,
		                       SQL_C_WCHAR, SQL_WVARCHAR, 127, 0, out_buf, sizeof(out_buf), &out_len));
		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
			Reject();
		}
		sent = true;
		auto rc = SQLExecute(stmt.get());
		if (rc == SQL_NO_DATA) {
			returned_rows = "[]";
			return 0;
		}
		Check(rc);
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols != 0)
			Reject();
		SQLLEN affected = -1;
		Check(SQLRowCount(stmt.get(), &affected));
		if (affected < 0)
			Reject();
		if (affected == 0) {
			returned_rows = "[]";
		} else if (affected == 1) {
			if (out_len == SQL_NULL_DATA || out_len <= 0 || out_len >= static_cast<SQLLEN>(sizeof(out_buf)) ||
			    (out_len % static_cast<SQLLEN>(sizeof(SQLWCHAR))) != 0) {
				error = "0A000";
				Reject();
			}
			std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
			auto units = static_cast<size_t>(out_len) / sizeof(SQLWCHAR);
			std::u16string u16(reinterpret_cast<const char16_t *>(out_buf), units);
			auto nul = u16.find(u'\0');
			if (nul != std::u16string::npos)
				u16.resize(nul);
			string val = converter.to_bytes(u16);
			if (val.empty() || val.size() > 38 || val.find_first_not_of("0123456789-") != string::npos)
				Reject();
			returned_rows = "[[" + JSON(val) + "]]";
		} else {
			error = "0A000";
			Reject();
		}
		rc = SQLMoreResults(stmt.get());
		if (rc != SQL_NO_DATA) {
			Check(rc);
			Reject();
		}
		return affected;
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
static unique_ptr<FunctionData> Bind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                     vector<duckdb::Identifier> &names) {
	for (auto &v : in.inputs)
		if (v.IsNull())
			Reject();
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, duckdb::Identifier(in.inputs[0].GetValue<string>()));
	if (!db || db->GetCatalog().GetCatalogType() != "oracle")
		Reject();
	auto d = make_uniq<Data>(db->GetCatalog().Cast<ODBCCatalog>(), in.inputs.size() == 6);
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
	d->returning = in.table_function.GetName().GetIdentifierName() == "oracle_guarded_insert_returning";
	if (d->returning) {
		auto rname = in.named_parameters.find("returning_name"), roid = in.named_parameters.find("returning_oid");
		if (rname == in.named_parameters.end() || roid == in.named_parameters.end() || rname->second.IsNull() ||
		    roid->second.IsNull())
			Reject();
		d->returning_name = rname->second.GetValue<string>();
		d->returning_oid = roid->second.GetValue<int64_t>();
		if (!SimpleName(d->returning_name) ||
		    (d->returning_oid != 20 && d->returning_oid != 21 && d->returning_oid != 23))
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
static string Metadata(Session &s, const Data &d, string &stage, int64_t *returning_col_oid = nullptr) {
	const auto owner = Literal(d.schema), table = Literal(d.table);
	const auto filter = "OWNER=" + owner + " AND TABLE_NAME=" + table;
	stage = "source_objects";
	auto objects = s.Rows("SELECT "
	                      "TO_CHAR(OBJECT_ID),COALESCE(TO_CHAR(DATA_OBJECT_ID),'0'),TO_CHAR(CREATED,'YYYYMMDDHH24MISS')"
	                      ",TO_CHAR(LAST_DDL_TIME,'YYYYMMDDHH24MISS'),STATUS FROM DBA_OBJECTS WHERE OWNER=" +
	                      owner + " AND OBJECT_NAME=" + table + " AND OBJECT_TYPE='TABLE' AND SUBOBJECT_NAME IS NULL");
	if (objects.size() != 1 || objects[0][4] != "VALID")
		Reject();
	stage = "source_locks";
	if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM V$LOCKED_OBJECT WHERE OBJECT_ID=" + objects[0][0] +
	             " AND LOCKED_MODE=6 AND SESSION_ID=TO_NUMBER(SYS_CONTEXT('USERENV','SID')) AND "
	             "CON_ID=TO_NUMBER(SYS_CONTEXT('USERENV','CON_ID'))") == "0")
		Reject();
	stage = "source_table";
	auto ordinary = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_TABLES WHERE " + filter +
	                         " AND TEMPORARY='N' AND SECONDARY='N' AND NESTED='NO' AND PARTITIONED='NO' AND IOT_TYPE "
	                         "IS NULL AND CLUSTER_NAME IS NULL AND EXTERNAL='NO' AND READ_ONLY='NO' AND DROPPED='NO'");
	if (ordinary != "1")
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

	stage = "source_columns";
	auto cols = s.Rows(
	    "SELECT "
	    "COLUMN_NAME,DATA_TYPE,NULLABLE,TO_CHAR(COLUMN_ID),COALESCE(TO_CHAR(DATA_PRECISION),'0'),COALESCE(TO_CHAR(DATA_"
	    "SCALE),'0'),"
	    "TO_CHAR(DATA_LENGTH),COALESCE(TO_CHAR(CHAR_LENGTH),'0'),COALESCE(CHAR_USED,'-'),"
	    "CASE WHEN COALESCE(DEFAULT_LENGTH,0)>0 THEN '1' ELSE '0' END,COALESCE(IDENTITY_COLUMN,'NO'),"
	    "CASE WHEN DATA_SCALE=0 THEN '1' ELSE '0' END "
	    "FROM DBA_TAB_COLS WHERE " +
	    filter +
	    " AND ((DATA_TYPE='NUMBER' AND (DATA_PRECISION IS NULL OR (DATA_PRECISION BETWEEN 1 AND 38 AND "
	    "COALESCE(DATA_SCALE,0) BETWEEN -84 AND 127)))"
	    " OR DATA_TYPE IN('BINARY_FLOAT','BINARY_DOUBLE','FLOAT')"
	    " OR (DATA_TYPE IN('VARCHAR2','NVARCHAR2','CHAR','NCHAR') AND DATA_LENGTH BETWEEN 1 AND 4000 AND "
	    "COALESCE(CHAR_LENGTH,DATA_LENGTH) BETWEEN 1 AND DATA_LENGTH AND COALESCE(CHAR_USED,'B') IN('B','C'))"
	    " OR DATA_TYPE IN('CLOB','NCLOB','BLOB','DATE','TIMESTAMP','TIMESTAMP WITH TIME ZONE')"
	    " OR (DATA_TYPE='RAW' AND DATA_LENGTH BETWEEN 1 AND 2000)"
	    " OR (DATA_TYPE "
	    "IN('TIMESTAMP(0)','TIMESTAMP(1)','TIMESTAMP(2)','TIMESTAMP(3)','TIMESTAMP(4)','TIMESTAMP(5)','TIMESTAMP(6)',"
	    "'TIMESTAMP(0) WITH TIME ZONE','TIMESTAMP(1) WITH TIME ZONE','TIMESTAMP(2) WITH TIME ZONE',"
	    "'TIMESTAMP(3) WITH TIME ZONE','TIMESTAMP(4) WITH TIME ZONE','TIMESTAMP(5) WITH TIME ZONE',"
	    "'TIMESTAMP(6) WITH TIME ZONE') "
	    "AND COALESCE(DATA_SCALE,6) BETWEEN 0 AND 6))"
	    " AND DATA_TYPE_OWNER IS NULL AND HIDDEN_COLUMN='NO' AND VIRTUAL_COLUMN='NO' ORDER BY COLUMN_ID");
	auto count = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_TAB_COLS WHERE " + filter +
	                      " AND HIDDEN_COLUMN='NO' AND VIRTUAL_COLUMN='NO'");
	if (cols.empty() || cols.size() > 1024 || count != std::to_string(cols.size()))
		Reject();
	string charset;
	for (auto &c : cols) {
		if (c[1] != "VARCHAR2" && c[1] != "NVARCHAR2" && c[1] != "CHAR" && c[1] != "NCHAR" &&
		    c[1] != "CLOB" && c[1] != "NCLOB")
			continue;
		stage = "source_charset";
		// Keep lossless AL32UTF8 Unicode transport. ODBC wide APIs avoid locale dependency;
		// only reject NLS_LANG when explicitly configured to an incompatible encoding.
		const auto lang = std::getenv("NLS_LANG");
		if (lang && *lang) {
			auto upper_lang = StringUtil::Upper(lang);
			if (!StringUtil::EndsWith(upper_lang, ".AL32UTF8") && !StringUtil::EndsWith(upper_lang, ".UTF8"))
				Reject();
		}
		charset = s.Scalar("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'");
		if (charset != "AL32UTF8")
			Reject();
		break;
	}

	stage = "source_constraints";
	auto constraints = s.Rows(
	    "SELECT CONSTRAINT_NAME,CONSTRAINT_TYPE,DEFERRABLE,DEFERRED,STATUS,VALIDATED,COALESCE(SEARCH_CONDITION_VC,' "
	    "'),TO_CHAR(LAST_CHANGE,'YYYYMMDDHH24MISS') FROM DBA_CONSTRAINTS WHERE " +
	    filter + " ORDER BY CONSTRAINT_NAME");
	for (auto &c : constraints) {
		if (c[2] != "NOT DEFERRABLE" || c[3] != "IMMEDIATE" || c[4] != "ENABLED" || c[5] != "VALIDATED")
			Reject();
		if (c[1] != "P" && c[1] != "U" && c[1] != "R" && c[1] != "C")
			Reject();
	}
	stage = "source_indexes";
	auto indexes = s.Rows("SELECT INDEX_NAME,INDEX_TYPE,UNIQUENESS,STATUS,PARTITIONED,TEMPORARY,VISIBILITY FROM "
	                      "DBA_INDEXES WHERE TABLE_OWNER=" +
	                      owner + " AND TABLE_NAME=" + table + " ORDER BY INDEX_NAME");
	for (auto &i : indexes)
		if (i[1] != "NORMAL" || i[3] != "VALID" || i[4] != "NO" || i[5] != "N" || i[6] != "VISIBLE")
			Reject();
	auto keys = s.Rows(
	    "SELECT INDEX_NAME,COLUMN_NAME,TO_CHAR(COLUMN_POSITION),DESCEND FROM DBA_IND_COLUMNS WHERE TABLE_OWNER=" +
	    owner + " AND TABLE_NAME=" + table + " ORDER BY INDEX_NAME,COLUMN_POSITION");
	auto conscols =
	    s.Rows("SELECT CONSTRAINT_NAME,COLUMN_NAME,COALESCE(TO_CHAR(POSITION),'0') FROM DBA_CONS_COLUMNS WHERE " +
	           filter + " ORDER BY CONSTRAINT_NAME,POSITION");
	stage = "source_identity";
	auto identity = s.Rows("SELECT "
	                       "SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),SYS_CONTEXT('USERENV','SERVICE_NAME'),SYS_CONTEXT('"
	                       "USERENV','CON_ID') FROM DUAL");
	auto revision = JSONRows(identity) + JSONRows(objects) + JSONRows(cols) + JSONRows(constraints) +
	                JSONRows(indexes) + JSONRows(keys) + JSONRows(conscols);
	string out = "{\"database\":" + JSON(identity[0][1]) + ",\"schema\":" + JSON(d.schema) +
	             ",\"table\":" + JSON(d.table) + ",\"object_id\":" + JSON(objects[0][0]) +
	             ",\"revision\":" + JSON(revision) + ",\"columns\":[";
	bool first = true;
	for (auto &c : cols) {
		if (!SimpleName(c[0]))
			Reject();
		if (!first)
			out += ",";
		first = false;
		auto type = c[1];
		if (type.find("WITH TIME ZONE") != string::npos)
			type = "TIMESTAMPTZ";
		else if (StringUtil::StartsWith(type, "TIMESTAMP("))
			type = "TIMESTAMP";
		int64_t type_oid = 0;
		if (c[1] == "NUMBER" && c[11] == "1") {
			auto prec = std::stoll(c[4]);
			if (prec == 0)
				type_oid = 20;
			else if (prec <= 5)
				type_oid = 21;
			else if (prec <= 10)
				type_oid = 23;
			else if (prec <= 19)
				type_oid = 20;
		}
		if (returning_col_oid && c[0] == d.returning_name)
			*returning_col_oid = type_oid;
		out += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(type) +
		       ",\"nullable\":" + (c[2] == "Y" ? "true" : "false") + ",\"precision\":" + c[4] + ",\"scale\":" + c[5] +
		       ",\"has_default\":" + (c[9] == "1" ? "true" : "false") +
		       ",\"identity\":" + (c[10] == "YES" ? "true" : "false");
		if (type_oid != 0)
			out += ",\"type_oid\":" + std::to_string(type_oid);
		if (type == "VARCHAR2" || type == "NVARCHAR2" || type == "CHAR" || type == "NCHAR")
			out += ",\"max_length\":" + c[6] + ",\"char_length\":" + c[7] + ",\"char_used\":" + JSON(c[8]) +
			       ",\"charset\":" + JSON(charset);
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
	int64_t ret_oid = 0;
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
		stage = "source_lock";
		s.Exec("LOCK TABLE " + Identifier(d.schema) + "." + Identifier(d.table) + " IN EXCLUSIVE MODE WAIT 5");

		descriptor = Metadata(s, d, stage, d.returning ? &ret_oid : nullptr);
		if (d.execute) {
			stage = "source_descriptor";
			if (descriptor != d.expected) {
				error = "0A000";
				Reject();
			}
			if (d.returning && (ret_oid == 0 || ret_oid != d.returning_oid)) {
				error = "0A000";
				Reject();
			}
			stage = "source_execute";
			affected = d.returning ? s.ExecuteReturning(d.sql, d.params, sent, returned_rows)
			                       : s.Execute(d.sql, d.params, sent);
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
	out.SetValue(0, 0, Value(status));
	out.SetValue(1, 0, Value::BIGINT(affected));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value(version));
	out.SetValue(5, 0, Value(descriptor));
	if (d.returning)
		out.SetValue(6, 0, Value(status == "COMMITTED" ? returned_rows : ""));
	if (d.diagnostics)
		out.SetValue(6 + (d.returning ? 1 : 0), 0, Value(source_error));
	out.SetChildCardinality(1);
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
                                          vector<duckdb::Identifier> &names) {
	if (in.inputs[0].IsNull() || in.inputs[1].IsNull())
		Reject();
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, duckdb::Identifier(in.inputs[0].GetValue<string>()));
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
	out.SetValue(0, 0, Value::BOOLEAN(true));
	out.SetChildCardinality(1);
}
} // namespace
void RegisterOracleGuardedCRUD(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("oracle_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AdminScan, AdminBind, Init));
	TableFunction metadata("oracle_crud_metadata", vector<LogicalType>(3, LogicalType::VARCHAR), Scan, Bind, Init);
	metadata.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("source_diagnostics", LogicalType::BOOLEAN); });
	loader.RegisterFunction(metadata);
	TableFunction exec("oracle_guarded_mutation", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind, Init);
	exec.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("params", LogicalType::ANY);
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
	});
	loader.RegisterFunction(exec);
	TableFunction ret("oracle_guarded_insert_returning", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind,
	                  Init);
	ret.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("params", LogicalType::ANY);
		options.Add("returning_name", LogicalType::VARCHAR);
		options.Add("returning_oid", LogicalType::BIGINT);
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
	});
	loader.RegisterFunction(ret);
}
} // namespace duckdb
