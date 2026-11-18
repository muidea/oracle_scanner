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
#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <mutex>
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
	int64_t Execute(const string &sql, const vector<Value> &params, bool &sent) {
		Start();
		Check(OracleODBCPrepareUTF8(stmt.get(), sql));
		SQLSMALLINT count = 0;
		Check(SQLNumParams(stmt.get(), &count));
		if (count != params.size() || count > 2100)
			Reject();
		std::vector<std::vector<SQLWCHAR>> values(params.size());
		vector<SQLLEN> lengths(params.size(), 0);
		for (idx_t i = 0; i < params.size(); i++) {
			auto &v = params[i];
			if (v.IsNull())
				lengths[i] = SQL_NULL_DATA;
			else {
				if (v.type().id() != LogicalTypeId::VARCHAR)
					Reject();
				auto text = v.GetValue<string>();
				if (text.empty() || text.size() > 4000 || text.find('\0') != string::npos)
					Reject();
				values[i] = OracleODBCUTF8ToWide(text);
				lengths[i] = (values[i].size() - 1) * sizeof(SQLWCHAR);
			}
			Check(SQLBindParameter(stmt.get(), i + 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR,
			                       std::max<size_t>(1, values[i].empty() ? 0 : values[i].size() - 1), 0,
			                       values[i].empty() ? nullptr : values[i].data(), values[i].size() * sizeof(SQLWCHAR),
			                       &lengths[i]));
		}

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
	auto diagnostic = in.named_parameters.find("source_diagnostics");
	if (diagnostic != in.named_parameters.end()) {
		if (diagnostic->second.IsNull())
			Reject();
		d->diagnostics = diagnostic->second.GetValue<bool>();
	}
	names = {"status", "affected_rows", "sqlstate", "stage", "version", "descriptor"};
	types = {LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::VARCHAR,
	         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
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
static string Metadata(Session &s, const Data &d, string &stage) {
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
	auto cols = s.Rows(
	    "SELECT "
	    "COLUMN_NAME,DATA_TYPE,NULLABLE,TO_CHAR(COLUMN_ID),COALESCE(TO_CHAR(DATA_PRECISION),'0'),COALESCE(TO_CHAR(DATA_"
	    "SCALE),'0'),"
	    "TO_CHAR(DATA_LENGTH),COALESCE(TO_CHAR(CHAR_LENGTH),'0'),COALESCE(CHAR_USED,'-') "
	    "FROM DBA_TAB_COLS WHERE " +
	    filter +
	    " AND ((DATA_TYPE='NUMBER' AND DATA_PRECISION BETWEEN 1 AND 38 AND DATA_SCALE BETWEEN 0 AND DATA_PRECISION)"
	    " OR (DATA_TYPE='VARCHAR2' AND DATA_LENGTH BETWEEN 1 AND 4000 AND CHAR_LENGTH BETWEEN 1 AND DATA_LENGTH AND "
	    "CHAR_USED IN('B','C'))"
	    " OR DATA_TYPE='DATE' OR (DATA_TYPE "
	    "IN('TIMESTAMP(0)','TIMESTAMP(1)','TIMESTAMP(2)','TIMESTAMP(3)','TIMESTAMP(4)','TIMESTAMP(5)','TIMESTAMP(6)') "
	    "AND DATA_SCALE BETWEEN 0 AND 6))"
	    " AND DATA_TYPE_OWNER IS NULL AND HIDDEN_COLUMN='NO' AND VIRTUAL_COLUMN='NO' AND IDENTITY_COLUMN='NO' AND "
	    "DEFAULT_ON_NULL='NO' AND COALESCE(DEFAULT_LENGTH,0)=0 ORDER BY COLUMN_ID");
	auto count = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM DBA_TAB_COLS WHERE " + filter);
	if (cols.empty() || cols.size() > 1024 || count != std::to_string(cols.size()))
		Reject();
	string charset;
	for (auto &c : cols) {
		if (c[1] != "VARCHAR2")
			continue;
		stage = "source_charset";
		// Keep the existing lossless source/client charset contract for guarded
		// VARCHAR2 writes and the narrow-character metadata reads below.
		const auto lang = std::getenv("NLS_LANG");
		if (!lang || !StringUtil::EndsWith(StringUtil::Upper(lang), ".AL32UTF8"))
			Reject();
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
		if (StringUtil::StartsWith(type, "TIMESTAMP("))
			type = "TIMESTAMP";
		out += "{\"name\":" + JSON(c[0]) + ",\"data_type\":" + JSON(type) +
		       ",\"nullable\":" + (c[2] == "Y" ? "true" : "false") + ",\"precision\":" + c[4] + ",\"scale\":" + c[5];
		if (type == "VARCHAR2")
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
	string source_error;
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
		stage = "source_lock";
		s.Exec("LOCK TABLE " + Identifier(d.schema) + "." + Identifier(d.table) + " IN EXCLUSIVE MODE WAIT 5");

		descriptor = Metadata(s, d, stage);
		if (d.execute) {
			stage = "source_descriptor";
			if (descriptor != d.expected) {
				error = "0A000";
				Reject();
			}
			stage = "source_execute";
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
	if (d.diagnostics)
		out.SetValue(6, 0, Value(source_error));
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
	out.SetCardinality(1);
	out.SetValue(0, 0, Value::BOOLEAN(true));
}
} // namespace
void RegisterOracleGuardedCRUD(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("oracle_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AdminScan, AdminBind, Init));
	TableFunction metadata("oracle_crud_metadata_v1", vector<LogicalType>(3, LogicalType::VARCHAR), Scan, Bind, Init);
	metadata.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("source_diagnostics", LogicalType::BOOLEAN); });
	loader.RegisterFunction(metadata);
	TableFunction exec("oracle_guarded_mutation_v1", vector<LogicalType>(6, LogicalType::VARCHAR), Scan, Bind, Init);
	exec.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("params", LogicalType::ANY);
		options.Add("source_diagnostics", LogicalType::BOOLEAN);
	});
	loader.RegisterFunction(exec);
}
} // namespace duckdb
