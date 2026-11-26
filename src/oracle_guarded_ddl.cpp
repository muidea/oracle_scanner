//===----------------------------------------------------------------------===//
//                         DuckDB
//
// oracle_guarded_ddl.cpp
//
// Guarded Oracle capability observation, catalog description, and
// implicit-commit-safe core-subset DDL execution on an isolated session.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "guarded_odbc_source_error.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "oracle_odbc_unicode.hpp"
#include "storage/odbc_catalog.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace duckdb {
namespace {
class DDLRejectException : public std::runtime_error {
public:
	string sqlstate;
	string stage;
	DDLRejectException(string state_p, string stage_p, const string &msg)
	    : std::runtime_error(msg), sqlstate(std::move(state_p)), stage(std::move(stage_p)) {
	}
};

[[noreturn]] static void RejectDDL(const string &sqlstate = "0A000", const string &stage = "source_identity",
                                   const string &msg = "guarded Oracle DDL rejected") {
	throw DDLRejectException(sqlstate, stage, msg);
}

[[noreturn]] static void RejectLocal() {
	throw GuardedODBCLocalRejection("guarded Oracle DDL input invalid");
}

static string JSON(const string &s) {
	const char *hex = "0123456789abcdef";
	string out = "\"";
	for (unsigned char c : s) {
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
			if (c < 32) {
				out += "\\u00";
				out += hex[c >> 4];
				out += hex[c & 15];
			} else {
				out += static_cast<char>(c);
			}
		}
	}
	return out + "\"";
}

static bool ValidName(const string &s) {
	return !s.empty() && s.size() <= 63 && s.find('\0') == string::npos;
}

static bool ValidUUID(const string &s) {
	if (s.size() != 36) {
		return false;
	}
	for (idx_t i = 0; i < s.size(); i++) {
		char c = s[i];
		if (i == 8 || i == 13 || i == 18 || i == 23) {
			if (c != '-') {
				return false;
			}
		} else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
			return false;
		}
	}
	return true;
}

static string Identifier(const string &s) {
	if (s.find('\0') != string::npos) {
		RejectLocal();
	}
	return ODBCUtils::WriteIdentifier(s);
}

static string Literal(const string &s) {
	if (s.find('\0') != string::npos) {
		RejectLocal();
	}
	return ODBCUtils::WriteLiteral(s);
}

static string TextVal(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::VARCHAR) {
		RejectLocal();
	}
	auto s = v.GetValue<string>();
	if (s.find('\0') != string::npos) {
		RejectLocal();
	}
	return s;
}

static int32_t IntVal(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::INTEGER) {
		RejectLocal();
	}
	return v.GetValue<int32_t>();
}

static bool BoolVal(const Value &v) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::BOOLEAN) {
		RejectLocal();
	}
	return v.GetValue<bool>();
}

static bool IsSignedInt64Literal(const string &s) {
	if (s.empty() || s == "-") {
		return false;
	}
	for (idx_t i = 0; i < s.size(); i++) {
		char c = s[i];
		if (!((c >= '0' && c <= '9') || (i == 0 && c == '-'))) {
			return false;
		}
	}
	try {
		size_t pos = 0;
		long long val = std::stoll(s, &pos, 10);
		return pos == s.size() && std::to_string(val) == s;
	} catch (...) {
		return false;
	}
}

static string ValidSQLState(const string &candidate, const string &fallback) {
	if (candidate.size() == 5) {
		bool ok = true;
		for (char c : candidate) {
			if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z'))) {
				ok = false;
				break;
			}
		}
		if (ok) {
			return candidate;
		}
	}
	return fallback;
}

static string MapOracleSQLState(SQLINTEGER native, const char *odbc_state) {
	switch (native) {
	case 54:
	case 30006:
		return "55P03";
	case 60:
		return "40P01";
	case 942:
		return "42P01";
	case 955:
		return "42P07";
	case 904:
		return "42703";
	case 1430:
		return "42701";
	case 1418:
	case 2443:
		return "42704";
	case 1442:
	case 1451:
	case 12983:
		return "42P16";
	case 2260:
	case 2261:
	case 2264:
	case 2275:
	case 1408:
		return "42710";
	case 2429:
	case 2449:
	case 2266:
	case 2273:
	case 12991:
	case 12992:
		return "2BP01";
	case 1400:
	case 1407:
	case 1758:
	case 2296:
		return "23502";
	case 2290:
	case 2293:
		return "23514";
	case 2291:
	case 2292:
	case 2298:
	case 2270:
		return "23503";
	case 1:
	case 1452:
	case 2437:
		return "23505";
	case 1031:
		return "42501";
	case 972:
		return "42622";
	case 1439:
	case 1440:
	case 1441:
	case 2327:
		return "0A000";
	case 1013:
		return "57014";
	case 16000:
		return "25006";
	default:
		break;
	}
	if (odbc_state) {
		return ValidSQLState(odbc_state, "HY000");
	}
	return "HY000";
}

// Deterministic Oracle errors guaranteed to abort before altering persistent definition.
static bool IsOracleConfirmedNotExecuted(SQLINTEGER native) {
	switch (native) {
	case 54:
	case 30006:
	case 942:
	case 955:
	case 904:
	case 1430:
	case 1418:
	case 1442:
	case 1451:
	case 2260:
	case 2261:
	case 2264:
	case 2275:
	case 1408:
	case 1439:
	case 1440:
	case 1441:
	case 1758:
	case 2429:
	case 2443:
	case 2449:
	case 2266:
	case 2273:
	case 12983:
	case 12991:
	case 12992:
	case 2296:
	case 2293:
	case 2298:
	case 2270:
	case 1452:
	case 2437:
	case 1031:
	case 972:
	case 2327:
	case 16000:
		return true;
	default:
		return false;
	}
}

// Never configure SQL_ATTR_QUERY_TIMEOUT on Oracle ODBC: it uses process-wide SIGALRM on Unix.
// Cancel via a session-scoped watcher thread using SQLCancelHandle instead.
class OracleDDLSession {
public:
	OracleDDLSession(ClientContext &ctx_p, SQLHDBC dbc, uint32_t stmt_timeout_sec = 15)
	    : ctx(ctx_p), stmt(nullptr, ODBCStatementDelete), timeout_sec(stmt_timeout_sec) {
		SQLHSTMT h = nullptr;
		if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &h) != SQL_SUCCESS) {
			RejectDDL("08006", "source_identity", "failed to allocate Oracle statement");
		}
		stmt = ODBCStatementPtr(h, ODBCStatementDelete);
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
	~OracleDDLSession() {
		{
			std::lock_guard<std::mutex> lock(mu);
			stopped = true;
		}
		cv.notify_all();
		if (watcher.joinable()) {
			watcher.join();
		}
	}

	string error;
	SQLINTEGER last_native = 0;
	int64_t last_warning_count = 0;
	GuardedODBCSourceError source;
	string current_stage = "source_identity";

	void Check(SQLRETURN rc) {
		if (rc == SQL_SUCCESS) {
			return;
		}
		if (rc == SQL_SUCCESS_WITH_INFO) {
			for (SQLSMALLINT rec = 1; rec <= 32; rec++) {
				SQLCHAR state[6] = {0};
				SQLINTEGER native = 0;
				SQLSMALLINT len = 0;
				if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), rec, state, &native, nullptr, 0, &len) == SQL_NO_DATA) {
					break;
				}
				last_warning_count++;
			}
			return;
		}
		source = ReadGuardedODBCSourceError(SQL_HANDLE_STMT, stmt.get(), "oracle");
		SQLCHAR state[6] = {0};
		SQLINTEGER native = 0;
		SQLSMALLINT len = 0;
		SQLGetDiagRec(SQL_HANDLE_STMT, stmt.get(), 1, state, &native, nullptr, 0, &len);
		last_native = native;
		if (cancelled.load() || ctx.IsInterrupted()) {
			error = "57014";
		} else {
			error = MapOracleSQLState(native, reinterpret_cast<const char *>(state));
		}
		RejectDDL(error, current_stage, source.message.empty() ? "Oracle DDL error" : source.message);
	}

	void Start(bool cleanup = false) {
		if (!cleanup && (cancelled.load() || ctx.IsInterrupted())) {
			RejectDDL("57014", current_stage, "operation cancelled");
		}
		{
			std::lock_guard<std::mutex> lock(mu);
			cleanup_mode = cleanup;
			cancel_sent = false;
			statement_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
		}
		cv.notify_all();
		auto rc1 = SQLFreeStmt(stmt.get(), SQL_CLOSE);
		if (!cleanup) Check(rc1);
		auto rc2 = SQLFreeStmt(stmt.get(), SQL_UNBIND);
		if (!cleanup) Check(rc2);
		auto rc3 = SQLFreeStmt(stmt.get(), SQL_RESET_PARAMS);
		if (!cleanup) Check(rc3);
	}

	void Exec(const string &sql, bool cleanup = false) {
		Start(cleanup);
		auto rc = OracleODBCExecDirectUTF8(stmt.get(), sql);
		if (cleanup) {
			return;
		}
		if (rc == SQL_NO_DATA) {
			return;
		}
		Check(rc);
		while ((rc = SQLMoreResults(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
		}
	}

	vector<vector<string>> Rows(const string &sql, bool allow_null = true) {
		Start(false);
		Check(OracleODBCExecDirectUTF8(stmt.get(), sql));
		SQLSMALLINT cols = 0;
		Check(SQLNumResultCols(stmt.get(), &cols));
		if (cols < 1 || cols > 64) {
			RejectDDL("XX000", current_stage, "unexpected column count in Oracle query");
		}
		vector<vector<string>> rows;
		idx_t total_bytes = 0;
		SQLRETURN rc;
		while ((rc = SQLFetch(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
			if (rows.size() >= 8192) {
				RejectDDL("54000", current_stage, "Oracle metadata row bound exceeded");
			}
			vector<string> row;
			for (SQLUSMALLINT col = 1; col <= cols; col++) {
				char buf[8192] = {0};
				SQLLEN ind = 0;
				auto get_rc = SQLGetData(stmt.get(), col, SQL_C_CHAR, buf, sizeof(buf), &ind);
				if (get_rc != SQL_SUCCESS) {
					Check(get_rc);
				}
				if (ind == SQL_NULL_DATA) {
					if (!allow_null) {
						RejectDDL("XX000", current_stage, "unexpected NULL Oracle metadata value");
					}
					row.emplace_back("");
				} else if (ind < 0 || static_cast<idx_t>(ind) >= sizeof(buf)) {
					RejectDDL("54000", current_stage, "Oracle metadata cell bound exceeded");
				} else {
					row.emplace_back(buf, static_cast<size_t>(ind));
				}
				total_bytes += row.back().size();
				if (total_bytes > 1024 * 1024) {
					RejectDDL("54000", current_stage, "Oracle metadata document bound exceeded");
				}
			}
			rows.push_back(std::move(row));
		}
		while ((rc = SQLMoreResults(stmt.get())) != SQL_NO_DATA) {
			Check(rc);
		}
		return rows;
	}

	string Scalar(const string &sql) {
		auto r = Rows(sql, true);
		if (r.size() != 1 || r[0].size() != 1) {
			RejectDDL("XX000", current_stage, "expected scalar Oracle result");
		}
		return r[0][0];
	}

	bool HasInterrupted() const {
		return cancelled.load() || ctx.IsInterrupted();
	}

private:
	ClientContext &ctx;
	ODBCStatementPtr stmt;
	std::mutex mu;
	std::condition_variable cv;
	std::thread watcher;
	uint32_t timeout_sec = 15;
	bool stopped = false, cleanup_mode = false, cancel_sent = true;
	std::chrono::steady_clock::time_point statement_deadline = std::chrono::steady_clock::now();
	std::atomic<bool> cancelled {false};
};

static bool ParseMajorMinor(const string &version, int &major, int &minor) {
	major = 0;
	minor = 0;
	idx_t i = 0;
	while (i < version.size() && version[i] >= '0' && version[i] <= '9') {
		major = major * 10 + (version[i++] - '0');
		if (major > 10000) return false;
	}
	if (i == 0 || i >= version.size() || version[i] != '.') {
		return false;
	}
	i++;
	idx_t start = i;
	while (i < version.size() && version[i] >= '0' && version[i] <= '9') {
		minor = minor * 10 + (version[i++] - '0');
		if (minor > 10000) return false;
	}
	return i > start;
}

struct OracleServerFacts {
	string version;
	int major = 0;
	int minor = 0;
	string service_name;
	string db_identity;
	string server_identity;
	string schema_id;
	string session_user;
	string current_schema;
};

static OracleServerFacts QueryOracleFacts(OracleDDLSession &s, SQLHDBC dbc, const string &schema) {
	SQLCHAR dbms[128] = {0};
	SQLSMALLINT dbms_len = 0;
	if (!SQL_SUCCEEDED(SQLGetInfo(dbc, SQL_DBMS_NAME, dbms, sizeof(dbms), &dbms_len)) ||
	    StringUtil::Lower(reinterpret_cast<char *>(dbms)) != "oracle") {
		RejectDDL("0A000", "source_identity", "connected datasource is not Oracle");
	}
	auto full_col = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_TAB_COLUMNS WHERE OWNER='SYS' AND "
	                         "TABLE_NAME='PRODUCT_COMPONENT_VERSION' AND COLUMN_NAME='VERSION_FULL'");
	auto con_col = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_TAB_COLUMNS WHERE OWNER='SYS' AND "
	                        "TABLE_NAME='V_$LOCKED_OBJECT' AND COLUMN_NAME='CON_ID'");
	const string version_col = full_col == "1" ? "VERSION_FULL" : "VERSION";
	const string con_expr = con_col == "1" ? "NVL(SYS_CONTEXT('USERENV','CON_ID'),'0')" : "'0'";

	auto rows = s.Rows("SELECT "
	                   "NVL((SELECT MAX(" +
	                   version_col +
	                   ") FROM PRODUCT_COMPONENT_VERSION WHERE PRODUCT LIKE 'Oracle Database%'),''),"
	                   "NVL(SYS_CONTEXT('USERENV','SERVICE_NAME'),''),"
	                   "NVL(SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),'')||':'||" +
	                   con_expr +
	                   ","
	                   "NVL(SYS_CONTEXT('USERENV','SERVER_HOST'),'')||':'||NVL(SYS_CONTEXT('USERENV','INSTANCE_NAME'),''),"
	                   "NVL((SELECT MAX(TO_CHAR(USER_ID)) FROM ALL_USERS WHERE USERNAME=" +
	                   Literal(schema) +
	                   "),''),"
	                   "NVL(SYS_CONTEXT('USERENV','SESSION_USER'),''),"
	                   "NVL(SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),'') FROM DUAL");
	if (rows.size() != 1 || rows[0].size() != 7) {
		RejectDDL("0A000", "source_identity", "Oracle identity query failed");
	}
	OracleServerFacts f;
	f.version = rows[0][0];
	f.service_name = rows[0][1];
	f.db_identity = rows[0][2];
	f.server_identity = rows[0][3];
	f.schema_id = rows[0][4];
	f.session_user = rows[0][5];
	f.current_schema = rows[0][6];
	StringUtil::Trim(f.version);
	if (f.version.empty() || f.service_name.empty() || f.db_identity.empty() ||
	    !ParseMajorMinor(f.version, f.major, f.minor)) {
		RejectDDL("0A000", "source_identity", "Oracle version/service identity unavailable");
	}
	return f;
}

// ---------- Observation table functions ----------

struct DDLMetaData : TableFunctionData {
	ODBCCatalog &catalog;
	string schema;
	string kind;
	string name;
	bool describe = false;
	DDLMetaData(ODBCCatalog &c, bool d) : catalog(c), describe(d) {
	}
};

struct DDLState : GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<GlobalTableFunctionState> DDLInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<DDLState>();
}

static unique_ptr<FunctionData> DDLMetadataBind(ClientContext &ctx, TableFunctionBindInput &in,
                                                vector<LogicalType> &types, vector<string> &names) {
	for (auto &v : in.inputs) {
		if (v.IsNull()) {
			RejectLocal();
		}
	}
	auto db_name = TextVal(in.inputs[0]);
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, db_name);
	if (!db || db->GetCatalog().GetCatalogType() != "oracle") {
		throw BinderException("DDL catalog is unavailable");
	}
	bool describe = in.inputs.size() == 4;
	auto d = make_uniq<DDLMetaData>(db->GetCatalog().Cast<ODBCCatalog>(), describe);
	d->schema = TextVal(in.inputs[1]);
	if (!ValidName(d->schema)) {
		RejectLocal();
	}
	if (describe) {
		d->kind = TextVal(in.inputs[2]);
		d->name = TextVal(in.inputs[3]);
		if ((d->kind != "table" && d->kind != "index") || !ValidName(d->name)) {
			RejectLocal();
		}
	}
	names = {"sqlstate", "stage", "document"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(d);
}

static void DDLMetadataScan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<DDLState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &d = in.bind_data->Cast<DDLMetaData>();
	string sqlstate, stage = "source_identity", document;

	ODBCPoolConnection pooled;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<OracleDDLSession> session;
	try {
		if (ctx.IsInterrupted()) {
			RejectDDL("57014", "source_identity", "interrupted");
		}
		if (!d.catalog.GetConnectionPool().TryGetConnection(pooled)) {
			RejectDDL("08006", "source_identity", "connection unavailable");
		}
		connection = pooled.GetConnection();
		session = make_uniq<OracleDDLSession>(ctx, connection->GetConn(), 10);
		auto &s = *session;
		if (s.Scalar("SELECT CASE WHEN DBMS_TRANSACTION.LOCAL_TRANSACTION_ID IS NULL THEN '0' ELSE '1' END FROM DUAL") !=
		    "0") {
			RejectDDL("0A000", "source_identity", "Oracle session has an active transaction");
		}
		auto facts = QueryOracleFacts(s, connection->GetConn(), d.schema);
		bool ns_present = !facts.schema_id.empty();
		bool read_only = false;

		if (!d.describe) {
			document = string("{") + "\"contract_version\":1,\"product\":\"oracle\",\"source_version\":" +
			           JSON(facts.version) + ",\"database\":" + JSON(facts.service_name) +
			           ",\"database_identity\":" + JSON(facts.db_identity) +
			           ",\"server_identity\":" + JSON(facts.server_identity) + ",\"schema\":" + JSON(d.schema) +
			           ",\"namespace_identity\":" + JSON(facts.schema_id) +
			           ",\"namespace_present\":" + (ns_present ? "true" : "false") +
			           ",\"source_read_only\":" + (read_only ? "true" : "false") +
			           ",\"session_identity\":{\"user\":" + JSON(facts.session_user) +
			           ",\"current_schema\":" + JSON(facts.current_schema) +
			           "},\"visibility\":\"CATALOG_READ\",\"executor_available\":false" +
			           ",\"identity_protection\":\"UNVERIFIED\"}";
		} else {
			string presence = "ABSENT";
			string object_identity, observed_kind, owning_table, table_identity;
			string cols_json = "[]", indexes_json = "[]", constraints_json = "[]";
			if (ns_present && d.kind == "table") {
				auto objs = s.Rows("SELECT TO_CHAR(o.OBJECT_ID),o.OBJECT_TYPE FROM ALL_OBJECTS o WHERE o.OWNER=" +
				                   Literal(d.schema) + " AND o.OBJECT_NAME=" + Literal(d.name) +
				                   " AND o.OBJECT_TYPE IN ('TABLE','VIEW') AND o.SUBOBJECT_NAME IS NULL");
				if (objs.size() == 1) {
					presence = "PRESENT";
					object_identity = objs[0][0];
					observed_kind = objs[0][1] == "VIEW" ? "v" : "r";
					auto col_rows = s.Rows(
					    "SELECT TO_CHAR(c.COLUMN_ID),c.COLUMN_NAME,c.DATA_TYPE,"
					    "NVL(TO_CHAR(c.DATA_LENGTH),'0'),NVL(TO_CHAR(c.DATA_PRECISION),''),"
					    "NVL(TO_CHAR(c.DATA_SCALE),''),c.NULLABLE,CASE WHEN c.DEFAULT_LENGTH>0 THEN '1' ELSE '0' END "
					    "FROM ALL_TAB_COLUMNS c WHERE c.OWNER=" +
					    Literal(d.schema) + " AND c.TABLE_NAME=" + Literal(d.name) + " ORDER BY c.COLUMN_ID");
					cols_json = "[";
					for (idx_t idx = 0; idx < col_rows.size() && idx < 256; idx++) {
						if (idx > 0) cols_json += ",";
						auto &r = col_rows[idx];
						cols_json += "{\"ordinal\":" + r[0] + ",\"name\":" + JSON(r[1]) + ",\"type\":" + JSON(r[2]) +
						             ",\"length\":" + r[3] + ",\"precision\":" + JSON(r[4]) +
						             ",\"scale\":" + JSON(r[5]) +
						             ",\"nullable\":" + (r[6] == "Y" ? "true" : "false") +
						             ",\"has_default\":" + (r[7] == "1" ? "true" : "false") + "}";
					}
					cols_json += "]";

					auto idx_rows = s.Rows(
					    "SELECT i.INDEX_NAME,i.INDEX_TYPE,i.UNIQUENESS,i.STATUS FROM ALL_INDEXES i "
					    "WHERE i.TABLE_OWNER=" +
					    Literal(d.schema) + " AND i.TABLE_NAME=" + Literal(d.name) + " ORDER BY i.INDEX_NAME");
					indexes_json = "[";
					for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
						if (idx > 0) indexes_json += ",";
						auto &r = idx_rows[idx];
						indexes_json += "{\"table\":" + JSON(d.name) + ",\"name\":" + JSON(r[0]) +
						                ",\"type\":" + JSON(r[1]) +
						                ",\"unique\":" + (r[2] == "UNIQUE" ? "true" : "false") +
						                ",\"status\":" + JSON(r[3]) + "}";
					}
					indexes_json += "]";

					auto con_rows = s.Rows(
					    "SELECT c.CONSTRAINT_NAME,c.CONSTRAINT_TYPE,c.STATUS,c.VALIDATED "
					    "FROM ALL_CONSTRAINTS c WHERE c.OWNER=" +
					    Literal(d.schema) + " AND c.TABLE_NAME=" + Literal(d.name) +
					    " AND c.CONSTRAINT_TYPE IN ('P','U','R','C') ORDER BY c.CONSTRAINT_NAME");
					constraints_json = "[";
					for (idx_t idx = 0; idx < con_rows.size() && idx < 256; idx++) {
						if (idx > 0) constraints_json += ",";
						auto &r = con_rows[idx];
						constraints_json += "{\"name\":" + JSON(r[0]) + ",\"type\":" + JSON(r[1]) +
						                    ",\"status\":" + JSON(r[2]) + ",\"validated\":" + JSON(r[3]) + "}";
					}
					constraints_json += "]";
				}
			} else if (ns_present && d.kind == "index") {
				auto idx_rows = s.Rows(
				    "SELECT TO_CHAR(o.OBJECT_ID),i.TABLE_NAME,TO_CHAR(t.OBJECT_ID),i.UNIQUENESS "
				    "FROM ALL_INDEXES i "
				    "JOIN ALL_OBJECTS o ON o.OWNER=i.OWNER AND o.OBJECT_NAME=i.INDEX_NAME AND o.OBJECT_TYPE='INDEX' "
				    "JOIN ALL_OBJECTS t ON t.OWNER=i.TABLE_OWNER AND t.OBJECT_NAME=i.TABLE_NAME AND t.OBJECT_TYPE='TABLE' "
				    "WHERE i.OWNER=" +
				    Literal(d.schema) + " AND i.INDEX_NAME=" + Literal(d.name) + " AND o.SUBOBJECT_NAME IS NULL "
				    "AND t.SUBOBJECT_NAME IS NULL ORDER BY i.TABLE_NAME");
				indexes_json = "[";
				for (idx_t idx = 0; idx < idx_rows.size() && idx < 256; idx++) {
					if (idx > 0) indexes_json += ",";
					auto &r = idx_rows[idx];
					indexes_json += "{\"table\":" + JSON(r[1]) + ",\"name\":" + JSON(d.name) +
					                ",\"unique\":" + (r[3] == "UNIQUE" ? "true" : "false") + "}";
				}
				indexes_json += "]";
				if (idx_rows.size() == 1) {
					presence = "PRESENT";
					object_identity = idx_rows[0][0];
					owning_table = idx_rows[0][1];
					table_identity = idx_rows[0][2];
					observed_kind = "i";
				} else if (idx_rows.size() > 1) {
					presence = "UNKNOWN";
				}
			}
			document = string("{") + "\"contract_version\":1,\"product\":\"oracle\",\"source_version\":" +
			           JSON(facts.version) + ",\"database\":" + JSON(facts.service_name) +
			           ",\"database_identity\":" + JSON(facts.db_identity) +
			           ",\"server_identity\":" + JSON(facts.server_identity) + ",\"schema\":" + JSON(d.schema) +
			           ",\"name\":" + JSON(d.name) + ",\"kind\":" + JSON(d.kind) +
			           ",\"namespace_identity\":" + JSON(facts.schema_id) +
			           ",\"namespace_present\":" + (ns_present ? "true" : "false") +
			           ",\"presence\":" + JSON(presence) + ",\"object_identity\":" + JSON(object_identity);
			if (!observed_kind.empty()) {
				document += ",\"observed_kind\":" + JSON(observed_kind);
			}
			if (!owning_table.empty()) {
				document += ",\"owning_table\":" + JSON(owning_table);
			}
			if (!table_identity.empty()) {
				document += ",\"table_identity\":" + JSON(table_identity);
			}
			document += ",\"metadata_complete\":false,\"identity_protection\":\"UNVERIFIED\",\"columns\":" + cols_json +
			            ",\"indexes\":" + indexes_json + ",\"constraints\":" + constraints_json + "}";
		}
		stage.clear();
	} catch (const DDLRejectException &rej) {
		sqlstate = ValidSQLState(rej.sqlstate, "HY000");
		stage = rej.stage;
		document.clear();
	} catch (...) {
		sqlstate = ctx.IsInterrupted() ? "57014" : "08006";
		document.clear();
	}
	session.reset();
	if (connection) {
		connection->Close();
	}
	out.SetCardinality(1);
	out.SetValue(0, 0, Value(sqlstate));
	out.SetValue(1, 0, Value(stage));
	out.SetValue(2, 0, Value(document));
}

// ---------- Guarded execution table function ----------

struct DDLColumnDef {
	string name;
	string type;
	int32_t length = 0;
	int32_t precision = 0;
	int32_t scale = 0;
	bool not_null = false;
	string default_kind;
	string default_value;
	bool primary_key = false;
};

struct OracleDDLIntent {
	string database;
	string schema;
	string operation;
	string action;
	string name;
	string index_name;
	string owner;
	string new_name;
	string column_name;
	string default_kind;
	string default_value;
	string constraint_name;
	string constraint_kind;
	string check_column;
	string check_operator;
	string check_value;
	string reference_schema;
	string reference_table;
	string identity_mode;
	vector<string> constraint_keys;
	vector<string> reference_keys;
	vector<DDLColumnDef> columns;
	vector<string> keys;
	bool conditional = false;
	string primary_key_name;
	vector<string> primary_key_keys;
};

static string FormatOracleDefault(const string &kind, const string &value) {
	if (kind == "null") {
		if (!value.empty()) {
			RejectDDL("0A000", "source_definition", "invalid null default");
		}
		return "NULL";
	}
	if (kind == "integer") {
		if (!IsSignedInt64Literal(value)) {
			RejectDDL("0A000", "source_definition", "invalid integer default");
		}
		return value;
	}
	if (kind == "string") {
		if (value.empty()) {
			RejectDDL("0A000", "source_definition", "Oracle cannot store empty-string default distinctly");
		}
		return Literal(value);
	}
	if (kind == "boolean") {
		RejectDDL("0A000", "source_definition", "Oracle does not support boolean defaults");
	}
	RejectDDL("0A000", "source_definition", "unsupported default kind");
}

static bool IsOracleSystemNotNullCheck(const string &col_name, const string &cond_raw) {
	string norm = cond_raw;
	StringUtil::Trim(norm);
	for (int depth = 0; depth < 4 && norm.size() >= 2 && norm.front() == '(' && norm.back() == ')'; depth++) {
		int bal = 0;
		bool in_quote = false;
		bool wrapped = true;
		for (idx_t idx = 0; idx < norm.size(); idx++) {
			char ch = norm[idx];
			if (in_quote) {
				if (ch == '"') {
					if (idx + 1 < norm.size() && norm[idx + 1] == '"') {
						idx++;
						continue;
					}
					in_quote = false;
				}
				continue;
			}
			if (ch == '"') {
				in_quote = true;
			} else if (ch == '(') {
				bal++;
			} else if (ch == ')') {
				bal--;
				if (bal == 0 && idx + 1 < norm.size()) {
					wrapped = false;
				}
			}
			if (!wrapped || bal < 0) {
				wrapped = false;
				break;
			}
		}
		if (!wrapped || bal != 0 || in_quote) {
			break;
		}
		norm = norm.substr(1, norm.size() - 2);
		StringUtil::Trim(norm);
	}
	const string upper = StringUtil::Upper(norm);
	const string suffix = " IS NOT NULL";
	if (upper.size() <= suffix.size() || !StringUtil::EndsWith(upper, suffix)) {
		return false;
	}
	string lhs = norm.substr(0, norm.size() - suffix.size());
	StringUtil::Trim(lhs);
	return lhs == col_name || lhs == Identifier(col_name);
}

static string MapColumnTypeOracle(const DDLColumnDef &col) {
	if (col.type == "numeric") {
		if (col.length != 0 || col.precision < 1 || col.precision > 38 || col.scale < 0 || col.scale > col.precision) {
			RejectLocal();
		}
		return "NUMBER(" + std::to_string(col.precision) + "," + std::to_string(col.scale) + ")";
	}
	if (col.type == "timestamp" || col.type == "timestamptz") {
		if (col.length != 0 || col.precision < 0 || col.precision > 6 || col.scale != 0) {
			RejectLocal();
		}
		return "TIMESTAMP(" + std::to_string(col.precision) + ")" +
		       (col.type == "timestamptz" ? " WITH TIME ZONE" : "");
	}
	if (col.precision != 0 || col.scale != 0) {
		RejectLocal();
	}
	if (col.type == "varchar") {
		if (col.length < 1 || col.length > 4000) {
			RejectLocal();
		}
		return "VARCHAR2(" + std::to_string(col.length) + " CHAR)";
	}
	if (col.length != 0) {
		RejectLocal();
	}
	if (col.type == "smallint") return "NUMBER(5,0)";
	if (col.type == "integer") return "NUMBER(10,0)";
	if (col.type == "bigint") return "NUMBER(19,0)";
	if (col.type == "real") return "BINARY_FLOAT";
	if (col.type == "double") return "BINARY_DOUBLE";
	if (col.type == "text") return "CLOB";
	if (col.type == "bytea") return "BLOB";
	if (col.type == "date") return "DATE";
	RejectDDL("0A000", "source_definition", "unsupported Oracle column type");
}

static vector<string> ReadStringList(const Value &v, idx_t max_len) {
	if (v.IsNull() || v.type().id() != LogicalTypeId::LIST) {
		RejectLocal();
	}
	vector<string> out;
	const auto &items = ListValue::GetChildren(v);
	if (items.size() > max_len) {
		RejectLocal();
	}
	for (const auto &item : items) {
		auto s = TextVal(item);
		if (!ValidName(s) || std::find(out.begin(), out.end(), s) != out.end()) {
			RejectLocal();
		}
		out.push_back(std::move(s));
	}
	return out;
}

static OracleDDLIntent ParseUnifiedIntent(const vector<Value> &inputs) {
	if (inputs.size() != 5) {
		RejectLocal();
	}
	OracleDDLIntent d;
	d.database = TextVal(inputs[1]);
	d.schema = TextVal(inputs[2]);
	d.operation = TextVal(inputs[3]);
	if (!ValidName(d.database) || !ValidName(d.schema) || !ValidUUID(d.operation)) {
		RejectLocal();
	}
	const auto &sval = inputs[4];
	if (sval.IsNull() || sval.type().id() != LogicalTypeId::STRUCT) {
		RejectLocal();
	}
	const auto &types = StructType::GetChildTypes(sval.type());
	const auto &fields = StructValue::GetChildren(sval);
	const vector<string> expected = {
	    "contract_version", "target_contract",  "action",          "name",             "index_name",
	    "owner",            "new_name",         "column_name",     "default_kind",     "default_value",
	    "constraint_name",  "constraint_kind",  "check_column",    "check_operator",   "check_value",
	    "reference_schema", "reference_table",  "identity_mode",   "constraint_keys",  "reference_keys",
	    "columns",          "keys",             "conditional",     "primary_key_name", "primary_key_keys",
	};
	if (types.size() != expected.size()) {
		RejectLocal();
	}
	for (idx_t i = 0; i < expected.size(); i++) {
		if (types[i].first != expected[i] || fields[i].IsNull()) {
			RejectLocal();
		}
	}
	if (IntVal(fields[0]) != 1 || TextVal(fields[1]) != "EXECUTION_NAME") {
		RejectLocal();
	}
	d.action = TextVal(fields[2]);
	d.name = TextVal(fields[3]);
	d.index_name = TextVal(fields[4]);
	d.owner = TextVal(fields[5]);
	d.new_name = TextVal(fields[6]);
	d.column_name = TextVal(fields[7]);
	d.default_kind = TextVal(fields[8]);
	d.default_value = TextVal(fields[9]);
	d.constraint_name = TextVal(fields[10]);
	d.constraint_kind = TextVal(fields[11]);
	d.check_column = TextVal(fields[12]);
	d.check_operator = TextVal(fields[13]);
	d.check_value = TextVal(fields[14]);
	d.reference_schema = TextVal(fields[15]);
	d.reference_table = TextVal(fields[16]);
	d.identity_mode = TextVal(fields[17]);

	if (!ValidName(d.name)) RejectLocal();
	for (const auto *opt : {&d.index_name, &d.owner, &d.new_name, &d.column_name, &d.constraint_name,
	                        &d.check_column, &d.reference_schema, &d.reference_table}) {
		if (!opt->empty() && !ValidName(*opt)) {
			RejectLocal();
		}
	}
	if (d.default_value.size() > 4096) {
		RejectLocal();
	}
	d.constraint_keys = ReadStringList(fields[18], 32);
	d.reference_keys = ReadStringList(fields[19], 32);
	d.keys = ReadStringList(fields[21], 32);
	d.conditional = BoolVal(fields[22]);
	d.primary_key_name = TextVal(fields[23]);
	if (!d.primary_key_name.empty() && !ValidName(d.primary_key_name)) {
		RejectLocal();
	}
	d.primary_key_keys = ReadStringList(fields[24], 32);

	if (d.conditional && d.action != "create_table" && d.action != "drop_table" && d.action != "add_column" &&
	    d.action != "drop_column" && d.action != "create_index" && d.action != "create_unique_index" &&
	    d.action != "drop_index") {
		RejectLocal();
	}
	if ((!d.primary_key_keys.empty() || !d.primary_key_name.empty()) && d.action != "create_table") {
		RejectLocal();
	}

	if (fields[20].type().id() != LogicalTypeId::LIST) {
		RejectLocal();
	}
	const auto &cols = ListValue::GetChildren(fields[20]);
	if (cols.size() > 256) {
		RejectLocal();
	}
	vector<string> seen_cols;
	idx_t inline_pk_count = 0;
	for (const auto &col : cols) {
		if (col.IsNull() || col.type().id() != LogicalTypeId::STRUCT) {
			RejectLocal();
		}
		const auto &ct = StructType::GetChildTypes(col.type());
		const auto &cv = StructValue::GetChildren(col);
		if (ct.size() != 9 || ct[0].first != "name" || ct[1].first != "type" || ct[2].first != "length" ||
		    ct[3].first != "precision" || ct[4].first != "scale" || ct[5].first != "not_null" ||
		    ct[6].first != "default_kind" || ct[7].first != "default_value" || ct[8].first != "primary_key") {
			RejectLocal();
		}
		DDLColumnDef def;
		def.name = TextVal(cv[0]);
		def.type = TextVal(cv[1]);
		def.length = IntVal(cv[2]);
		def.precision = IntVal(cv[3]);
		def.scale = IntVal(cv[4]);
		def.not_null = BoolVal(cv[5]);
		def.default_kind = TextVal(cv[6]);
		def.default_value = TextVal(cv[7]);
		def.primary_key = BoolVal(cv[8]);
		if (!ValidName(def.name) || std::find(seen_cols.begin(), seen_cols.end(), def.name) != seen_cols.end()) {
			RejectLocal();
		}
		if (def.default_value.size() > 4096) {
			RejectLocal();
		}
		if (def.default_kind.empty() || def.default_kind == "null") {
			if (!def.default_value.empty()) {
				RejectLocal();
			}
		} else if (def.default_kind == "integer") {
			if (!IsSignedInt64Literal(def.default_value)) {
				RejectLocal();
			}
		} else if (def.default_kind == "boolean") {
			if (def.default_value != "true" && def.default_value != "false") {
				RejectLocal();
			}
		} else if (def.default_kind != "string") {
			RejectLocal();
		}
		if (d.action == "alter_column_type" && (def.not_null || !def.default_kind.empty() || def.primary_key)) {
			RejectLocal();
		}
		if (d.action == "add_column" && def.primary_key) {
			RejectLocal();
		}
		if ((def.not_null || def.primary_key) && def.default_kind == "null") {
			RejectLocal();
		}
		if (def.primary_key) {
			inline_pk_count++;
		}
		seen_cols.push_back(def.name);
		d.columns.push_back(std::move(def));
	}
	if (inline_pk_count > 1 || (inline_pk_count > 0 && !d.primary_key_keys.empty())) {
		RejectLocal();
	}
	for (const auto &k : d.primary_key_keys) {
		auto it = std::find_if(d.columns.begin(), d.columns.end(), [&](const DDLColumnDef &c) { return c.name == k; });
		if (it == d.columns.end() || it->default_kind == "null") {
			RejectLocal();
		}
	}
	if (!d.primary_key_name.empty() && inline_pk_count == 0 && d.primary_key_keys.empty()) {
		RejectLocal();
	}
	return d;
}

static void CheckOracleIdentifierBounds(const OracleDDLIntent &i, idx_t max_bytes) {
	auto check = [&](const string &s) {
		if (!s.empty() && s.size() > max_bytes) {
			RejectDDL("42622", "source_definition", "Oracle identifier length exceeds version limit");
		}
	};
	check(i.schema);
	check(i.name);
	check(i.index_name);
	check(i.owner);
	check(i.new_name);
	check(i.column_name);
	check(i.constraint_name);
	check(i.check_column);
	check(i.reference_schema);
	check(i.reference_table);
	check(i.primary_key_name);
	for (const auto &c : i.columns) check(c.name);
	for (const auto &k : i.keys) check(k);
	for (const auto &k : i.constraint_keys) check(k);
	for (const auto &k : i.reference_keys) check(k);
	for (const auto &k : i.primary_key_keys) check(k);
}

struct DDLExecData : TableFunctionData {
	ODBCCatalog &catalog;
	OracleDDLIntent intent;
	bool diagnostics = false;
	DDLExecData(ODBCCatalog &c, OracleDDLIntent d) : catalog(c), intent(std::move(d)) {
	}
};

static unique_ptr<FunctionData> DDLExecBind(ClientContext &ctx, TableFunctionBindInput &in, vector<LogicalType> &types,
                                            vector<string> &names) {
	auto intent = ParseUnifiedIntent(in.inputs);
	auto db = DatabaseManager::Get(ctx).GetDatabase(ctx, TextVal(in.inputs[0]));
	if (!db || db->GetCatalog().GetCatalogType() != "oracle") {
		throw BinderException("DDL catalog is unavailable");
	}
	auto data = make_uniq<DDLExecData>(db->GetCatalog().Cast<ODBCCatalog>(), std::move(intent));
	auto diag = in.named_parameters.find("source_diagnostics");
	if (diag != in.named_parameters.end() && !diag->second.IsNull()) {
		data->diagnostics = diag->second.GetValue<bool>();
	}
	names = {"operation_id", "status", "sqlstate", "stage", "warning_count"};
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::BIGINT};
	if (data->diagnostics) {
		names.push_back("source_error");
		types.push_back(LogicalType::VARCHAR);
	}
	return std::move(data);
}

static void DDLExecScan(ClientContext &ctx, TableFunctionInput &in, DataChunk &out) {
	auto &state = in.global_state->Cast<DDLState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &d = in.bind_data->Cast<DDLExecData>();
	const auto &i = d.intent;

	string status = "CONFIRMED_NOT_EXECUTED";
	string error = "0A000";
	string stage = "source_identity";
	int64_t warnings = 0;
	string source_error;
	bool sent = false;

	ODBCPoolConnection pooled;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<OracleDDLSession> session;
	try {
		if (ctx.IsInterrupted()) {
			RejectDDL("57014", "source_identity", "operation cancelled");
		}
		if (!d.catalog.GetConnectionPool().TryGetConnection(pooled)) {
			RejectDDL("08006", "source_identity", "connection unavailable");
		}
		connection = pooled.GetConnection();
		if (!SQL_SUCCEEDED(SQLSetConnectAttr(connection->GetConn(), SQL_ATTR_ACCESS_MODE,
		                                     reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0))) {
			RejectDDL("25006", "source_identity", "failed to configure read-write connection mode");
		}
		if (!SQL_SUCCEEDED(SQLSetConnectAttr(connection->GetConn(), SQL_ATTR_AUTOCOMMIT,
		                                     reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_OFF), 0))) {
			RejectDDL("08006", "source_identity", "failed to configure session commit boundary");
		}
		session = make_uniq<OracleDDLSession>(ctx, connection->GetConn(), 15);
		auto &s = *session;

		if (s.Scalar("SELECT CASE WHEN DBMS_TRANSACTION.LOCAL_TRANSACTION_ID IS NULL THEN '0' ELSE '1' END FROM DUAL") !=
		    "0") {
			RejectDDL("0A000", "source_identity", "Oracle session has an active transaction");
		}
		auto facts = QueryOracleFacts(s, connection->GetConn(), i.schema);
		if (facts.service_name != i.database) {
			RejectDDL("0A000", "source_identity", "Oracle service identity mismatch");
		}
		if (facts.schema_id.empty()) {
			RejectDDL("3F000", "source_definition", "target schema does not exist");
		}

		// Fail-fast on active lock contention instead of waiting on table locks.
		s.Exec("ALTER SESSION SET DDL_LOCK_TIMEOUT = 0");

		stage = "source_definition";
		s.current_stage = "source_definition";
		const idx_t max_ident = (facts.major < 12 || (facts.major == 12 && facts.minor < 2)) ? 30 : 63;
		CheckOracleIdentifierBounds(i, max_ident);

		const string target = Identifier(i.schema) + "." + Identifier(i.name);
		auto table_exists = [&](const string &tbl) {
			return s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_TABLES WHERE OWNER=" + Literal(i.schema) +
			                " AND TABLE_NAME=" + Literal(tbl)) != "0";
		};
		auto object_exists = [&](const string &obj) {
			return s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_OBJECTS WHERE OWNER=" + Literal(i.schema) +
			                " AND OBJECT_NAME=" + Literal(obj) + " AND SUBOBJECT_NAME IS NULL") != "0";
		};
		auto col_nullable = [&](const string &col) {
			return s.Scalar("SELECT NVL(MAX(NULLABLE),'') FROM ALL_TAB_COLUMNS WHERE OWNER=" + Literal(i.schema) +
			                " AND TABLE_NAME=" + Literal(i.name) + " AND COLUMN_NAME=" + Literal(col));
		};
		auto check_col_dependencies = [&](const string &tbl_name, const string &col_name, const string &nullable) {
			if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_IND_COLUMNS WHERE TABLE_OWNER=" + Literal(i.schema) +
			             " AND TABLE_NAME=" + Literal(tbl_name) + " AND COLUMN_NAME=" + Literal(col_name)) != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify or drop column referenced by an index");
			}
			if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_CONSTRAINTS con "
			             "JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=con.OWNER AND cc.CONSTRAINT_NAME=con.CONSTRAINT_NAME "
			             "AND cc.TABLE_NAME=con.TABLE_NAME WHERE con.OWNER=" +
			             Literal(i.schema) + " AND con.TABLE_NAME=" + Literal(tbl_name) +
			             " AND cc.COLUMN_NAME=" + Literal(col_name) +
			             " AND con.CONSTRAINT_TYPE IN ('P','U','R')") != "0") {
				RejectDDL("2BP01", "source_definition",
				          "cannot modify or drop column referenced by a key or foreign key constraint");
			}
			auto chk_rows = s.Rows(
			    "SELECT con.CONSTRAINT_NAME,con.GENERATED,con.SEARCH_CONDITION FROM ALL_CONSTRAINTS con "
			    "JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=con.OWNER AND cc.CONSTRAINT_NAME=con.CONSTRAINT_NAME "
			    "AND cc.TABLE_NAME=con.TABLE_NAME WHERE con.OWNER=" +
			    Literal(i.schema) + " AND con.TABLE_NAME=" + Literal(tbl_name) +
			    " AND cc.COLUMN_NAME=" + Literal(col_name) + " AND con.CONSTRAINT_TYPE='C'");
			for (const auto &r : chk_rows) {
				if (r[1] == "USER NAME" || !IsOracleSystemNotNullCheck(col_name, r[2]) ||
				    (r[2].empty() && nullable != "N")) {
					RejectDDL("2BP01", "source_definition",
					          "cannot modify or drop column referenced by a check constraint");
				}
			}
			if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_DEPENDENCIES WHERE REFERENCED_OWNER=" + Literal(i.schema) +
			             " AND REFERENCED_NAME=" + Literal(tbl_name) +
			             " AND REFERENCED_TYPE='TABLE' AND TYPE IN ('VIEW','MATERIALIZED VIEW')") != "0") {
				RejectDDL("2BP01", "source_definition", "cannot modify or drop column of table referenced by a view");
			}
		};

		string native_sql;
		bool no_op_commit = false;
		bool source_noop = false;

		if (i.action == "create_table") {
			if (i.columns.empty() || !i.keys.empty() || !i.index_name.empty() || !i.owner.empty() ||
			    !i.new_name.empty() || !i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "invalid create_table fields");
			}
			if (table_exists(i.name)) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42P07", "source_definition", "target object already exists");
				}
			} else if (object_exists(i.name)) {
				RejectDDL("42P07", "source_definition", "target object already exists");
			}
			if (!source_noop) {
				vector<string> eff_pk = i.primary_key_keys;
				if (eff_pk.empty()) {
					for (const auto &c : i.columns) {
						if (c.primary_key) {
							eff_pk.push_back(c.name);
							break;
						}
					}
				}
				vector<string> defs;
				for (const auto &c : i.columns) {
					bool eff_nn = c.not_null || c.primary_key ||
					              std::find(i.primary_key_keys.begin(), i.primary_key_keys.end(), c.name) !=
					                  i.primary_key_keys.end();
					string col_def = Identifier(c.name) + " " + MapColumnTypeOracle(c);
					if (!c.default_kind.empty()) {
						col_def += " DEFAULT " + FormatOracleDefault(c.default_kind, c.default_value);
					}
					col_def += eff_nn ? " NOT NULL" : " NULL";
					defs.push_back(std::move(col_def));
				}
				if (!eff_pk.empty()) {
					vector<string> pk_quoted;
					pk_quoted.reserve(eff_pk.size());
					for (const auto &k : eff_pk) {
						pk_quoted.push_back(Identifier(k));
					}
					string pk_clause;
					if (!i.primary_key_name.empty()) {
						if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_CONSTRAINTS WHERE OWNER=" + Literal(i.schema) +
						             " AND CONSTRAINT_NAME=" + Literal(i.primary_key_name)) != "0") {
							RejectDDL("42710", "source_definition", "constraint name already exists");
						}
						pk_clause = "CONSTRAINT " + Identifier(i.primary_key_name) + " ";
					}
					pk_clause += "PRIMARY KEY (" + StringUtil::Join(pk_quoted, ",") + ")";
					defs.push_back(std::move(pk_clause));
				}
				native_sql = "CREATE TABLE " + target + " (" + StringUtil::Join(defs, ",") + ")";
			}
		} else if (i.action == "drop_table") {
			if (!table_exists(i.name)) {
				if (object_exists(i.name)) {
					RejectDDL("42809", "source_definition", "target object is not a base table");
				}
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42P01", "source_definition", "target table does not exist");
				}
			} else {
				if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_CONSTRAINTS pk "
				             "JOIN ALL_CONSTRAINTS fk ON fk.R_OWNER=pk.OWNER AND fk.R_CONSTRAINT_NAME=pk.CONSTRAINT_NAME "
				             "WHERE pk.OWNER=" +
				             Literal(i.schema) + " AND pk.TABLE_NAME=" + Literal(i.name) +
				             " AND fk.CONSTRAINT_TYPE='R' AND NOT (fk.OWNER=pk.OWNER AND fk.TABLE_NAME=pk.TABLE_NAME)") !=
				    "0") {
					RejectDDL("2BP01", "source_definition", "cannot drop table with referencing foreign keys");
				}
				native_sql = "DROP TABLE " + target;
			}
		} else if (i.action == "rename_table") {
			if (i.new_name.empty() || i.new_name == i.name) {
				RejectDDL("0A000", "source_definition", "invalid rename_table name");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			if (object_exists(i.new_name)) {
				RejectDDL("42P07", "source_definition", "destination table already exists");
			}
			native_sql = "ALTER TABLE " + target + " RENAME TO " + Identifier(i.new_name);
		} else if (i.action == "add_column") {
			if (i.columns.size() != 1 || i.columns[0].primary_key) {
				RejectDDL("0A000", "source_definition", "invalid add_column payload");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			const auto &c = i.columns[0];
			if (!col_nullable(c.name).empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42701", "source_definition", "column already exists");
				}
			} else {
				if (c.not_null && (c.default_kind.empty() || c.default_kind == "null")) {
					if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM " + target + " WHERE ROWNUM=1") != "0") {
						RejectDDL("23502", "source_definition",
						          "cannot add NOT NULL column without default to non-empty table");
					}
				}
				string col_def = Identifier(c.name) + " " + MapColumnTypeOracle(c);
				if (!c.default_kind.empty()) {
					col_def += " DEFAULT " + FormatOracleDefault(c.default_kind, c.default_value);
				}
				col_def += c.not_null ? " NOT NULL" : " NULL";
				native_sql = "ALTER TABLE " + target + " ADD (" + col_def + ")";
			}
		} else if (i.action == "drop_column") {
			if (i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "column name required");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			auto nullable = col_nullable(i.column_name);
			if (nullable.empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42703", "source_definition", "column does not exist");
				}
			} else {
				if (std::stoi(s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_TAB_COLUMNS WHERE OWNER=" + Literal(i.schema) +
				                       " AND TABLE_NAME=" + Literal(i.name))) <= 1) {
					RejectDDL("42P16", "source_definition", "cannot drop the only column of a table");
				}
				check_col_dependencies(i.name, i.column_name, nullable);
				native_sql = "ALTER TABLE " + target + " DROP COLUMN " + Identifier(i.column_name);
			}
		} else if (i.action == "alter_column_type") {
			if (i.column_name.empty() || i.columns.size() != 1 || i.columns[0].name != i.column_name) {
				RejectDDL("0A000", "source_definition", "invalid alter_column_type payload");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			auto col_rows = s.Rows(
			    "SELECT DATA_TYPE,NVL(TO_CHAR(DATA_LENGTH),'0'),NVL(TO_CHAR(CHAR_LENGTH),'0'),"
			    "NVL(CHAR_USED,''),NVL(TO_CHAR(DATA_PRECISION),''),NVL(TO_CHAR(DATA_SCALE),''),NULLABLE "
			    "FROM ALL_TAB_COLUMNS WHERE OWNER=" +
			    Literal(i.schema) + " AND TABLE_NAME=" + Literal(i.name) + " AND COLUMN_NAME=" + Literal(i.column_name));
			if (col_rows.size() != 1) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			auto has_vc = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_TAB_COLUMNS WHERE OWNER='SYS' AND "
			                       "TABLE_NAME='ALL_TAB_COLS' AND COLUMN_NAME='VIRTUAL_COLUMN'") != "0";
			auto has_ic = s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_TAB_COLUMNS WHERE OWNER='SYS' AND "
			                       "TABLE_NAME='ALL_TAB_COLS' AND COLUMN_NAME='IDENTITY_COLUMN'") != "0";
			if (has_vc || has_ic) {
				string vc_expr = has_vc ? "NVL(VIRTUAL_COLUMN,'NO')" : "'NO'";
				string ic_expr = has_ic ? "NVL(IDENTITY_COLUMN,'NO')" : "'NO'";
				auto flag_rows = s.Rows("SELECT " + vc_expr + "," + ic_expr +
				                        " FROM ALL_TAB_COLS WHERE OWNER=" + Literal(i.schema) +
				                        " AND TABLE_NAME=" + Literal(i.name) +
				                        " AND COLUMN_NAME=" + Literal(i.column_name));
				if (flag_rows.size() == 1 && (flag_rows[0][0] == "YES" || flag_rows[0][1] == "YES")) {
					RejectDDL("0A000", "source_definition", "cannot alter virtual or identity column");
				}
			}
			const auto &r = col_rows[0];
			check_col_dependencies(i.name, i.column_name, r[6]);
			const string dtype = StringUtil::Upper(r[0]);
			const int data_len = std::stoi(r[1]);
			const int char_len = std::stoi(r[2]);
			const string char_used = StringUtil::Upper(r[3]);
			const string prec_str = r[4];
			const string scale_str = r[5];
			const auto &target_col = i.columns[0];
			string new_type;

			if (dtype == "NUMBER") {
				if (!prec_str.empty() && !scale_str.empty()) {
					int p1 = std::stoi(prec_str);
					int s1 = std::stoi(scale_str);
					if (target_col.type == "integer") {
						if (s1 == 0 && p1 == 5) {
							new_type = "NUMBER(10,0)";
						}
					} else if (target_col.type == "bigint") {
						if (s1 == 0 && (p1 == 5 || p1 == 10)) {
							new_type = "NUMBER(19,0)";
						}
					} else if (target_col.type == "numeric") {
						if (target_col.length == 0 && s1 == target_col.scale && target_col.precision > p1 &&
						    target_col.precision <= 38) {
							new_type = "NUMBER(" + std::to_string(target_col.precision) + "," +
							           std::to_string(target_col.scale) + ")";
						}
					}
				}
			} else if (dtype == "BINARY_FLOAT" && target_col.type == "double") {
				new_type = "BINARY_DOUBLE";
			} else if (dtype == "VARCHAR2" && target_col.type == "varchar") {
				if (target_col.length >= 1 && target_col.length <= 4000) {
					if (char_used == "C" && target_col.length > char_len) {
						new_type = "VARCHAR2(" + std::to_string(target_col.length) + " CHAR)";
					} else if (char_used == "B" && target_col.length > data_len) {
						new_type = "VARCHAR2(" + std::to_string(target_col.length) + " BYTE)";
					}
				}
			} else if (StringUtil::StartsWith(dtype, "TIMESTAMP(") && dtype.size() >= 12 && dtype[10] >= '0' &&
			           dtype[10] <= '6' && dtype[11] == ')') {
				int p1 = dtype[10] - '0';
				const string rest = dtype.substr(12);
				if (rest.empty() && target_col.type == "timestamp") {
					if (target_col.precision > p1 && target_col.precision <= 6 && target_col.scale == 0 &&
					    target_col.length == 0) {
						new_type = "TIMESTAMP(" + std::to_string(target_col.precision) + ")";
					}
				} else if (rest == " WITH TIME ZONE" && target_col.type == "timestamptz") {
					if (target_col.precision > p1 && target_col.precision <= 6 && target_col.scale == 0 &&
					    target_col.length == 0) {
						new_type = "TIMESTAMP(" + std::to_string(target_col.precision) + ") WITH TIME ZONE";
					}
				}
			}
			if (new_type.empty()) {
				RejectDDL("0A000", "source_definition", "only lossless type widening is supported");
			}
			native_sql = "ALTER TABLE " + target + " MODIFY (" + Identifier(i.column_name) + " " + new_type + ")";
		} else if (i.action == "rename_column") {
			if (i.column_name.empty() || i.new_name.empty() || i.new_name == i.column_name) {
				RejectDDL("0A000", "source_definition", "invalid rename_column payload");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			if (col_nullable(i.column_name).empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			if (!col_nullable(i.new_name).empty()) {
				RejectDDL("42701", "source_definition", "destination column already exists");
			}
			native_sql = "ALTER TABLE " + target + " RENAME COLUMN " + Identifier(i.column_name) + " TO " +
			             Identifier(i.new_name);
		} else if (i.action == "set_default" || i.action == "drop_default") {
			if (i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "column name required");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			if (col_nullable(i.column_name).empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			if (i.action == "drop_default") {
				native_sql = "ALTER TABLE " + target + " MODIFY (" + Identifier(i.column_name) + " DEFAULT NULL)";
			} else {
				native_sql = "ALTER TABLE " + target + " MODIFY (" + Identifier(i.column_name) + " DEFAULT " +
				             FormatOracleDefault(i.default_kind, i.default_value) + ")";
			}
		} else if (i.action == "set_not_null" || i.action == "drop_not_null") {
			if (i.column_name.empty()) {
				RejectDDL("0A000", "source_definition", "column name required");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			auto nullable = col_nullable(i.column_name);
			if (nullable.empty()) {
				RejectDDL("42703", "source_definition", "column does not exist");
			}
			if (i.action == "set_not_null") {
				if (nullable == "N") {
					no_op_commit = true;
				} else {
					native_sql = "ALTER TABLE " + target + " MODIFY (" + Identifier(i.column_name) + " NOT NULL)";
				}
			} else {
				if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_CONSTRAINTS con "
				             "JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=con.OWNER AND cc.CONSTRAINT_NAME=con.CONSTRAINT_NAME "
				             "WHERE con.OWNER=" +
				             Literal(i.schema) + " AND con.TABLE_NAME=" + Literal(i.name) +
				             " AND con.CONSTRAINT_TYPE='P' AND con.STATUS='ENABLED' AND cc.COLUMN_NAME=" +
				             Literal(i.column_name)) != "0") {
					RejectDDL("42P16", "source_definition", "cannot drop NOT NULL on primary key column");
				}
				if (nullable == "Y") {
					no_op_commit = true;
				} else {
					native_sql = "ALTER TABLE " + target + " MODIFY (" + Identifier(i.column_name) + " NULL)";
				}
			}
		} else if (i.action == "add_constraint") {
			if (i.constraint_name.empty() || i.constraint_kind.empty()) {
				RejectDDL("0A000", "source_definition", "invalid add_constraint payload");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_CONSTRAINTS WHERE OWNER=" + Literal(i.schema) +
			             " AND CONSTRAINT_NAME=" + Literal(i.constraint_name)) != "0") {
				RejectDDL("42710", "source_definition", "constraint name already exists");
			}
			auto quote_list = [](const vector<string> &items) {
				vector<string> quoted;
				quoted.reserve(items.size());
				for (const auto &it : items) quoted.push_back(Identifier(it));
				return StringUtil::Join(quoted, ",");
			};
			if (i.constraint_kind == "primary_key" || i.constraint_kind == "unique") {
				if (i.constraint_keys.empty()) RejectDDL("0A000", "source_definition", "constraint keys required");
				native_sql = "ALTER TABLE " + target + " ADD CONSTRAINT " + Identifier(i.constraint_name) +
				             (i.constraint_kind == "primary_key" ? " PRIMARY KEY (" : " UNIQUE (") +
				             quote_list(i.constraint_keys) + ")";
			} else if (i.constraint_kind == "foreign_key") {
				if (i.constraint_keys.empty() || i.reference_table.empty() ||
				    i.reference_keys.size() != i.constraint_keys.size() || i.reference_schema != i.schema) {
					RejectDDL("0A000", "source_definition", "invalid foreign key definition");
				}
				if (!table_exists(i.reference_table)) {
					RejectDDL("42P01", "source_definition", "referenced table does not exist");
				}
				native_sql = "ALTER TABLE " + target + " ADD CONSTRAINT " + Identifier(i.constraint_name) +
				             " FOREIGN KEY (" + quote_list(i.constraint_keys) + ") REFERENCES " +
				             Identifier(i.reference_schema) + "." + Identifier(i.reference_table) + " (" +
				             quote_list(i.reference_keys) + ")";
			} else if (i.constraint_kind == "check") {
				if (i.check_column.empty() || i.check_value.empty() || i.check_value == "-") {
					RejectDDL("0A000", "source_definition", "invalid check constraint");
				}
				if (i.check_operator != "=" && i.check_operator != "<>" && i.check_operator != "<" &&
				    i.check_operator != ">" && i.check_operator != "<=" && i.check_operator != ">=") {
					RejectDDL("0A000", "source_definition", "unsupported check operator");
				}
				for (idx_t idx = 0; idx < i.check_value.size(); idx++) {
					char c = i.check_value[idx];
					if (!((c >= '0' && c <= '9') || (idx == 0 && c == '-'))) {
						RejectDDL("0A000", "source_definition", "invalid integer check literal");
					}
				}
				native_sql = "ALTER TABLE " + target + " ADD CONSTRAINT " + Identifier(i.constraint_name) + " CHECK (" +
				             Identifier(i.check_column) + " " + i.check_operator + " " + i.check_value + ")";
			} else {
				RejectDDL("0A000", "source_definition", "unsupported constraint kind");
			}
		} else if (i.action == "drop_constraint") {
			if (i.constraint_name.empty()) {
				RejectDDL("0A000", "source_definition", "constraint name required");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_CONSTRAINTS WHERE OWNER=" + Literal(i.schema) +
			             " AND TABLE_NAME=" + Literal(i.name) + " AND CONSTRAINT_NAME=" +
			             Literal(i.constraint_name)) == "0") {
				RejectDDL("42704", "source_definition", "constraint does not exist");
			}
			native_sql = "ALTER TABLE " + target + " DROP CONSTRAINT " + Identifier(i.constraint_name);
		} else if (i.action == "create_index" || i.action == "create_unique_index") {
			if (i.index_name.empty() || i.keys.empty()) {
				RejectDDL("0A000", "source_definition", "index name and keys required");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			auto existing_idx = s.Rows("SELECT TABLE_NAME FROM ALL_INDEXES WHERE OWNER=" + Literal(i.schema) +
			                           " AND INDEX_NAME=" + Literal(i.index_name));
			if (!existing_idx.empty()) {
				if (i.conditional && existing_idx[0][0] == i.name) {
					source_noop = true;
				} else {
					RejectDDL("42P07", "source_definition", "index name already exists in schema");
				}
			} else if (object_exists(i.index_name)) {
				RejectDDL("42P07", "source_definition", "index name already exists in schema");
			}
			if (!source_noop) {
				vector<string> cols;
				for (const auto &k : i.keys) cols.push_back(Identifier(k));
				native_sql = string(i.action == "create_unique_index" ? "CREATE UNIQUE INDEX " : "CREATE INDEX ") +
				             Identifier(i.schema) + "." + Identifier(i.index_name) + " ON " + target + " (" +
				             StringUtil::Join(cols, ",") + ")";
			}
		} else if (i.action == "drop_index") {
			auto idx_rows = s.Rows("SELECT TABLE_NAME FROM ALL_INDEXES WHERE OWNER=" + Literal(i.schema) +
			                       " AND INDEX_NAME=" + Literal(i.name));
			if (idx_rows.empty()) {
				if (i.conditional) {
					source_noop = true;
				} else {
					RejectDDL("42704", "source_definition", "index does not exist");
				}
			} else if (!i.owner.empty() && idx_rows[0][0] != i.owner) {
				RejectDDL("42704", "source_definition", "index owner mismatch");
			} else {
				native_sql = "DROP INDEX " + target;
			}
		} else if (i.action == "rename_index") {
			if (i.new_name.empty() || i.new_name == i.name) {
				RejectDDL("0A000", "source_definition", "invalid rename_index payload");
			}
			auto idx_rows = s.Rows("SELECT TABLE_NAME FROM ALL_INDEXES WHERE OWNER=" + Literal(i.schema) +
			                       " AND INDEX_NAME=" + Literal(i.name));
			if (idx_rows.empty()) {
				RejectDDL("42704", "source_definition", "index does not exist");
			}
			if (!i.owner.empty() && idx_rows[0][0] != i.owner) {
				RejectDDL("42704", "source_definition", "index owner mismatch");
			}
			if (object_exists(i.new_name)) {
				RejectDDL("42P07", "source_definition", "destination index name already exists in schema");
			}
			native_sql = "ALTER INDEX " + target + " RENAME TO " + Identifier(i.new_name);
		} else if (i.action == "truncate") {
			if (i.identity_mode == "restart") {
				RejectDDL("0A000", "source_definition", "Oracle TRUNCATE RESTART IDENTITY is unsupported");
			}
			if (!table_exists(i.name)) {
				RejectDDL("42P01", "source_definition", "target table does not exist");
			}
			if (s.Scalar("SELECT TO_CHAR(COUNT(*)) FROM ALL_CONSTRAINTS pk "
			             "JOIN ALL_CONSTRAINTS fk ON fk.R_OWNER=pk.OWNER AND fk.R_CONSTRAINT_NAME=pk.CONSTRAINT_NAME "
			             "WHERE pk.OWNER=" +
			             Literal(i.schema) + " AND pk.TABLE_NAME=" + Literal(i.name) +
			             " AND fk.CONSTRAINT_TYPE='R' AND fk.STATUS='ENABLED'") != "0") {
				RejectDDL("2BP01", "source_definition", "cannot truncate table with enabled referencing foreign keys");
			}
			native_sql = "TRUNCATE TABLE " + target;
		} else {
			RejectDDL("0A000", "source_definition", "unsupported Oracle DDL action");
		}

		stage = "source_execute";
		s.current_stage = "source_execute";
		if (!no_op_commit && !source_noop) {
			if (ctx.IsInterrupted()) {
				RejectDDL("57014", "source_execute", "operation cancelled");
			}
			s.last_warning_count = 0;
			sent = true;
			s.Exec(native_sql);
		}
		status = "COMMITTED";
		if (source_noop) {
			warnings = 0;
			error.clear();
			stage = "source_noop";
		} else {
			warnings = s.last_warning_count;
			error.clear();
			stage = warnings > 0 ? "source_warning" : "";
		}
	} catch (const GuardedODBCLocalRejection &) {
		error = status == "COMMITTED" ? "XX000" : "0A000";
		stage = "source_identity";
	} catch (const DDLRejectException &rej) {
		if (source_error.empty() && session) {
			source_error = session->source.JSON();
		}
		error = ValidSQLState(rej.sqlstate, "HY000");
		stage = rej.stage;
		if (sent && status != "COMMITTED") {
			if (session && IsOracleConfirmedNotExecuted(session->last_native)) {
				status = "CONFIRMED_NOT_EXECUTED";
				if (session->last_native == 54 || session->last_native == 30006) {
					stage = "source_lock";
				}
			} else {
				status = "UNKNOWN";
			}
		}
	} catch (...) {
		if (source_error.empty() && session) {
			source_error = session->source.JSON();
		}
		error = session && !session->error.empty() ? session->error : (ctx.IsInterrupted() ? "57014" : "08006");
		if (sent && status != "COMMITTED") {
			status = "UNKNOWN";
		}
	}

	if (status != "COMMITTED") {
		warnings = 0;
		if (stage == "source_warning" || stage == "source_verify") {
			stage = "source_execute";
		}
		if (status != "CONFIRMED_NOT_EXECUTED" && (stage == "source_lock" || stage == "source_definition")) {
			stage = "source_execute";
		}
		error = ValidSQLState(error, "HY000");
	}

	if (session) {
		try {
			session->Exec("ROLLBACK", true);
		} catch (...) {
		}
	}
	session.reset();
	if (connection) {
		connection->Close();
	}

	out.SetCardinality(1);
	out.SetValue(0, 0, Value(i.operation));
	out.SetValue(1, 0, Value(status));
	out.SetValue(2, 0, Value(error));
	out.SetValue(3, 0, Value(stage));
	out.SetValue(4, 0, Value::BIGINT(warnings));
	if (d.diagnostics) {
		out.SetValue(5, 0, Value(source_error));
	}
}
} // namespace

void RegisterOracleGuardedDDL(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction("oracle_ddl_capabilities_v1", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                      DDLMetadataScan, DDLMetadataBind, DDLInit));
	loader.RegisterFunction(TableFunction(
	    "oracle_ddl_describe_v1",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, DDLMetadataScan,
	    DDLMetadataBind, DDLInit));
	TableFunction exec("oracle_guarded_ddl_v1",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                    LogicalType::ANY},
	                   DDLExecScan, DDLExecBind, DDLInit);
	exec.named_parameters["source_diagnostics"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(exec);
}
} // namespace duckdb
