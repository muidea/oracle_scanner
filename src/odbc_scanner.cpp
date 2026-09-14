#include "duckdb.hpp"
#include "odbc_scanner.hpp"
#include "odbc_utils.hpp"
#include "odbc_filter_pushdown.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "storage/odbc_connection_pool.hpp"
#include "storage/odbc_table_set.hpp"
#include "odbc_filter_pushdown.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include <chrono>
#include <thread>
#include <mutex>
#include <unordered_map>

namespace duckdb {

// Keep one native relational metadata statement per attached Oracle catalog.
// Independent catalogs use independent ODBC connections and must not block
// each other. Same-catalog admission is bounded so overload fails explicitly
// before the outer PostgreSQL execution timeout.
static std::mutex relational_metadata_locks_mutex;
static std::unordered_map<string, std::shared_ptr<std::timed_mutex>> relational_metadata_locks;

static std::shared_ptr<std::timed_mutex> OracleRelationalMetadataLock(const string &catalog_name) {
	lock_guard<std::mutex> guard(relational_metadata_locks_mutex);
	auto &entry = relational_metadata_locks[catalog_name];
	if (!entry) {
		entry = std::make_shared<std::timed_mutex>();
	}
	return entry;
}

struct ODBCGlobalState;

struct ODBCLocalState : public LocalTableFunctionState {
	ODBCPoolConnection pool_connection;
	shared_ptr<ODBCConnection> connection;
	unique_ptr<ODBCResult> result;
};

struct ODBCGlobalState : public GlobalTableFunctionState {
	explicit ODBCGlobalState(unique_ptr<ODBCResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<ODBCResult> result;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static string NormalizeSQLServerDateTimeOffset(string input) {
	StringUtil::Trim(input);
	auto sign_pos = input.find_last_of("+-");
	if (sign_pos != string::npos && sign_pos > 0 && input[sign_pos - 1] == ' ') {
		input.erase(sign_pos - 1, 1);
	}
	return input;
}

static bool TryCastDateTimeOffsetVector(Vector &res_vec, Vector &output_vec, idx_t count, string &error) {
	if (res_vec.GetType().id() != LogicalTypeId::VARCHAR || output_vec.GetType().id() != LogicalTypeId::TIMESTAMP_TZ) {
		return false;
	}
	auto src = FlatVector::GetData<string_t>(res_vec);
	auto dst = FlatVector::GetData<timestamp_tz_t>(output_vec);
	for (idx_t r = 0; r < count; r++) {
		if (FlatVector::IsNull(res_vec, r)) {
			FlatVector::SetNull(output_vec, r, true);
			continue;
		}
		auto s = src[r];
		string raw(s.GetData(), s.GetSize());
		string normalized = NormalizeSQLServerDateTimeOffset(raw);
		timestamp_t parsed_ts;
		if (Timestamp::TryConvertTimestamp(normalized.c_str(), normalized.size(), parsed_ts, true) !=
		    TimestampCastResult::SUCCESS) {
			error = StringUtil::Format("invalid timestamp field format: \"%s\"", raw);
			return false;
		}
		dst[r] = timestamp_tz_t(parsed_ts.value);
	}
	error.clear();
	return true;
}

static unique_ptr<FunctionData> ODBCBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("ODBCBind");
}

static unique_ptr<GlobalTableFunctionState> ODBCInitGlobalState(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ODBCBindData>();
	// generate the SELECT statement
	string select;
	select += "SELECT ";

	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			select += "NULL";
		} else {
			auto column_id = input.column_ids[c];
			string col_name;
			if (column_id < bind_data.remote_names.size()) {
				col_name = bind_data.remote_names[column_id];
			} else {
				auto &col = bind_data.table.GetColumn(LogicalIndex(column_id));
				col_name = col.GetName();
			}
			select += ODBCUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += ODBCUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += ODBCUtils::WriteIdentifier(bind_data.table.name);
	auto &filter_names = bind_data.remote_names.empty() ? bind_data.names : bind_data.remote_names;
	string filter_string = ODBCFilterPushdown::TransformFilters(input.column_ids, input.filters, filter_names);
	if (!filter_string.empty()) {
		select += " WHERE (" + filter_string + ")";
	}
	if (!bind_data.limit.empty()) {
		select += filter_string.empty() ? " WHERE " : " AND ";
		select += "ROWNUM <= " + bind_data.limit;
	}

	// run the query
	auto &transaction = ODBCTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();

	auto query_result = con.Query(select, bind_data.streaming);

	// 检查查询结果是否为空
	if (!query_result) {
		throw InternalException("ODBCInitGlobalState: Query returned null result for table %s.%s",
		                        bind_data.table.schema.name.c_str(), bind_data.table.name.c_str());
	}

	auto result = make_uniq<ODBCGlobalState>(std::move(query_result));

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ODBCInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
	return make_uniq<ODBCLocalState>();
}

static void ODBCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ODBCGlobalState>();

	if (!gstate.result) {
		throw InternalException("ODBCScan: result pointer is null");
	}

	DataChunk &res_chunk = gstate.result->NextChunk();

	// 空结果集，设置输出为空
	if (res_chunk.size() == 0) {
		output.SetCardinality(0);
		return;
	}

	// 没有列的结果集，设置输出为空
	if (res_chunk.ColumnCount() == 0) {
		output.SetCardinality(0);
		return;
	}

	// 检查列数匹配
	if (output.ColumnCount() != res_chunk.ColumnCount()) {
		throw InternalException("ODBCScan: column count mismatch. Output has %lu columns, result has %lu columns",
		                        output.ColumnCount(), res_chunk.ColumnCount());
	}

	string error;
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		Vector &output_vec = output.data[c];
		Vector &res_vec = res_chunk.data[c];
		error.clear();

		// 检查向量有效性
		if (output_vec.GetType().id() == LogicalTypeId::INVALID) {
			throw InternalException("ODBCScan: output vector %lu has invalid type", c);
		}
		if (res_vec.GetType().id() == LogicalTypeId::INVALID) {
			throw InternalException("ODBCScan: result vector %lu has invalid type", c);
		}

		if (res_vec.GetType() == output_vec.GetType()) {
			// A same-type TryCast is DuckDB's NopCast and only references the
			// source vector. Materialized ODBC chunks are owned by the function
			// state, so the output must receive an independent copy.
			VectorOperations::Copy(res_vec, output_vec, res_chunk.size(), 0, 0);
		} else {
			VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
		}

		if (!error.empty()) {
			if (TryCastDateTimeOffsetVector(res_vec, output_vec, res_chunk.size(), error)) {
				continue;
			}
		}

		// throw an error in case of TryCast fails
		if (!error.empty()) {
			throw BinderException(error);
		}
	}
	output.SetCardinality(res_chunk.size());
}

static InsertionOrderPreservingMap<string> ODBCScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<ODBCBindData>();
	result["Table"] = bind_data.table.name;
	return result;
}

static void ODBCScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                              const TableFunction &function) {
	throw NotImplementedException("ODBCScanSerialize");
}

static unique_ptr<FunctionData> ODBCScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("ODBCScanDeserialize");
}

static BindInfo ODBCGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ODBCBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table;
	return info;
}

ODBCScanFunction::ODBCScanFunction()
    : TableFunction("oracle_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan,
                    ODBCBind, ODBCInitGlobalState, ODBCInitLocalState) {
	to_string = ODBCScanToString;
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
	get_bind_info = ODBCGetBindInfo;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// ODBC Query
//===--------------------------------------------------------------------===//
static void ValidateOracleReadOnlyQuery(const string &sql) {
	auto normalized = StringUtil::Lower(sql);
	StringUtil::Trim(normalized);
	if (!normalized.empty() && normalized.back() == ';') {
		normalized.pop_back();
		StringUtil::Trim(normalized);
	}
	if (normalized.find(';') != string::npos) {
		throw PermissionException("oracle_query accepts exactly one read-only statement");
	}
	if (!StringUtil::StartsWith(normalized, "select") && !StringUtil::StartsWith(normalized, "with")) {
		throw PermissionException("oracle_query only accepts SELECT or WITH queries");
	}
}

static unique_ptr<FunctionData> ODBCQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to oracle_query cannot be NULL");
	}

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in oracle_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "oracle") {
		throw BinderException("Attached database \"%s\" does not refer to an Oracle database", db_name);
	}
	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto sql = input.inputs[1].GetValue<string>();
	ValidateOracleReadOnlyQuery(sql);

	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to oracle_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}

	auto result = transaction.GetConnection().Query(sql, std::move(params), ODBCResultStreaming::FORCE_MATERIALIZATION);
	for (auto &field : result->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.duckdb_type);
	}
	if (return_types.empty()) {
		throw InvalidInputException("Failed to fetch return types for query '%s'", sql);
	}
	return make_uniq<ODBCQueryBindData>(catalog, std::move(result), std::move(sql));
}

static unique_ptr<GlobalTableFunctionState> ODBCQueryInitGlobalState(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<ODBCQueryBindData>();
	unique_ptr<ODBCResult> odbc_result;
	if (bind_data.result) {
		odbc_result = std::move(bind_data.result);
	} else {
		auto &transaction = ODBCTransaction::Get(context, bind_data.catalog);
		odbc_result = transaction.GetConnection().Query(bind_data.query, ODBCResultStreaming::FORCE_MATERIALIZATION);
	}

	return make_uniq<ODBCGlobalState>(std::move(odbc_result));
}

ODBCQueryFunction::ODBCQueryFunction()
    : TableFunction("oracle_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan, ODBCQueryBind,
                    ODBCQueryInitGlobalState, ODBCInitLocalState) {
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
}

//===--------------------------------------------------------------------===//
// Oracle relational metadata
//===--------------------------------------------------------------------===//
static constexpr uint32_t ORACLE_RELATIONAL_QUERY_TIMEOUT_SECONDS = 60;

struct OracleRelationalCapabilities {
	bool search_condition_vc;
	bool identity_columns;
	bool index_expressions;
	bool virtual_columns;
};

static string OracleMetadataLiteral(const string &value) {
	return "'" + StringUtil::Replace(value, "'", "''") + "'";
}

static string OracleCapabilitySQL() {
	return R"SQL(SELECT CAST(COUNT(CASE WHEN table_name='ALL_CONSTRAINTS' AND column_name='SEARCH_CONDITION_VC' THEN 1 END) AS varchar2(20)) AS search_condition_vc_count,CAST(COUNT(CASE WHEN table_name='ALL_TAB_IDENTITY_COLS' THEN 1 END) AS varchar2(20)) AS identity_columns_count,CAST(COUNT(CASE WHEN table_name='ALL_IND_EXPRESSIONS' THEN 1 END) AS varchar2(20)) AS index_expressions_count,CAST(COUNT(CASE WHEN table_name='ALL_TAB_COLS' AND column_name IN ('VIRTUAL_COLUMN','HIDDEN_COLUMN') THEN 1 END) AS varchar2(20)) AS virtual_columns_count FROM all_tab_columns WHERE owner='SYS' AND table_name IN ('ALL_CONSTRAINTS','ALL_TAB_IDENTITY_COLS','ALL_IND_EXPRESSIONS','ALL_TAB_COLS'))SQL";
}

static idx_t OracleCapabilityCount(string value, const string &name) {
	StringUtil::Trim(value);
	if (value.empty() || value.size() > 9) {
		throw InvalidInputException("Invalid Oracle capability count for %s: \"%s\"", name, value);
	}
	idx_t result = 0;
	for (auto ch : value) {
		if (ch < '0' || ch > '9') {
			throw InvalidInputException("Invalid Oracle capability count for %s: \"%s\"", name, value);
		}
		result = result * 10 + static_cast<idx_t>(ch - '0');
	}
	return result;
}

static OracleRelationalCapabilities ProbeOracleRelationalCapabilities(ODBCConnection &connection) {
	auto result = connection.QueryWithTimeout(OracleCapabilitySQL(), ODBCResultStreaming::FORCE_MATERIALIZATION,
	                                          ORACLE_RELATIONAL_QUERY_TIMEOUT_SECONDS);
	if (result->Fields().size() != 4 || !result->Next()) {
		throw InvalidInputException("Unexpected Oracle relational metadata capability result");
	}
	OracleRelationalCapabilities capabilities {
	    OracleCapabilityCount(result->GetString(0), "SEARCH_CONDITION_VC") > 0,
	    OracleCapabilityCount(result->GetString(1), "ALL_TAB_IDENTITY_COLS") > 0,
	    OracleCapabilityCount(result->GetString(2), "ALL_IND_EXPRESSIONS") > 0,
	    OracleCapabilityCount(result->GetString(3), "ALL_TAB_COLS virtual columns") == 2};
	if (result->Next()) {
		throw InvalidInputException("Oracle relational metadata capability result returned more than one row");
	}
	return capabilities;
}

static string OracleRelationalMetadataSQL(const string &category, const string &owner,
                                          const OracleRelationalCapabilities &capabilities) {
	if (category == "columnProperties") {
		auto identity_join = string();
		auto identity_generation = string("''");
		if (capabilities.identity_columns) {
			identity_join =
			    " LEFT JOIN all_tab_identity_cols idc ON idc.owner=c.owner AND idc.table_name=c.table_name AND "
			    "idc.column_name=c.column_name";
			identity_generation = "NVL(idc.generation_type,'')";
		}
		auto column_catalog = string("all_tab_columns");
		auto virtual_column = string("''");
		auto visible_condition = string();
		if (capabilities.virtual_columns) {
			column_catalog = "all_tab_cols";
			virtual_column = "c.virtual_column";
			visible_condition = " AND c.hidden_column='NO'";
		}
		return "SELECT c.owner AS owner_name,c.table_name AS table_name,c.column_name AS column_name,TO_CHAR(c."
		       "column_id) AS column_id,c.nullable AS is_nullable,c.data_default AS default_definition," +
		       identity_generation + " AS identity_generation," + virtual_column +
		       " AS virtual_column,c.data_type AS data_type FROM " + column_catalog + " c" + identity_join +
		       " WHERE c.owner=" + OracleMetadataLiteral(owner) + visible_condition +
		       " ORDER BY c.table_name,c.column_id";
	}
	if (category == "constraints") {
		// SEARCH_CONDITION is LONG on Oracle 11g. ODBC drivers cannot bind it
		// safely in a mixed catalog result, so absence of SEARCH_CONDITION_VC
		// is an explicit partial capability instead of a legacy fallback.
		auto check_expression =
		    string(capabilities.search_condition_vc ? "c.search_condition_vc" : "CAST(NULL AS varchar2(1))");
		return "SELECT c.owner AS owner_name,c.table_name AS table_name,c.constraint_name AS constraint_name,c."
		       "constraint_type AS constraint_type,NVL(TO_CHAR(cc.position),'') AS column_position,NVL(cc.column_name,"
		       "'') AS column_name,NVL(rc.owner,'') AS referenced_owner,NVL(rc.table_name,'') AS referenced_table,NVL("
		       "rcc.column_name,'') AS referenced_column,NVL(c.delete_rule,'') AS delete_rule,c.status AS "
		       "constraint_status,c.validated AS validated,c.deferrable AS deferrable,c.deferred AS deferred," +
		       check_expression +
		       " AS check_definition,c.generated AS generated_name,NVL(c.index_owner,'') AS index_owner,NVL(c."
		       "index_name,'') AS index_name FROM all_constraints c LEFT JOIN all_cons_columns cc ON cc.owner=c.owner "
		       "AND cc.constraint_name=c.constraint_name AND cc.position IS NOT NULL LEFT JOIN all_constraints rc ON "
		       "rc.owner=c.r_owner AND rc."
		       "constraint_name=c.r_constraint_name LEFT JOIN all_cons_columns rcc ON rcc.owner=rc.owner AND rcc."
		       "constraint_name=rc.constraint_name AND rcc.position=cc.position AND rcc.position IS NOT NULL WHERE "
		       "c.owner=" +
		       OracleMetadataLiteral(owner) +
		       " AND c.constraint_type IN ('P','U','R','C') ORDER BY "
		       "c.table_name,c.constraint_name,NVL(cc.position,0),cc.column_name";
	}
	if (category == "indexes") {
		auto expression_join = string();
		auto expression = string("''");
		auto index_type_condition = string(" AND i.index_type NOT LIKE 'FUNCTION-BASED%'");
		if (capabilities.index_expressions) {
			expression_join =
			    " LEFT JOIN all_ind_expressions ie ON ie.index_owner=i.owner AND ie.index_name=i.index_name AND "
			    "ie.table_owner=i.table_owner AND ie.table_name=i.table_name AND ie.column_position=ic.column_position";
			expression = "NVL(ie.column_expression,'')";
			index_type_condition.clear();
		}
		return "SELECT i.table_owner AS owner_name,i.table_name AS table_name,i.owner||':'||i.index_name AS "
		       "index_id,i.index_name AS index_name,i.index_type AS index_type,i.uniqueness AS uniqueness,i.status AS "
		       "index_status,i.visibility AS visibility,i.partitioned AS partitioned,i.temporary AS temporary,i."
		       "generated AS generated_name,NVL(i.funcidx_status,'') AS function_status,TO_CHAR(ic.column_position) AS "
		       "column_position,ic.column_name AS column_name,ic.descend AS descend," +
		       expression +
		       " AS column_expression FROM all_indexes i JOIN all_ind_columns ic ON ic.index_owner=i.owner AND ic."
		       "index_name=i.index_name AND ic.table_owner=i.table_owner AND ic.table_name=i.table_name" +
		       expression_join + " WHERE i.table_owner=" + OracleMetadataLiteral(owner) + " AND i.index_type<>'LOB'" +
		       index_type_condition + " ORDER BY i.table_name,i.index_name,ic.column_position";
	}
	throw InvalidInputException("Unsupported oracle_relational_metadata category \"%s\"", category);
}

static unique_ptr<FunctionData> ODBCRelationalMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("Parameters to oracle_relational_metadata cannot be NULL");
		}
	}
	auto db_name = input.inputs[0].GetValue<string>();
	auto category = input.inputs[1].GetValue<string>();
	auto owner = input.inputs[2].GetValue<string>();
	StringUtil::Trim(owner);
	if (owner.empty()) {
		throw BinderException("Oracle owner passed to oracle_relational_metadata cannot be empty");
	}
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in oracle_relational_metadata",
		                      db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "oracle") {
		throw BinderException("Attached database \"%s\" does not refer to an Oracle database", db_name);
	}
	auto admission_lock = OracleRelationalMetadataLock(db_name);
	unique_lock<std::timed_mutex> relational_metadata_guard(*admission_lock, std::defer_lock);
	if (!relational_metadata_guard.try_lock_for(std::chrono::seconds(10))) {
		throw IOException("Oracle relational metadata admission timed out for catalog \"%s\"", db_name);
	}
	auto &connection = ODBCTransaction::Get(context, catalog).GetConnection();
	string sql;
	if (category == "capabilities") {
		sql = OracleCapabilitySQL();
	} else {
		auto capabilities = ProbeOracleRelationalCapabilities(connection);
		sql = OracleRelationalMetadataSQL(category, owner, capabilities);
	}
	auto result = connection.QueryWithTimeout(sql, ODBCResultStreaming::FORCE_MATERIALIZATION,
	                                          ORACLE_RELATIONAL_QUERY_TIMEOUT_SECONDS);
	for (auto &field : result->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.duckdb_type);
	}
	if (return_types.empty()) {
		throw InvalidInputException("oracle_relational_metadata returned no columns for category \"%s\"", category);
	}
	return make_uniq<ODBCQueryBindData>(catalog, std::move(result), std::move(sql));
}

ODBCRelationalMetadataFunction::ODBCRelationalMetadataFunction()
    : TableFunction("oracle_relational_metadata", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ODBCScan, ODBCRelationalMetadataBind, ODBCQueryInitGlobalState, ODBCInitLocalState) {
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
}

} // namespace duckdb
