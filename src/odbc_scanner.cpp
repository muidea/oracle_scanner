#include "duckdb.hpp"
#include "odbc_scanner.hpp"
#include "odbc_utils.hpp"
#include "odbc_filter_pushdown.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "storage/odbc_connection_pool.hpp"
#include "storage/odbc_table_set.hpp"
#include "odbc_filter_pushdown.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "oracle_metadata_diagnostics.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include <chrono>
#include <cstdio>
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
	auto dst = FlatVector::GetDataMutable<timestamp_tz_t>(output_vec);
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
                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
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
				col_name = col.GetName().GetIdentifierName();
			}
			select += ODBCUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += ODBCUtils::WriteIdentifier(bind_data.table.schema.name.GetIdentifierName());
	select += ".";
	select += ODBCUtils::WriteIdentifier(bind_data.table.name.GetIdentifierName());
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
	result["Table"] = bind_data.table.name.GetIdentifierName();
	return result;
}

static void ODBCScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                              const BoundTableFunction &function) {
	throw NotImplementedException("ODBCScanSerialize");
}

static unique_ptr<FunctionData> ODBCScanDeserialize(Deserializer &deserializer, BoundTableFunction &function) {
	throw NotImplementedException("ODBCScanDeserialize");
}

static optional_ptr<TableCatalogEntry> ODBCGetTableEntry(optional_ptr<const FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ODBCBindData>();
	return &bind_data.table;
}

static BindInfo ODBCGetBindInfo(TableFunctionGetBindInfoInput &input) {
	return BindInfo(ScanType::EXTERNAL);
}

ODBCScanFunction::ODBCScanFunction()
    : TableFunction("oracle_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan,
                    ODBCBind, ODBCInitGlobalState, ODBCInitLocalState) {
	to_string = ODBCScanToString;
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
	get_bind_info = ODBCGetBindInfo;
	get_table_entry = ODBCGetTableEntry;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// ODBC Query
//===--------------------------------------------------------------------===//
// oracle_query is used by the federated metadata scanner for direct catalog
// reads.  It force-materializes the ODBC result during bind, so an Oracle
// owner whose catalog query cannot finish must be bounded at the ODBC layer;
// otherwise DuckDB's native read gate remains occupied until the driver
// returns.  A timeout is an explicit metadata error, never a schema fallback.
static constexpr uint32_t ORACLE_QUERY_TIMEOUT_SECONDS = 60;

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
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to oracle_query cannot be NULL");
	}

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
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

	unique_ptr<ODBCResult> result;
	if (params.empty()) {
		result = transaction.GetConnection().QueryWithTimeout(sql, ODBCResultStreaming::FORCE_MATERIALIZATION,
		                                                      ORACLE_QUERY_TIMEOUT_SECONDS);
	} else {
		// Keep parameterized oracle_query calls on the existing prepared-statement
		// path. Metadata queries are literal-only, while callers using params must
		// retain the ABI's parameter binding semantics.
		result = transaction.GetConnection().Query(sql, std::move(params), ODBCResultStreaming::FORCE_MATERIALIZATION);
	}
	for (auto &field : result->Fields()) {
		names.emplace_back(field.name);
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
	GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) { options.Add("params", LogicalType::ANY); });
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

static OracleRelationalCapabilities ProbeOracleRelationalCapabilities(ODBCConnection &connection,
                                                                      ODBCQueryMetrics *metrics = nullptr) {
	auto result = connection.QueryWithTimeout(OracleCapabilitySQL(), ODBCResultStreaming::FORCE_MATERIALIZATION,
	                                          ORACLE_RELATIONAL_QUERY_TIMEOUT_SECONDS, metrics);
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
                                          const OracleRelationalCapabilities &capabilities,
                                          const vector<string> &tables) {
	string table_scope;
	for (const auto &table : tables) {
		if (!table_scope.empty())
			table_scope += ",";
		table_scope += OracleMetadataLiteral(table);
	}
	if (!table_scope.empty())
		table_scope = " AND c.table_name IN (" + table_scope + ")";
	if (category == "columnExpressions" || category == "checkExpressions") {
		// DBMS_XMLGEN safely converts legacy LONG catalog values into XML CLOB.
		// Values are quoted twice: once for the fixed source SELECT, once for
		// GETXMLTYPE's argument. No client SQL is accepted by this contract.
		const bool columns = category == "columnExpressions";
		const string item = columns ? "column_name" : "constraint_name";
		const string expression = columns ? "data_default" : "search_condition";
		const string catalog = columns ? "all_tab_cols" : "all_constraints";
		const string filter = columns ? " AND virtual_column='YES' AND hidden_column='NO'" : " AND constraint_type='C'";
		const string query = "SELECT c.owner,c.table_name,c." + item + ",c." + expression + " FROM " + catalog +
		                     " c WHERE c.owner=" + OracleMetadataLiteral(owner) + table_scope + filter;
		return "SELECT x.owner_name,x.table_name,x.item_name,x.expression FROM XMLTABLE('/ROWSET/ROW' "
		       "PASSING DBMS_XMLGEN.GETXMLTYPE(" +
		       OracleMetadataLiteral(query) +
		       ") COLUMNS owner_name VARCHAR2(128) PATH 'OWNER',table_name VARCHAR2(128) PATH 'TABLE_NAME',"
		       "item_name VARCHAR2(128) PATH '" +
		       StringUtil::Upper(item) + "',expression CLOB PATH '" + StringUtil::Upper(expression) +
		       "') x ORDER BY x.table_name,x.item_name";
	}

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
		       "column_id) AS column_id,c.nullable AS is_nullable,CASE WHEN c.default_length>0 THEN '1' ELSE '0' END "
		       "AS default_definition," +
		       identity_generation + " AS identity_generation," + virtual_column +
		       " AS virtual_column,c.data_type AS data_type FROM " + column_catalog + " c" + identity_join +
		       " WHERE c.owner=" + OracleMetadataLiteral(owner) + table_scope + visible_condition +
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
		       " AS check_definition,c.generated AS generated_name,NVL((SELECT CASE WHEN COUNT(*)=1 THEN MIN(bi.owner) "
		       "END "
		       "FROM all_indexes bi WHERE bi.table_owner=c.owner AND bi.table_name=c.table_name "
		       "AND bi.index_name=c.index_name AND (c.index_owner IS NULL OR bi.owner=c.index_owner)), '') AS "
		       "index_owner,NVL(c."
		       "index_name,'') AS index_name FROM all_constraints c LEFT JOIN all_cons_columns cc ON cc.owner=c.owner "
		       "AND cc.constraint_name=c.constraint_name AND cc.table_name=c.table_name AND cc.position IS NOT NULL "
		       "LEFT JOIN all_constraints rc ON "
		       "rc.owner=c.r_owner AND rc."
		       "constraint_name=c.r_constraint_name LEFT JOIN all_cons_columns rcc ON rcc.owner=rc.owner AND rcc."
		       "constraint_name=rc.constraint_name AND rcc.table_name=rc.table_name AND rcc.position=cc.position AND "
		       "rcc.position IS NOT NULL WHERE "
		       "c.owner=" +
		       OracleMetadataLiteral(owner) + table_scope +
		       " AND c.constraint_type IN ('P','U','R','C') ORDER BY "
		       "c.table_name,c.constraint_name,NVL(cc.position,0),cc.column_name";
	}
	if (category == "indexPartitionStates") {
		auto partition_count = [](const string &extra) {
			return "TO_CHAR((SELECT COUNT(*) FROM all_ind_partitions p WHERE p.index_owner=i.owner AND "
			       "p.index_name=i.index_name" +
			       extra + "))";
		};
		auto subpartition_count = [](const string &extra) {
			return "TO_CHAR((SELECT COUNT(*) FROM all_ind_subpartitions p WHERE p.index_owner=i.owner AND "
			       "p.index_name=i.index_name" +
			       extra + "))";
		};
		return "SELECT i.table_owner AS owner_name,i.table_name AS table_name,i.owner||':'||i.index_name AS index_id,"
		       "NVL(TO_CHAR(pi.partition_count),'') AS expected_partitions,NVL(pi.subpartitioning_type,'') AS "
		       "subpartitioning_type," +
		       partition_count("") + " AS actual_partitions," +
		       partition_count(" AND p.composite='NO' AND p.status IN ('UNUSABLE','DISABLED')") +
		       " AS unusable_partitions," +
		       partition_count(" AND p.composite='NO' AND (p.status IS NULL OR p.status NOT IN "
		                       "('USABLE','VALID','UNUSABLE','DISABLED'))") +
		       " AS unknown_partitions," + partition_count(" AND p.composite='YES'") +
		       " AS composite_partitions,"
		       "TO_CHAR((SELECT NVL(SUM(p.subpartition_count),0) FROM all_ind_partitions p WHERE p.index_owner=i.owner "
		       "AND p.index_name=i.index_name AND p.composite='YES')) AS expected_subpartitions," +
		       subpartition_count("") + " AS actual_subpartitions," +
		       subpartition_count(" AND p.status IN ('UNUSABLE','DISABLED')") + " AS unusable_subpartitions," +
		       subpartition_count(
		           " AND (p.status IS NULL OR p.status NOT IN ('USABLE','VALID','UNUSABLE','DISABLED'))") +
		       " AS unknown_subpartitions," +
		       partition_count(" AND (p.composite IS NULL OR p.composite NOT IN ('YES','NO'))") +
		       " AS unknown_composites," +
		       partition_count(" AND p.composite='YES' AND (p.subpartition_count IS NULL OR p.subpartition_count<=0)") +
		       " AS empty_composites"
		       " FROM all_indexes i LEFT JOIN all_part_indexes pi ON pi.owner=i.owner AND pi.index_name=i.index_name"
		       " WHERE i.table_owner=" +
		       OracleMetadataLiteral(owner) + StringUtil::Replace(table_scope, "c.table_name", "i.table_name") +
		       " AND i.partitioned='YES' AND i.index_type<>'LOB' ORDER BY i.table_name,i.index_name";
	}
	if (category == "indexes") {
		auto expression_join = string();
		auto expression = string("'0'");
		auto index_type_condition = string(" AND i.index_type NOT LIKE 'FUNCTION-BASED%'");
		if (capabilities.index_expressions) {
			expression_join =
			    " LEFT JOIN all_ind_expressions ie ON ie.index_owner=i.owner AND ie.index_name=i.index_name AND "
			    "ie.table_owner=i.table_owner AND ie.table_name=i.table_name AND ie.column_position=ic.column_position";
			expression = "CASE WHEN ie.column_position IS NULL THEN '0' ELSE '1' END";
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
		       expression_join + " WHERE i.table_owner=" + OracleMetadataLiteral(owner) +
		       StringUtil::Replace(table_scope, "c.table_name", "i.table_name") + " AND i.index_type<>'LOB'" +
		       index_type_condition + " ORDER BY i.table_name,i.index_name,ic.column_position";
	}
	throw InvalidInputException("Unsupported oracle_relational_metadata category \"%s\"", category);
}

// Raw LONG is fetched only for rows whose fixed descriptor proves it exists.
// Do not use XML conversion: XML normalizes CRLF and changes expression text.
static string OracleSparseMetadataSQL(const string &category, const string &owner, const vector<string> &tables,
                                      bool virtual_columns) {
	string scope;
	for (const auto &table : tables) {
		if (!scope.empty())
			scope += ",";
		scope += OracleMetadataLiteral(table);
	}
	const auto filter = scope.empty() ? string() : " AND c.table_name IN (" + scope + ")";
	if (category == "columnProperties") {
		return "SELECT c.owner,c.table_name,c.column_name,c.data_default FROM " +
		       string(virtual_columns ? "all_tab_cols" : "all_tab_columns") +
		       " c WHERE c.owner=" + OracleMetadataLiteral(owner) + filter + " AND c.default_length>0" +
		       (virtual_columns ? " AND c.hidden_column='NO'" : "");
	}
	return "SELECT "
	       "c.table_owner,c.table_name,c.index_owner||':'||c.index_name,TO_CHAR(c.column_position),c.column_expression "
	       "FROM all_ind_expressions c JOIN all_indexes i ON i.owner=c.index_owner AND i.index_name=c.index_name "
	       "AND i.table_owner=c.table_owner AND i.table_name=c.table_name WHERE c.table_owner=" +
	       OracleMetadataLiteral(owner) + filter + " AND i.index_type<>'LOB'";
}

// Per-call measurements distinguish source SQL from DuckDB function calls.
// Materialization includes field decoding/fetch, not Go result consumption.

static unique_ptr<FunctionData> ODBCRelationalMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<Identifier> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("Parameters to oracle_relational_metadata cannot be NULL");
		}
	}
	auto db_name = input.inputs[0].GetValue<string>();
	auto category = input.inputs[1].GetValue<string>();
	auto owner = input.inputs[2].GetValue<string>();
	if ((category == "columnExpressions" || category == "checkExpressions") && input.inputs.size() != 4)
		throw BinderException("Oracle expression metadata requires scoped table list");
	vector<string> tables;
	if (input.inputs.size() == 4) {
		for (const auto &value : ListValue::GetChildren(input.inputs[3])) {
			if (value.IsNull())
				throw BinderException("Oracle table scope cannot contain NULL");
			auto table = value.GetValue<string>();
			if (table.empty() || table.size() > 128 || table.find('\0') != string::npos)
				throw BinderException("Oracle table scope contains an invalid name");
			tables.push_back(table);
		}
		if (tables.empty() || tables.size() > 128)
			throw BinderException("Oracle table scope must contain 1 to 128 names");
	}
	StringUtil::Trim(owner);
	if (owner.empty()) {
		throw BinderException("Oracle owner passed to oracle_relational_metadata cannot be empty");
	}
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
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
	OracleMetadataPhases phases {db_name, owner, category, tables.size()};
	string sql;
	bool virtual_columns = false;
	if (category == "capabilities") {
		sql = OracleCapabilitySQL();
		phases.probe_calls++;
	} else {
		phases.probe_calls++;
		auto capabilities = ProbeOracleRelationalCapabilities(connection, &phases.probe);
		virtual_columns = capabilities.virtual_columns;
		sql = OracleRelationalMetadataSQL(category, owner, capabilities, tables);
	}
	if (category != "capabilities")
		phases.category_calls++;
	phases.SetSQL(sql);
	auto result = connection.QueryWithTimeout(sql, ODBCResultStreaming::FORCE_MATERIALIZATION,
	                                          ORACLE_RELATIONAL_QUERY_TIMEOUT_SECONDS,
	                                          category == "capabilities" ? &phases.probe : &phases.query);
	if (category == "columnProperties" || category == "indexes") {
		const idx_t column = category == "columnProperties" ? 5 : 15;
		const vector<idx_t> keys =
		    category == "columnProperties" ? vector<idx_t> {0, 1, 2} : vector<idx_t> {0, 1, 2, 12};
		unique_ptr<ODBCResult> sparse;
		if (result->HasSparseMetadataText(column)) {
			const auto sparse_sql = OracleSparseMetadataSQL(category, owner, tables, virtual_columns);
			const auto before_rows = phases.query.rows;
			const auto before_fetch = phases.query.fetch_us;
			phases.category_calls++;
			sparse = connection.QueryWithTimeout(sparse_sql, ODBCResultStreaming::FORCE_MATERIALIZATION,
			                                     ORACLE_RELATIONAL_QUERY_TIMEOUT_SECONDS, &phases.query);
			phases.sparse_rows = phases.query.rows - before_rows;
			phases.sparse_fetch_us = phases.query.fetch_us - before_fetch;
		}
		result->MergeSparseMetadataText(column, keys, sparse.get());
	}
	for (auto &field : result->Fields()) {
		names.emplace_back(field.name);
		return_types.push_back(field.duckdb_type);
	}
	if (return_types.empty()) {
		throw InvalidInputException("oracle_relational_metadata returned no columns for category \"%s\"", category);
	}
	unique_ptr<FunctionData> binding = make_uniq<ODBCQueryBindData>(catalog, std::move(result), std::move(sql));
	phases.success = true;
	return binding;
}

ODBCRelationalMetadataFunction::ODBCRelationalMetadataFunction(bool scoped, bool expressions)
    : TableFunction("oracle_relational_metadata",
                    scoped ? vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                                  LogicalType::LIST(LogicalType::VARCHAR)}
                           : vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ODBCScan, ODBCRelationalMetadataBind, ODBCQueryInitGlobalState, ODBCInitLocalState) {
	serialize = ODBCScanSerialize;
	deserialize = ODBCScanDeserialize;
}

} // namespace duckdb
