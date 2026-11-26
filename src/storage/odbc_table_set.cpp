#include "storage/odbc_table_set.hpp"

#include "duckdb/common/string_util.hpp"
#include <algorithm>

#include "storage/odbc_transaction.hpp"
#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_catalog.hpp"

#include "odbc_types.hpp"

namespace duckdb {

ODBCTableSet::ODBCTableSet(ODBCSchemaEntry &schema) : ODBCInSchemaSet(schema) {
}

static string NormalizeObjectName(string name) {
	name.erase(std::remove(name.begin(), name.end(), '\0'), name.end());
	auto is_ascii_space = [](unsigned char ch) {
		return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
	};
	while (!name.empty() && is_ascii_space(static_cast<unsigned char>(name.front()))) {
		name.erase(name.begin());
	}
	while (!name.empty() && is_ascii_space(static_cast<unsigned char>(name.back()))) {
		name.pop_back();
	}
	return name;
}

static bool ODBCSchemaIsInternal(const string &name, bool was_quoted) {
	if (was_quoted) {
		return false;
	}
	auto lower = StringUtil::Lower(name);
	return lower == "sys" || lower == "system" || lower == "xdb" || lower == "sysaux";
}

void ODBCTableSet::LoadEntries(ClientContext &context) {
	auto &odbc_catalog = schema.ParentCatalog().Cast<ODBCCatalog>();
	if (ODBCSchemaIsInternal(schema.name, odbc_catalog.DefaultSchemaWasQuoted())) {
		return;
	}

	// Oracle exposes object and column metadata in bulk. Building the catalog
	// from these views avoids issuing one describe query per remote table, which
	// is prohibitively expensive for owners with thousands of objects.
	auto objects_query = StringUtil::Replace(R"(
	SELECT
	    OBJECT_NAME,
	    OBJECT_TYPE
	FROM ALL_OBJECTS
	WHERE OWNER = ${SCHEMA_NAME}
	  AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
	  AND OBJECT_NAME NOT LIKE 'BIN$%'
	ORDER BY OBJECT_NAME
	)",
	                                         "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema.name));
	auto columns_query = StringUtil::Replace(R"(
	SELECT
	    TABLE_NAME,
	    COLUMN_NAME,
	    DATA_TYPE,
	    TO_CHAR(DATA_PRECISION),
	    TO_CHAR(DATA_SCALE)
	FROM ALL_TAB_COLUMNS
	WHERE OWNER = ${SCHEMA_NAME}
	  AND TABLE_NAME NOT LIKE 'BIN$%'
	ORDER BY TABLE_NAME, COLUMN_ID
	)",
	                                         "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema.name));

	auto load_table_entries_from_oracle_catalog = [&]() -> idx_t {
		auto &transaction = ODBCTransaction::Get(context, catalog);
		try {
			unordered_map<string, string> object_types;
			auto objects = transaction.Query(objects_query);
			while (objects->Next()) {
				auto object_name = NormalizeObjectName(objects->GetString(0));
				auto object_type = StringUtil::Upper(objects->GetString(1));
				if (object_name.empty() || object_name.find('?') != string::npos) {
					continue;
				}
				auto existing = object_types.find(object_name);
				if (existing == object_types.end() || (existing->second == "TABLE" && object_type != "TABLE")) {
					object_types[object_name] = std::move(object_type);
				}
			}
			if (object_types.empty()) {
				return 0;
			}

			unordered_map<string, unique_ptr<ODBCTableInfo>> table_infos;
			ODBCTypeConfig type_config(context);
			auto columns = transaction.GetConnection().Query(columns_query, ODBCResultStreaming::ALLOW_STREAMING);
			while (columns->Next()) {
				auto table_name = NormalizeObjectName(columns->GetString(0));
				auto object = object_types.find(table_name);
				if (object == object_types.end()) {
					continue;
				}

				auto remote_column_name = columns->GetString(1);
				auto column_name = NormalizeObjectName(remote_column_name);
				if (column_name.empty()) {
					continue;
				}

				ODBCTypeData type_info;
				type_info.type_name = columns->GetString(2);
				type_info.column_type = type_info.type_name;
				type_info.precision = columns->IsNull(3) ? -1 : std::stoll(columns->GetString(3));
				type_info.scale = columns->IsNull(4) ? -1 : std::stoll(columns->GetString(4));

				auto &table_info = table_infos[table_name];
				if (!table_info) {
					table_info = make_uniq<ODBCTableInfo>(schema, table_name, object->second != "TABLE");
				}
				auto column_type = ODBCTypes::TypeToLogicalType(type_config, type_info);
				ColumnDefinition column(std::move(column_name), std::move(column_type));
				table_info->create_info->columns.AddColumn(std::move(column));
				table_info->remote_column_names.push_back(std::move(remote_column_name));
			}

			idx_t loaded_count = 0;
			for (auto &entry : table_infos) {
				if (entry.second->remote_column_names.empty() || GetEntryIfLoaded(entry.first)) {
					continue;
				}
				auto table_entry = make_uniq<ODBCTableEntry>(catalog, schema, *entry.second);
				CreateEntry(std::move(table_entry));
				loaded_count++;
			}
			return loaded_count;
		} catch (...) {
			return 0;
		}
	};

	if (load_table_entries_from_oracle_catalog() > 0) {
		return;
	}

	auto load_table_entry = [&](const string &raw_table_name, bool is_view) -> bool {
		auto table_name = NormalizeObjectName(raw_table_name);
		if (table_name.empty()) {
			return false;
		}
		// Skip obviously corrupted table names to avoid failing the whole schema
		// load because of encoding issues on a single table.
		if (table_name.find('?') != string::npos) {
			return false;
		}
		try {
			auto table_info = GetTableInfo(context, schema, table_name, is_view);
			if (!table_info) {
				return false;
			}
			auto table_entry = make_uniq<ODBCTableEntry>(catalog, schema, *table_info);
			CreateEntry(std::move(table_entry));
			return true;
		} catch (...) {
			// Ignore per-table probe errors (e.g., 42S02 for corrupted names)
			// and continue loading remaining tables.
			return false;
		}
	};

	auto load_table_entries_from_query = [&](const string &query) -> idx_t {
		auto &transaction = ODBCTransaction::Get(context, catalog);
		idx_t loaded_count = 0;
		try {
			auto result = transaction.Query(query);
			while (result->Next()) {
				auto object_type = StringUtil::Upper(result->GetString(1));
				if (load_table_entry(result->GetString(0), object_type != "TABLE")) {
					loaded_count++;
				}
			}
		} catch (...) {
			return 0;
		}
		return loaded_count;
	};

	auto load_table_entries_from_odbc_catalog = [&]() -> idx_t {
		auto &transaction = ODBCTransaction::Get(context, catalog);
		SQLHSTMT stmt = SQL_NULL_HSTMT;
		auto retcode = SQLAllocHandle(SQL_HANDLE_STMT, transaction.GetConnection().GetConn(), &stmt);
		if (!SQL_SUCCEEDED(retcode)) {
			return 0;
		}

		idx_t loaded_count = 0;
		auto cleanup = [&]() {
			if (stmt != SQL_NULL_HSTMT) {
				SQLFreeStmt(stmt, SQL_CLOSE);
				SQLFreeHandle(SQL_HANDLE_STMT, stmt);
				stmt = SQL_NULL_HSTMT;
			}
		};

		retcode = SQLTables(stmt, nullptr, 0, (SQLCHAR *)schema.name.c_str(), SQL_NTS, nullptr, 0,
		                    (SQLCHAR *)"TABLE,VIEW", SQL_NTS);
		if (!SQL_SUCCEEDED(retcode)) {
			cleanup();
			return 0;
		}

		SQLCHAR table_name_buffer[1024] = {};
		SQLCHAR table_type_buffer[128] = {};
		SQLLEN table_name_indicator = 0;
		SQLLEN table_type_indicator = 0;
		SQLBindCol(stmt, 3, SQL_C_CHAR, table_name_buffer, sizeof(table_name_buffer), &table_name_indicator);
		SQLBindCol(stmt, 4, SQL_C_CHAR, table_type_buffer, sizeof(table_type_buffer), &table_type_indicator);

		while (SQL_SUCCEEDED(retcode = SQLFetch(stmt))) {
			if (table_name_indicator == SQL_NULL_DATA) {
				continue;
			}
			string table_type;
			if (table_type_indicator != SQL_NULL_DATA) {
				table_type = StringUtil::Upper(string((char *)table_type_buffer));
			}
			if (!table_type.empty() && table_type != "TABLE" && table_type != "BASE TABLE" && table_type != "VIEW") {
				continue;
			}
			if (load_table_entry(string((char *)table_name_buffer), table_type == "VIEW")) {
				loaded_count++;
			}
		}
		cleanup();
		return loaded_count;
	};

	auto loaded_count = load_table_entries_from_query(objects_query);
	if (loaded_count > 0) {
		return;
	}

	(void)load_table_entries_from_odbc_catalog();
}

string GetTableInfoQuery(const string &schema_name, const string &table_name) {
	// Use a lightweight probe query to let the ODBC driver describe the real columns.
	// This avoids depending on catalog column views that can omit driver-specific details.
	string sql;
	sql += "SELECT * FROM ";
	sql += ODBCUtils::WriteIdentifier(schema_name);
	sql += ".";
	sql += ODBCUtils::WriteIdentifier(table_name);
	sql += " WHERE 1=0";
	return sql;
}

unique_ptr<ODBCTableInfo> ODBCTableSet::GetTableInfo(ClientContext &context, ODBCSchemaEntry &schema,
                                                     const string &table_name, bool is_view) {
	auto &transaction = ODBCTransaction::Get(context, schema.ParentCatalog());
	auto columns_query = StringUtil::Replace(
	    StringUtil::Replace(R"(
	SELECT
	    COLUMN_NAME,
	    DATA_TYPE,
	    TO_CHAR(DATA_PRECISION),
	    TO_CHAR(DATA_SCALE)
	FROM ALL_TAB_COLUMNS
	WHERE OWNER = ${SCHEMA_NAME}
	  AND TABLE_NAME = ${TABLE_NAME}
	ORDER BY COLUMN_ID
	)",
	                        "${SCHEMA_NAME}", ODBCUtils::WriteLiteral(schema.name)),
	    "${TABLE_NAME}", ODBCUtils::WriteLiteral(table_name));
	try {
		auto columns = transaction.Query(columns_query);
		auto table_info = make_uniq<ODBCTableInfo>(schema, table_name, is_view);
		ODBCTypeConfig type_config(context);
		while (columns->Next()) {
			auto remote_column_name = columns->GetString(0);
			auto column_name = NormalizeObjectName(remote_column_name);
			if (column_name.empty()) {
				continue;
			}
			ODBCTypeData type_info;
			type_info.type_name = columns->GetString(1);
			type_info.column_type = type_info.type_name;
			type_info.precision = columns->IsNull(2) ? -1 : std::stoll(columns->GetString(2));
			type_info.scale = columns->IsNull(3) ? -1 : std::stoll(columns->GetString(3));
			ColumnDefinition column(std::move(column_name), ODBCTypes::TypeToLogicalType(type_config, type_info));
			table_info->create_info->columns.AddColumn(std::move(column));
			table_info->remote_column_names.push_back(std::move(remote_column_name));
		}
		if (!table_info->remote_column_names.empty()) {
			return table_info;
		}
	} catch (...) {
		// Fall back to probe query when ALL_TAB_COLUMNS is unavailable or empty (e.g. synonyms).
	}

	auto query = GetTableInfoQuery(schema.name, table_name);
	auto result = transaction.Query(query);
	auto table_info = make_uniq<ODBCTableInfo>(schema, table_name, is_view);
	// The probe query returns no rows; we only need the field metadata.
	for (auto &field : result->Fields()) {
		auto remote_column_name = field.name;
		auto column_name = NormalizeObjectName(field.name);
		if (column_name.empty()) {
			continue;
		}
		ColumnDefinition column(std::move(column_name), field.duckdb_type);
		table_info->create_info->columns.AddColumn(std::move(column));
		table_info->remote_column_names.push_back(std::move(remote_column_name));
	}
	return table_info;
}

optional_ptr<CatalogEntry> ODBCTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	auto table_entry = make_uniq<ODBCTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

} // namespace duckdb
