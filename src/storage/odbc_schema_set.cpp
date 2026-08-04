#include "storage/odbc_schema_set.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>

namespace duckdb {

static string NormalizeSchemaName(string name) {
	name.erase(std::remove(name.begin(), name.end(), '\0'), name.end());
	StringUtil::Trim(name);
	return name;
}

ODBCSchemaSet::ODBCSchemaSet(Catalog &catalog) : ODBCCatalogSet(catalog) {
}

void ODBCSchemaSet::LoadEntries(ClientContext &context) {
	auto &transaction = ODBCTransaction::Get(context, catalog);
	auto schema_name = catalog.Cast<ODBCCatalog>().GetDefaultSchema();
	if (schema_name.empty()) {
		auto result = transaction.Query("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL");
		if (result->Next() && !result->IsNull(0)) {
			schema_name = result->GetString(0);
		}
	}
	schema_name = NormalizeSchemaName(std::move(schema_name));
	if (schema_name.empty()) {
		throw InvalidInputException("Oracle schema could not be determined; specify SCHEMA in ATTACH");
	}

	CreateSchemaInfo info;
	info.schema = std::move(schema_name);
	info.internal = false;
	CreateEntry(make_uniq<ODBCSchemaEntry>(catalog, info));
}

optional_ptr<CatalogEntry> ODBCSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	throw PermissionException("oracle_scanner catalogs are read-only");
}

} // namespace duckdb
