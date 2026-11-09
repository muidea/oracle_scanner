#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "oracle_metadata_diagnostics.hpp"
#include <iostream>

using namespace duckdb;

// Exercise the production diagnostic object inside a real DuckDB binder.
// ODBC transport mocks cannot detect execution-only ClientContext access here.
static unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                     vector<string> &names) {
	OracleMetadataPhases phases {"catalog", "owner", input.inputs[0].GetValue<string>(), 1};
	phases.SetSQL("SELECT 'diagnostic\\\"\\n' FROM source_catalog");
	phases.query.connection_id = 17;
	phases.query.connection_query_sequence = 23;
	phases.success = true;
	types.push_back(LogicalType::INTEGER);
	names.push_back("value");
	return nullptr;
}

static void Scan(ClientContext &, TableFunctionInput &, DataChunk &) {
}

#ifdef ORACLE_DIAGNOSTICS_TEST_EXTENSION
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(oracle_diagnostics_test, loader) {
	loader.RegisterFunction(TableFunction("test_oracle_metadata_diagnostics", {LogicalType::VARCHAR}, Scan, Bind));
}
}
#else
int main() {
	DuckDB db(nullptr);
	Connection conn(db);
	ExtensionLoader loader(*db.instance, "diagnostics_test");
	loader.RegisterFunction(TableFunction("test_oracle_metadata_diagnostics", {LogicalType::VARCHAR}, Scan, Bind));
	for (const auto &category : {"capabilities", "columnProperties", "constraints", "indexes"}) {
		auto sql = string("/* fedquery_metadata query_id=1 job_id=2 generation=3 revision=4 */ SELECT * FROM ") +
		           "test_oracle_metadata_diagnostics('" + category + "')";
		auto direct = conn.Query(sql);
		if (direct->HasError()) {
			std::cerr << direct->GetError() << std::endl;
			return 1;
		}
		auto prepared = conn.Prepare(sql);
		if (prepared->HasError()) {
			std::cerr << prepared->GetError() << std::endl;
			return 2;
		}
		for (int attempt = 0; attempt < 2; attempt++) {
			auto result = prepared->Execute();
			if (result->HasError()) {
				std::cerr << result->GetError() << std::endl;
				return 3;
			}
		}
	}
	std::cout << "PASS: production Oracle diagnostics in direct, prepare/bind and repeated execution, all categories\n";
}
#endif
