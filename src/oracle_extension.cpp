#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "odbc_scanner.hpp"
#include "odbc_storage.hpp"
#include "oracle_scanner_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_optimizer.hpp"

using namespace duckdb;

static void SetODBCDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	ODBCConnection::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

template <typename Fn>
static void ForEachODBCCatalog(ClientContext &context, Fn &&fn) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "oracle") {
			continue;
		}
		fn(catalog.Cast<ODBCCatalog>());
	}
}

static void SetODBCConnectionLimit(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("oracle_connection_limit can only be set globally");
	}
	auto new_max = parameter.GetValue<uint64_t>();
	ForEachODBCCatalog(context,
	                   [&](ODBCCatalog &catalog) { catalog.GetConnectionPool().SetMaximumConnections(new_max); });
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("oracle_connection_limit", parameter);
}

static void SetODBCConnectionCache(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("oracle_connection_cache can only be set globally");
	}
	ODBCConnectionPool::ODBCSetConnectionCache(context, scope, parameter);
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("oracle_connection_cache", parameter);
}

unique_ptr<BaseSecret> CreateOracleSecretFunction(ClientContext &, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "oracle", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "driver") {
			result->secret_map["driver"] = named_param.second.ToString();
		} else if (lower_name == "dbq") {
			result->secret_map["dbq"] = named_param.second.ToString();
		} else if (lower_name == "uid") {
			result->secret_map["uid"] = named_param.second.ToString();
		} else if (lower_name == "pwd") {
			result->secret_map["pwd"] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown named parameter passed to CreateOracleSecretFunction: " + lower_name);
		}
	}

	//! Set redact keys
	result->redact_keys = {"pwd", "password"};
	return std::move(result);
}

void SetOracleSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["driver"] = LogicalType::VARCHAR;
	function.named_parameters["dbq"] = LogicalType::VARCHAR;
	function.named_parameters["uid"] = LogicalType::VARCHAR;
	function.named_parameters["pwd"] = LogicalType::VARCHAR;
}

static void LoadInternal(ExtensionLoader &loader) {
	// The ODBC driver manager handles Oracle driver initialization.
	ODBCClearCacheFunction clear_cache_func;
	loader.RegisterFunction(clear_cache_func);

	ODBCQueryFunction query_function;
	loader.RegisterFunction(query_function);

	ODBCMetadataFunction metadata_function;
	loader.RegisterFunction(metadata_function);

	ODBCRelationalMetadataFunction relational_metadata_function;
	loader.RegisterFunction(relational_metadata_function);

	SecretType secret_type;
	secret_type.name = "oracle";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction oracle_secret_function = {"oracle", "config", CreateOracleSecretFunction};
	SetOracleSecretParameters(oracle_secret_function);
	loader.RegisterFunction(oracle_secret_function);

	auto &db = loader.GetDatabaseInstance();

	auto &config = DBConfig::GetConfig(db);
	StorageExtension::Register(config, "oracle_scanner", make_shared_ptr<ODBCStorageExtension>());

	config.AddExtensionOption("oracle_experimental_filter_pushdown",
	                          "Whether or not to use filter pushdown (currently experimental)", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
	config.AddExtensionOption("oracle_debug_show_queries", "DEBUG SETTING: print all queries sent to Oracle to stdout",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), SetODBCDebugQueryPrint);
	config.AddExtensionOption("oracle_session_time_zone",
	                          "Value to use as a session time zone for newly opened"
	                          " connections to ODBC server",
	                          LogicalType::VARCHAR, Value(""), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("oracle_time_as_time", "Whether or not to convert ODBC TIME columns to DuckDB TIME",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), ODBCClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("oracle_connection_limit", "Maximum number of connections per Oracle catalog",
	                          LogicalType::UBIGINT, Value::UBIGINT(ODBCConnectionPool::DEFAULT_MAX_CONNECTIONS),
	                          SetODBCConnectionLimit);
	config.AddExtensionOption("oracle_connection_cache", "Whether or not to cache idle Oracle connections",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), SetODBCConnectionCache);

	OptimizerExtension odbc_optimizer;
	odbc_optimizer.optimize_function = ODBCOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(odbc_optimizer));
}

void OracleScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(oracle_scanner, loader) {
	LoadInternal(loader);
}
}
