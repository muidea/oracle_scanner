#include "odbc_utils.hpp"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <tuple>

#ifndef _WIN32
#include <unistd.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include "duckdb/common/string_util.hpp"

#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_transaction.hpp"

namespace duckdb {

#ifndef _WIN32
static std::mutex odbc_ini_mutex;
#endif

static bool ParseValue(const string &dsn, idx_t &pos, string &result) {
	// skip leading spaces
	while (pos < dsn.size() && StringUtil::CharacterIsSpace(dsn[pos])) {
		pos++;
	}
	if (pos >= dsn.size()) {
		return false;
	}
	// check if we are parsing a quoted value or not
	if (dsn[pos] == '"') {
		pos++;
		// scan until we find another quote
		bool found_quote = false;
		for (; pos < dsn.size(); pos++) {
			if (dsn[pos] == '"') {
				found_quote = true;
				pos++;
				break;
			}
			if (dsn[pos] == '\\') {
				// backslash escapes the backslash or double-quote
				if (pos + 1 >= dsn.size()) {
					throw InvalidInputException("Invalid dsn \"%s\" - backslash at end of dsn", dsn);
				}
				if (dsn[pos + 1] != '\\' && dsn[pos + 1] != '"') {
					throw InvalidInputException("Invalid dsn \"%s\" - backslash can only escape \\ or \"", dsn);
				}
				result += dsn[pos + 1];
				pos++;
			} else {
				result += dsn[pos];
			}
		}
		if (!found_quote) {
			throw InvalidInputException("Invalid dsn \"%s\" - unterminated quote", dsn);
		}
	} else {
		// unquoted value, continue until space, equality sign or end of string
		for (; pos < dsn.size(); pos++) {
			if (dsn[pos] == '=') {
				break;
			}
			if (StringUtil::CharacterIsSpace(dsn[pos])) {
				break;
			}
			result += dsn[pos];
		}
	}
	return true;
}

static bool ParseBoolValue(const string &value) {
	return value != "0" && !StringUtil::CIEquals(value, "false");
}

bool ReadOptionFromEnv(const char *env, string &result) {
	auto res = std::getenv(env);
	if (!res) {
		return false;
	}
	result = res;
	return true;
}

uint32_t ParsePort(const string &value) {
	constexpr const static int PORT_MIN = 0;
	constexpr const static int PORT_MAX = 65353;
	int port_val = std::stoi(value);
	if (port_val < PORT_MIN || port_val > PORT_MAX) {
		throw InvalidInputException("Invalid port %d - port must be between %d and %d", port_val, PORT_MIN, PORT_MAX);
	}
	return uint32_t(port_val);
}

string EscapeODBCConnectionValue(const string &value) {
	if (value.empty()) {
		return value;
	}
	bool has_outer_space = StringUtil::CharacterIsSpace(value.front()) || StringUtil::CharacterIsSpace(value.back());
	if (!has_outer_space && value.find_first_of(";{}") == string::npos) {
		return value;
	}
	string result = "{";
	for (auto c : value) {
		if (c == '}') {
			result += "}}";
		} else {
			result += c;
		}
	}
	result += "}";
	return result;
}

#ifndef _WIN32
void WriteODBCIniValue(std::ofstream &file, const string &key, const string &value) {
	if (value.find_first_of("\r\n") != string::npos) {
		throw InvalidInputException("Invalid ODBC connection value for \"%s\" - newlines are not supported", key);
	}
	file << key << "=" << value << "\n";
}

string CreateTemporaryODBCIni(const ODBCConnectionParameters &config, string &dsn_name) {
	char path_template[] = "/tmp/duckdb_odbc_XXXXXX";
	auto fd = mkstemp(path_template);
	if (fd < 0) {
		throw IOException("Failed to create temporary ODBC ini file");
	}
	close(fd);

	string path = path_template;
	auto slash = path.find_last_of('/');
	dsn_name = slash == string::npos ? path : path.substr(slash + 1);

	std::ofstream file(path);
	if (!file.is_open()) {
		std::remove(path.c_str());
		throw IOException("Failed to open temporary ODBC ini file \"%s\"", path);
	}

	file << "[" << dsn_name << "]\n";
	WriteODBCIniValue(file, "Driver", config.driver);
	WriteODBCIniValue(file, "Server", config.server);
	if (config.port != 0) {
		WriteODBCIniValue(file, "Port", std::to_string(config.port));
	}
	if (!config.database.empty()) {
		WriteODBCIniValue(file, "Database", config.database);
	}
	if (!config.dbq.empty()) {
		WriteODBCIniValue(file, "DBQ", config.dbq);
	}
	for (auto &extra : config.extra_params) {
		auto separator = extra.find('=');
		if (separator == string::npos) {
			continue;
		}
		WriteODBCIniValue(file, extra.substr(0, separator), extra.substr(separator + 1));
	}
	file.close();
	if (!file.good()) {
		std::remove(path.c_str());
		throw IOException("Failed to write temporary ODBC ini file \"%s\"", path);
	}
	return path;
}

bool ShouldUseSQLConnectPasswordFallback(const ODBCConnectionParameters &config) {
	return !config.driver.empty() && !config.server.empty() && !config.uid.empty() && !config.pwd.empty() &&
	       config.pwd.find('}') != string::npos;
}

SQLRETURN SQLConnectWithTemporaryDSN(SQLHDBC dbc, const ODBCConnectionParameters &config) {
	string dsn_name;
	auto odbc_ini_path = CreateTemporaryODBCIni(config, dsn_name);

	lock_guard<std::mutex> lock(odbc_ini_mutex);
	auto old_odbc_ini = std::getenv("ODBCINI");
	string old_odbc_ini_value;
	if (old_odbc_ini) {
		old_odbc_ini_value = old_odbc_ini;
	}
	setenv("ODBCINI", odbc_ini_path.c_str(), 1);

	auto ret = SQLConnect(dbc, (SQLCHAR *)dsn_name.c_str(), SQL_NTS, (SQLCHAR *)config.uid.c_str(), SQL_NTS,
	                      (SQLCHAR *)config.pwd.c_str(), SQL_NTS);

	if (old_odbc_ini) {
		setenv("ODBCINI", old_odbc_ini_value.c_str(), 1);
	} else {
		unsetenv("ODBCINI");
	}
	std::remove(odbc_ini_path.c_str());
	return ret;
}
#endif

std::tuple<ODBCConnectionParameters, unordered_set<string>> ODBCUtils::ParseConnectionParameters(const string &dsn) {
	ODBCConnectionParameters result;

	unordered_set<string> set_options;
	// parse options from ODBC connection string format: "key1=value1;key2=value2;"
	idx_t pos = 0;
	while (pos < dsn.size()) {
		string key;
		string value;

		// Parse key
		while (pos < dsn.size() && dsn[pos] != '=' && dsn[pos] != ';') {
			key += dsn[pos];
			pos++;
		}

		if (pos >= dsn.size() || dsn[pos] != '=') {
			throw InvalidInputException(
			    "Invalid connection string \"%s\" - expected key=value pairs separated by semicolons", dsn);
		}
		pos++; // skip '='

		// Parse value
		if (pos < dsn.size() && dsn[pos] == '{') {
			pos++; // skip '{'
			bool found_closing_brace = false;
			while (pos < dsn.size()) {
				if (dsn[pos] == '}') {
					if (pos + 1 < dsn.size() && dsn[pos + 1] == '}') {
						value += '}';
						pos += 2;
						continue;
					}
					pos++; // skip closing '}'
					found_closing_brace = true;
					break;
				}
				value += dsn[pos];
				pos++;
			}
			if (!found_closing_brace) {
				throw InvalidInputException("Invalid connection string \"%s\" - unterminated braced value", dsn);
			}
			if (pos < dsn.size() && dsn[pos] != ';') {
				throw InvalidInputException("Invalid connection string \"%s\" - expected semicolon after braced value",
				                            dsn);
			}
		} else {
			while (pos < dsn.size() && dsn[pos] != ';') {
				value += dsn[pos];
				pos++;
			}
		}

		if (pos < dsn.size() && dsn[pos] == ';') {
			pos++; // skip ';'
		}

		auto original_key = key;
		key = StringUtil::Lower(key);

		if (set_options.find(key) != set_options.end()) {
			throw InvalidInputException("Duplicate '%s' parameter in connection string. Each parameter "
			                            "should only be specified once.",
			                            key);
		}

		// Handle standard ODBC connection parameters
		if (key == "driver") {
			set_options.insert("driver");
			result.driver = value;
		} else if (key == "server") {
			set_options.insert("server");
			result.server = value;
		} else if (key == "database") {
			set_options.insert("database");
			result.database = value;
		} else if (key == "dbq") {
			set_options.insert("dbq");
			result.dbq = value;
		} else if (key == "uid") {
			set_options.insert("uid");
			result.uid = value;
		} else if (key == "pwd") {
			set_options.insert("pwd");
			result.pwd = value;
		} else if (key == "port") {
			set_options.insert("port");
			result.port = ParsePort(value);
		} else {
			// Preserve driver-specific parameters verbatim.
			result.extra_params.push_back(original_key + "=" + value);
			continue;
		}
	}

	// For ODBC, connection parameters are specified in the connection string
	return std::make_tuple(result, set_options);
}

string ODBCUtils::BuildConnectionString(const string &dsn) {
	ODBCConnectionParameters config;
	unordered_set<string> unused_params;
	std::tie(config, unused_params) = ParseConnectionParameters(dsn);

	string connection_string;
	if (!config.driver.empty()) {
		connection_string += "DRIVER=" + EscapeODBCConnectionValue(config.driver) + ";";
	}
	if (!config.server.empty()) {
		connection_string += "SERVER=" + EscapeODBCConnectionValue(config.server) + ";";
	}
	if (!config.uid.empty()) {
		connection_string += "UID=" + EscapeODBCConnectionValue(config.uid) + ";";
	}
	if (!config.pwd.empty()) {
		connection_string += "PWD=" + EscapeODBCConnectionValue(config.pwd) + ";";
	}
	if (!config.database.empty()) {
		connection_string += "DATABASE=" + EscapeODBCConnectionValue(config.database) + ";";
	}
	if (!config.dbq.empty()) {
		connection_string += "DBQ=" + EscapeODBCConnectionValue(config.dbq) + ";";
	}
	if (config.port != 0) {
		connection_string += "PORT=" + std::to_string(config.port) + ";";
	}

	// Append driver-specific / unrecognized parameters verbatim
	for (auto &extra : config.extra_params) {
		auto separator = extra.find('=');
		if (separator == string::npos) {
			connection_string += extra + ";";
			continue;
		}
		connection_string +=
		    extra.substr(0, separator + 1) + EscapeODBCConnectionValue(extra.substr(separator + 1)) + ";";
	}

	return connection_string;
}

SQLHDBC ODBCUtils::Connect(const string &dsn, SQLHENV &out_env) {
	SQLHENV env = SQL_NULL_HENV;
	SQLHDBC dbc = SQL_NULL_HDBC;
	SQLRETURN ret;

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	if (!SQL_SUCCEEDED(ret)) {
		throw IOException("Failed to allocate ODBC environment handle");
	}

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
	if (!SQL_SUCCEEDED(ret)) {
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		throw IOException("Failed to set ODBC version");
	}

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	if (!SQL_SUCCEEDED(ret)) {
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		throw IOException("Failed to allocate ODBC connection handle");
	}

	ODBCConnectionParameters config;
	unordered_set<string> unused_params;
	std::tie(config, unused_params) = ParseConnectionParameters(dsn);

#ifndef _WIN32
	if (ShouldUseSQLConnectPasswordFallback(config)) {
		ret = SQLConnectWithTemporaryDSN(dbc, config);
	} else
#endif
	{
		string connection_string = BuildConnectionString(dsn);

		SQLCHAR outstr[1024];
		SQLSMALLINT outstrlen;
		ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *)connection_string.c_str(), SQL_NTS, outstr, sizeof(outstr),
		                       &outstrlen, SQL_DRIVER_NOPROMPT);
	}

	if (!SQL_SUCCEEDED(ret)) {
		SQLCHAR sqlstate[6] = {0};
		SQLINTEGER native_error = 0;
		SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH] = {0};
		SQLSMALLINT text_length = 0;

		SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlstate, &native_error, message_text, SQL_MAX_MESSAGE_LENGTH,
		              &text_length);

		SQLFreeHandle(SQL_HANDLE_DBC, dbc);
		SQLFreeHandle(SQL_HANDLE_ENV, env);

		throw IOException("Failed to connect to Oracle through ODBC: %s", (char *)message_text);
	}
	// This is a driver-level safeguard in addition to the read-only DuckDB catalog.
	SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);

	out_env = env;
	return dbc;
}

string ODBCUtils::EscapeQuotes(const string &text, char quote) {
	string result;
	for (auto c : text) {
		if (c == quote) {
			result += string(2, quote);
		} else {
			result += c;
		}
	}
	return result;
}

string ODBCUtils::WriteQuoted(const string &text, char quote) {
	return string(1, quote) + EscapeQuotes(text, quote) + string(1, quote);
}

string ODBCUtils::WriteIdentifier(const string &identifier) {
	return WriteQuoted(identifier, '"');
}

string ODBCUtils::WriteLiteral(const string &identifier) {
	return ODBCUtils::WriteQuoted(identifier, '\'');
}

OracleIdentifier ODBCUtils::ParseOracleIdentifier(const string &identifier) {
	auto value = identifier;
	StringUtil::Trim(value);
	if (value.empty()) {
		throw InvalidInputException("Oracle schema owner is empty");
	}

	auto starts_quoted = value.front() == '"';
	auto ends_quoted = value.back() == '"';
	if (starts_quoted || ends_quoted) {
		if (!starts_quoted || !ends_quoted || value.size() < 2) {
			throw InvalidInputException("Invalid quoted Oracle schema identifier \"%s\"", identifier);
		}
		string result;
		for (idx_t i = 1; i + 1 < value.size(); i++) {
			if (value[i] != '"') {
				result += value[i];
				continue;
			}
			if (i + 2 >= value.size() || value[i + 1] != '"') {
				throw InvalidInputException("Invalid quoted Oracle schema identifier \"%s\"", identifier);
			}
			result += '"';
			i++;
		}
		if (result.empty()) {
			throw InvalidInputException("Oracle schema owner is empty");
		}
		OracleIdentifier parsed;
		parsed.name = std::move(result);
		parsed.quoted = true;
		return parsed;
	}
	if (value.find('"') != string::npos) {
		throw InvalidInputException("Invalid unquoted Oracle schema identifier \"%s\"", identifier);
	}
	OracleIdentifier parsed;
	parsed.name = StringUtil::Upper(value);
	return parsed;
}

static string TransformBlobToOracle(const string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	string result = "HEXTORAW('";
	for (idx_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}
	result += "')";
	return result;
}

string ODBCUtils::TransformConstant(const Value &val) {
	if (val.type().IsNumeric()) {
		return val.ToSQLString();
	}
	if (val.type().id() == LogicalTypeId::BOOLEAN) {
		return BooleanValue::Get(val) ? "1" : "0";
	}
	if (val.type().id() == LogicalTypeId::BLOB) {
		return TransformBlobToOracle(StringValue::Get(val));
	}
	if (val.type().id() == LogicalTypeId::DATE) {
		return "DATE " + WriteLiteral(val.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>());
	}
	if (val.type().id() == LogicalTypeId::TIMESTAMP) {
		return "TIMESTAMP " + WriteLiteral(val.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>());
	}
	if (val.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
		return val.DefaultCastAs(LogicalType::TIMESTAMP).DefaultCastAs(LogicalType::VARCHAR).ToSQLString();
	}
	return val.DefaultCastAs(LogicalType::VARCHAR).ToSQLString();
}

} // namespace duckdb
