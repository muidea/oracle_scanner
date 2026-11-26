#include "odbc_types.hpp"
#include <algorithm>
#include <cstdio>

namespace duckdb {

static bool GetBoolOption(ClientContext &context, const std::string &opt_name) {
	Value val;
	if (context.TryGetCurrentSetting(opt_name, val)) {
		return BooleanValue::Get(val);
	}
	return false;
}

ODBCTypeConfig::ODBCTypeConfig() {
}

ODBCTypeConfig::ODBCTypeConfig(ClientContext &context)
    : bit1_as_boolean(false), tinyint1_as_boolean(false), time_as_time(GetBoolOption(context, "oracle_time_as_time")) {
}

LogicalType ODBCTypes::TypeToLogicalType(const ODBCTypeConfig &type_config, const ODBCTypeData &type_info) {
	auto type_name = StringUtil::Lower(type_info.type_name);
	StringUtil::Trim(type_name);

	if (StringUtil::Contains(type_name, "timestamp") &&
	    (StringUtil::Contains(type_name, "with time zone") ||
	     StringUtil::Contains(type_name, "with local time zone"))) {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (StringUtil::Contains(type_name, "timestamp")) {
		return LogicalType::TIMESTAMP;
	}
	if (type_name == "date") {
		// Oracle DATE includes hours, minutes and seconds.
		return LogicalType::TIMESTAMP;
	}
	if (type_name == "number" || type_name == "numeric" || type_name == "decimal" || type_name == "integer") {
		// NUMBER(p,-s) needs p+s integer digits; NUMBER(p,s>p) needs
		// scale digits in DuckDB. Unknown/wide descriptors remain exact text.
		auto width = std::max(type_info.precision, type_info.scale);
		if (type_info.scale < 0)
			width = type_info.precision - type_info.scale;
		if (type_info.precision > 0 && width > 0 && width <= 38 && type_info.scale <= 38) {
			return LogicalType::DECIMAL(NumericCast<uint8_t>(width),
			                            NumericCast<uint8_t>(std::max<int64_t>(0, type_info.scale)));
		}
		return LogicalType::VARCHAR_COLLATION("oracle_exact_numeric");
	}
	if (type_name == "binary_float" || type_name == "real") {
		return LogicalType::FLOAT;
	}
	if (type_name == "binary_double" || type_name == "float" || type_name == "double" ||
	    type_name == "double precision") {
		return LogicalType::DOUBLE;
	}
	if (type_name == "smallint") {
		return LogicalType::SMALLINT;
	}
	if (type_name == "int") {
		return LogicalType::INTEGER;
	}
	if (type_name == "bigint") {
		return LogicalType::BIGINT;
	}
	if (type_name == "tinyint") {
		return LogicalType::TINYINT;
	}
	if (type_name == "boolean" || type_name == "bit") {
		return LogicalType::BOOLEAN;
	}
	if (type_name == "time") {
		return type_config.time_as_time ? LogicalType::TIME : LogicalType::VARCHAR;
	}
	if (type_name == "raw" || type_name == "long raw" || type_name == "blob" || type_name == "binary" ||
	    type_name == "varbinary" || type_name == "longvarbinary") {
		return LogicalType::BLOB;
	}
	if (type_name == "char" || type_name == "nchar" || type_name == "varchar" || type_name == "varchar2" ||
	    type_name == "nvarchar" || type_name == "nvarchar2" || type_name == "long" || type_name == "text" ||
	    type_name == "longvarchar" || type_name == "clob" || type_name == "nclob" || type_name == "rowid" ||
	    type_name == "urowid" || type_name == "xmltype" || type_name == "json" ||
	    StringUtil::Contains(type_name, "interval")) {
		return LogicalType::VARCHAR;
	}
	return LogicalType::VARCHAR;
}

LogicalType ODBCTypes::FieldToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN col_size,
                                          SQLSMALLINT decimal_digits, const string &reported_type_name) {
	ODBCTypeData type_data;
	type_data.type_name = StringUtil::Lower(reported_type_name);
	type_data.precision = col_size > 0 ? NumericCast<int64_t>(col_size) : -1;
	type_data.scale = decimal_digits;

	// Oracle describes unconstrained NUMBER as SQL_FLOAT with a placeholder
	// precision/scale (often 38,0). It is decimal source data, not IEEE float,
	// and that placeholder cannot establish a fixed DECIMAL scale.
	if ((type_data.type_name == "number" || type_data.type_name == "numeric" || type_data.type_name == "decimal") &&
	    (data_type == SQL_FLOAT || data_type == SQL_DOUBLE || data_type == SQL_REAL)) {
		return LogicalType::VARCHAR_COLLATION("oracle_exact_numeric");
	}

	if (!type_data.type_name.empty()) {
		type_data.column_type = type_data.type_name;
		return TypeToLogicalType(type_config, type_data);
	}

	switch (data_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
		type_data.type_name = "varchar";
		break;
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		type_data.type_name = "nvarchar";
		break;
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		type_data.type_name = "number";
		break;
	case SQL_SMALLINT:
		type_data.type_name = "smallint";
		break;
	case SQL_INTEGER:
		type_data.type_name = "int";
		break;
	case SQL_REAL:
		type_data.type_name = "real";
		break;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		type_data.type_name = "double";
		break;
	case SQL_BIT:
		type_data.type_name = "bit";
		break;
	case SQL_TINYINT:
		type_data.type_name = "tinyint";
		break;
	case SQL_BIGINT:
		type_data.type_name = "bigint";
		break;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		type_data.type_name = "varbinary";
		break;
	case SQL_DATE:
	case SQL_TYPE_DATE:
		type_data.type_name = "date";
		break;
	case SQL_TIME:
	case SQL_TYPE_TIME:
		type_data.type_name = "time";
		break;
	case SQL_TIMESTAMP:
	case SQL_TYPE_TIMESTAMP:
		type_data.type_name = "timestamp";
		break;
	case SQL_INTERVAL_MONTH:
	case SQL_INTERVAL_YEAR:
	case SQL_INTERVAL_YEAR_TO_MONTH:
	case SQL_INTERVAL_DAY:
	case SQL_INTERVAL_HOUR:
	case SQL_INTERVAL_MINUTE:
	case SQL_INTERVAL_SECOND:
	case SQL_INTERVAL_DAY_TO_HOUR:
	case SQL_INTERVAL_DAY_TO_MINUTE:
	case SQL_INTERVAL_DAY_TO_SECOND:
	case SQL_INTERVAL_HOUR_TO_MINUTE:
	case SQL_INTERVAL_HOUR_TO_SECOND:
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		type_data.type_name = "interval";
		break;
	case SQL_GUID:
		type_data.type_name = "uniqueidentifier";
		break;
	default:
		type_data.type_name = "varchar";
		break;
	}

	type_data.column_type = type_data.type_name;
	return ODBCTypes::TypeToLogicalType(type_config, type_data);
}

SQLSMALLINT ODBCTypes::GetCType(SQLSMALLINT sql_type) {

	switch (sql_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		return SQL_C_WCHAR;
	case SQL_TINYINT:
		return SQL_C_UTINYINT;
	case SQL_SMALLINT:
		return SQL_C_SSHORT;
	case SQL_INTEGER:
		return SQL_C_SLONG;
	case SQL_BIGINT:
		return SQL_C_SBIGINT;
	case SQL_REAL:
		return SQL_C_FLOAT;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		return SQL_C_DOUBLE;
	case SQL_BIT:
		return SQL_C_BIT;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		return SQL_C_BINARY;
	case SQL_DATE:
	case SQL_TYPE_DATE:
		return SQL_C_TYPE_DATE;
	case SQL_TIME:
	case SQL_TYPE_TIME:
		return SQL_C_TYPE_TIME;
	case SQL_TIMESTAMP:
	case SQL_TYPE_TIMESTAMP:
		return SQL_C_TYPE_TIMESTAMP;
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		return SQL_C_CHAR; // Handle as string for simplicity
	case SQL_GUID:
		return SQL_C_GUID;
	default:
		return SQL_C_CHAR; // Default to string
	}
}

LogicalType ODBCTypes::ODBCToLogicalType(const ODBCTypeConfig &type_config, SQLSMALLINT data_type, SQLULEN column_size,
                                         SQLSMALLINT decimal_digits, const string &type_name) {
	return ODBCTypes::FieldToLogicalType(type_config, data_type, column_size, decimal_digits, type_name);
}

} // namespace duckdb
