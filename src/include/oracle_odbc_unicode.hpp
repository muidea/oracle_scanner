//===----------------------------------------------------------------------===//
//                         DuckDB
//
// oracle_odbc_unicode.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sql.h>
#include <sqlext.h>
#include <codecvt>
#include <limits>
#include <locale>
#include <stdexcept>
#include <string>
#include <vector>

namespace duckdb {

// DuckDB SQL and string values are UTF-8. unixODBC's ANSI entry points can
// decode them using LC_ALL=C even when Oracle NLS_LANG is AL32UTF8. Use the
// Unicode API directly; changing a process-global locale is not safe in a host
// that also loads other scanners. Never retry a failed statement as ANSI SQL.
inline std::vector<SQLWCHAR> OracleODBCUTF8ToWide(const std::string &text) {
	static_assert(sizeof(SQLWCHAR) == sizeof(char16_t), "Oracle ODBC requires UTF-16 SQLWCHAR");
	std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
	auto units = converter.from_bytes(text);
	if (converter.converted() != text.size()) {
		throw std::invalid_argument("Oracle ODBC input is incomplete UTF-8");
	}
	for (size_t i = 0; i < units.size(); i++) {
		if (units[i] >= 0xD800 && units[i] <= 0xDBFF) {
			if (++i == units.size() || units[i] < 0xDC00 || units[i] > 0xDFFF) {
				throw std::invalid_argument("Oracle ODBC input contains an unpaired surrogate");
			}
		} else if (units[i] >= 0xDC00 && units[i] <= 0xDFFF) {
			throw std::invalid_argument("Oracle ODBC input contains an unpaired surrogate");
		}
	}
	std::vector<SQLWCHAR> result(units.begin(), units.end());
	result.push_back(0);
	return result;
}

inline std::vector<SQLWCHAR> OracleODBCWideStatement(const std::string &sql) {
	if (sql.find('\0') != std::string::npos) {
		throw std::invalid_argument("Oracle ODBC SQL contains a null character");
	}
	auto result = OracleODBCUTF8ToWide(sql);
	if (result.size() - 1 > static_cast<size_t>(std::numeric_limits<SQLINTEGER>::max())) {
		throw std::length_error("Oracle ODBC SQL exceeds the statement length limit");
	}
	return result;
}

inline SQLRETURN OracleODBCExecDirectUTF8(SQLHSTMT stmt, const std::string &sql) {
	auto text = OracleODBCWideStatement(sql);
	return SQLExecDirectW(stmt, text.data(), static_cast<SQLINTEGER>(text.size() - 1));
}

inline SQLRETURN OracleODBCPrepareUTF8(SQLHSTMT stmt, const std::string &sql) {
	auto text = OracleODBCWideStatement(sql);
	return SQLPrepareW(stmt, text.data(), static_cast<SQLINTEGER>(text.size() - 1));
}

} // namespace duckdb
