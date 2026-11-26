//===----------------------------------------------------------------------===//
//                         DuckDB
//
// oracle_temporal_utils.hpp
//
// Helpers for Oracle session timezone normalization, SYYYY BC conversion,
// range checks, and offset parsing
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <cctype>
#include <cstdio>

namespace duckdb {

static constexpr const char *ORACLE_SESSION_TEMPORAL_SQL =
    "ALTER SESSION SET TIME_ZONE = '+00:00' NLS_CALENDAR = 'GREGORIAN' "
    "NLS_TIMESTAMP_TZ_FORMAT = 'SYYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM' "
    "NLS_TIMESTAMP_FORMAT = 'SYYYY-MM-DD HH24:MI:SS.FF6' "
    "NLS_DATE_FORMAT = 'SYYYY-MM-DD HH24:MI:SS'";

inline bool IsAllowedOracleSessionTimeZone(const string &raw) {
	string normalized = raw;
	StringUtil::Trim(normalized);
	normalized = StringUtil::Lower(normalized);
	return normalized.empty() || normalized == "utc" || normalized == "+00:00" || normalized == "-00:00" ||
	       normalized == "00:00" || normalized == "z" || normalized == "gmt" || normalized == "etc/utc" ||
	       normalized == "etc/gmt" || normalized == "etc/gmt+0" || normalized == "etc/gmt-0";
}

// DuckDB uses astronomical years (year 0 = 1 BC, -43 = 44 BC), whereas Oracle
// uses historical years (no year 0: -1 = 1 BC, -44 = 44 BC).
inline bool IsDuckDBYearInOracleRange(int32_t duck_year) {
	return duck_year >= -4712 && duck_year <= 9999;
}

inline int32_t DuckDBYearToOracleYear(int32_t duck_year) {
	return duck_year <= 0 ? (duck_year - 1) : duck_year;
}

inline int32_t OracleYearToDuckDBYear(int32_t ora_year) {
	return ora_year < 0 ? (ora_year + 1) : ora_year;
}

inline string FormatOracleSYYYY(int32_t ora_year) {
	char buf[16];
	if (ora_year < 0) {
		std::snprintf(buf, sizeof(buf), "-%04d", -ora_year);
	} else {
		std::snprintf(buf, sizeof(buf), "%04d", ora_year);
	}
	return string(buf);
}

inline string FormatOracleDateSYYYY(int32_t ora_year, int32_t month, int32_t day) {
	char buf[16];
	std::snprintf(buf, sizeof(buf), "-%02d-%02d", month, day);
	return FormatOracleSYYYY(ora_year) + string(buf);
}

inline string FormatOracleTimestampSYYYY(int32_t ora_year, int32_t month, int32_t day, int32_t hour, int32_t minute,
                                         int32_t second, int32_t micros) {
	char buf[40];
	std::snprintf(buf, sizeof(buf), "-%02d-%02d %02d:%02d:%02d.%06d", month, day, hour, minute, second, micros);
	return FormatOracleSYYYY(ora_year) + string(buf);
}

inline string FormatOracleTimestampTZSYYYY(int32_t ora_year, int32_t month, int32_t day, int32_t hour, int32_t minute,
                                           int32_t second, int32_t micros) {
	return FormatOracleTimestampSYYYY(ora_year, month, day, hour, minute, second, micros) + " +00:00";
}

inline string FormatOracleTimeISO(int32_t hour, int32_t minute, int32_t second, int32_t micros) {
	char buf[32];
	if (micros == 0) {
		std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", hour, minute, second);
	} else {
		std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d", hour, minute, second, micros);
	}
	return string(buf);
}

inline bool ParseOracleTimestampTZText(const string &raw, bool allow_implicit_utc, timestamp_tz_t &out) {
	string text = raw;
	StringUtil::Trim(text);
	if (text.empty()) {
		return false;
	}

	idx_t time_start = string::npos;
	if (text.size() >= 11 && text[0] == '-' && std::isdigit(static_cast<unsigned char>(text[1])) &&
	    std::isdigit(static_cast<unsigned char>(text[2])) && std::isdigit(static_cast<unsigned char>(text[3])) &&
	    std::isdigit(static_cast<unsigned char>(text[4])) && text[5] == '-') {
		auto sep = text.find_first_of(" T", 6);
		if (sep == string::npos) {
			return false;
		}
		string date_part = text.substr(1, sep - 1);
		string rest = text.substr(sep);
		text = date_part + " (BC)" + rest;
		time_start = date_part.size() + 5; // right after " (BC)"
		while (time_start < text.size() && (text[time_start] == ' ' || text[time_start] == 'T')) {
			time_start++;
		}
	} else {
		auto sep = text.find_first_of(" T");
		if (sep == string::npos) {
			return false;
		}
		auto bc_pos = text.find("(BC)", sep);
		if (bc_pos == sep + 1) {
			time_start = bc_pos + 4;
			while (time_start < text.size() && (text[time_start] == ' ' || text[time_start] == 'T')) {
				time_start++;
			}
		} else {
			time_start = sep + 1;
		}
	}

	if (time_start >= text.size()) {
		return false;
	}

	auto offset_pos = text.find_first_of("+-Zz", time_start);
	string normalized;
	if (offset_pos != string::npos) {
		auto prefix = text.substr(0, offset_pos);
		StringUtil::Trim(prefix);
		auto suffix = text.substr(offset_pos);
		StringUtil::Trim(suffix);
		if (suffix == "z" || suffix == "Z") {
			normalized = prefix + "Z";
		} else {
			if ((suffix[0] == '+' || suffix[0] == '-') && suffix.size() >= 2 &&
			    std::isdigit(static_cast<unsigned char>(suffix[1])) &&
			    (suffix.size() == 2 || !std::isdigit(static_cast<unsigned char>(suffix[2])))) {
				suffix.insert(1, 1, '0');
			}
			normalized = prefix + suffix;
		}
	} else {
		if (!allow_implicit_utc) {
			return false;
		}
		string time_part = text.substr(time_start);
		StringUtil::Trim(time_part);
		if (time_part.empty()) {
			return false;
		}
		for (char ch : time_part) {
			if (!std::isdigit(static_cast<unsigned char>(ch)) && ch != ':' && ch != '.') {
				return false;
			}
		}
		normalized = text + "+00:00";
	}

	timestamp_t ts_val;
	bool has_offset = false;
	string_t tz(nullptr, 0);
	if (Timestamp::TryConvertTimestampTZ(normalized.c_str(), normalized.size(), ts_val, true, has_offset, tz) !=
	        TimestampCastResult::SUCCESS ||
	    !has_offset || tz.GetSize() != 0) {
		return false;
	}
	out = timestamp_tz_t(ts_val.value);
	return true;
}

} // namespace duckdb
