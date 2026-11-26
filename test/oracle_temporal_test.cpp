#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "odbc_filter_pushdown.hpp"
#include "odbc_parameter.hpp"
#include "odbc_types.hpp"
#include "odbc_utils.hpp"
#include "oracle_temporal_utils.hpp"

#include <cstring>
#include <iostream>
#include <stdexcept>

using namespace duckdb;

static void Require(bool value, const char *msg) {
	if (!value) {
		throw std::runtime_error(msg);
	}
}

template <typename Fn>
static void RequireConversionError(Fn &&fn, const char *msg) {
	bool threw = false;
	try {
		fn();
	} catch (const ConversionException &) {
		threw = true;
	}
	Require(threw, msg);
}

int main() {
	// 1. Session timezone validation
	for (const char *ok : {"", " ", "+00:00", "utc", "UTC", "Etc/UTC", "GMT", "Z"}) {
		Require(IsAllowedOracleSessionTimeZone(ok), "expected allowed UTC session timezone");
	}
	for (const char *bad : {"+08:00", "-05:00", "Asia/Shanghai", "SYSTEM"}) {
		Require(!IsAllowedOracleSessionTimeZone(bad), "expected rejected non-UTC session timezone");
	}

	// 2. ODBCTypes mapping (TZ-D3: both TSTZ and TSLTZ map to TIMESTAMP_TZ, DATE -> TIMESTAMP)
	ODBCTypeConfig cfg;
	auto check_type = [&](const string &name, LogicalTypeId expected) {
		ODBCTypeData td;
		td.type_name = name;
		td.column_type = name;
		Require(ODBCTypes::TypeToLogicalType(cfg, td).id() == expected, name.c_str());
	};
	check_type("DATE", LogicalTypeId::TIMESTAMP);
	check_type("TIMESTAMP(6)", LogicalTypeId::TIMESTAMP);
	check_type("TIMESTAMP(6) WITH TIME ZONE", LogicalTypeId::TIMESTAMP_TZ);
	check_type("TIMESTAMP(6) WITH LOCAL TIME ZONE", LogicalTypeId::TIMESTAMP_TZ);
	Require(ODBCTypes::FieldToLogicalType(cfg, SQL_TYPE_TIMESTAMP, 26, 6, "TIMESTAMP WITH LOCAL TIME ZONE").id() ==
	            LogicalTypeId::TIMESTAMP_TZ,
	        "FieldToLogicalType TSLTZ -> TIMESTAMP_TZ");

	// 3. Astronomical <-> Oracle historical BC year conversion (1 BC: DuckDB 0 <-> Oracle -1)
	Require(DuckDBYearToOracleYear(0) == -1 && DuckDBYearToOracleYear(-43) == -44 && DuckDBYearToOracleYear(2026) == 2026,
	        "DuckDBYearToOracleYear");
	Require(OracleYearToDuckDBYear(-1) == 0 && OracleYearToDuckDBYear(-44) == -43 && OracleYearToDuckDBYear(2026) == 2026,
	        "OracleYearToDuckDBYear");
	Require(IsDuckDBYearInOracleRange(-4712) && IsDuckDBYearInOracleRange(9999) &&
	            !IsDuckDBYearInOracleRange(-4713) && !IsDuckDBYearInOracleRange(10000),
	        "Oracle year range");

	// 4. ParseOracleTimestampTZText (AD SYYYY space, BC SYYYY -YYYY, implicit UTC vs strict)
	timestamp_tz_t parsed_tz;
	auto want_ad = timestamp_tz_t(Timestamp::FromDatetime(Date::FromDate(2026, 1, 1), Time::FromTime(19, 4, 5, 123456)));
	Require(ParseOracleTimestampTZText(" 2026-01-02 03:04:05.123456 +08:00", false, parsed_tz) && parsed_tz == want_ad,
	        "AD TSTZ with SYYYY leading space");
	Require(!ParseOracleTimestampTZText("2026-01-01 19:04:05.123456000", false, parsed_tz),
	        "strict mode rejects offsetless string");
	Require(ParseOracleTimestampTZText("2026-01-01 19:04:05.123456000", true, parsed_tz) && parsed_tz == want_ad,
	        "raw ODBC mode treats plain offsetless string under +00:00 as UTC");
	Require(!ParseOracleTimestampTZText("2026-01-01 19:04:05 Asia/Shanghai", true, parsed_tz),
	        "raw ODBC mode rejects non-numeric trailing zone text");

	auto want_44bc =
	    timestamp_tz_t(Timestamp::FromDatetime(Date::FromDate(-43, 3, 15), Time::FromTime(12, 34, 56, 123456)));
	Require(ParseOracleTimestampTZText("-0044-03-15 12:34:56.123456 +00:00", false, parsed_tz) &&
	            parsed_tz == want_44bc,
	        "BC -0044 maps to astronomical -43");
	auto want_1bc =
	    timestamp_tz_t(Timestamp::FromDatetime(Date::FromDate(0, 1, 1), Time::FromTime(12, 34, 56, 123456)));
	Require(ParseOracleTimestampTZText("-0001-01-01 12:34:56.123456 +00:00", false, parsed_tz) && parsed_tz == want_1bc,
	        "BC -0001 maps to astronomical 0");

	// 5. TryTransformConstant & TransformFiltersWithFallback
	string out;
	Value d_44bc = Value::DATE(Date::FromDate(-43, 3, 15));
	Require(ODBCUtils::TryTransformConstant(d_44bc, out) && out == "TO_DATE('-0044-03-15', 'SYYYY-MM-DD')",
	        "BC DATE pushdown uses SYYYY historical year");
	Value ts_1bc = Value::TIMESTAMP(Timestamp::FromDatetime(Date::FromDate(0, 1, 1), Time::FromTime(0, 0, 0, 123456)));
	Require(ODBCUtils::TryTransformConstant(ts_1bc, out) &&
	            out == "TO_TIMESTAMP('-0001-01-01 00:00:00.123456', 'SYYYY-MM-DD HH24:MI:SS.FF6')",
	        "BC TIMESTAMP pushdown uses SYYYY historical year");
	Value tstz_1bc = Value::TIMESTAMPTZ(want_1bc);
	Require(ODBCUtils::TryTransformConstant(tstz_1bc, out) &&
	            out == "TO_TIMESTAMP_TZ('-0001-01-01 12:34:56.123456 +00:00', 'SYYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')",
	        "BC TIMESTAMPTZ pushdown uses SYYYY historical year");
	Require(!ODBCUtils::TryTransformConstant(Value::TIMESTAMP(timestamp_t::infinity()), out),
	        "infinity TIMESTAMP falls back locally");
	Require(!ODBCUtils::TryTransformConstant(Value::TIME(Time::FromTime(12, 0, 0, 0)), out),
	        "TIME falls back locally");

	{
		TableFilterSet fs;
		fs.filters[0] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, ts_1bc);
		fs.filters[1] = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, Value::TIMESTAMP(timestamp_t::infinity()));
		vector<column_t> col_ids;
		col_ids.push_back(0);
		col_ids.push_back(1);
		vector<string> names;
		names.push_back("C_TS");
		names.push_back("C_INF");
		auto res = ODBCFilterPushdown::TransformFiltersWithFallback(col_ids, &fs, names);
		Require(res.where_clause ==
		            "\"C_TS\" = TO_TIMESTAMP('-0001-01-01 00:00:00.123456', 'SYYYY-MM-DD HH24:MI:SS.FF6')",
		        "pushable BC filter pushed");
		Require(res.unpushed_filter_columns.size() == 1 && res.unpushed_filter_columns[0] == 1,
		        "unpushable infinity filter recorded for local eval");
	}

	// 6. ODBCParameter contracts (BC SYYYY + TIMESTAMP_TZ DecimalDigits=6 + guards)
	{
		ODBCParameter p_date("q", d_44bc);
		Require(p_date.buffer_type == SQL_TYPE_DATE && p_date.column_size == 10 && p_date.decimal_digits == 0,
		        "DATE parameter metadata");
		SQL_DATE_STRUCT ds {};
		std::memcpy(&ds, p_date.bind_buffer.data(), sizeof(SQL_DATE_STRUCT));
		Require(ds.year == -44 && ds.month == 3 && ds.day == 15, "DATE BC parameter struct uses historical -44");

		ODBCParameter p_tstz("q", tstz_1bc);
		Require(p_tstz.buffer_type == SQL_TYPE_TIMESTAMP && p_tstz.column_size == 26 && p_tstz.decimal_digits == 6,
		        "TIMESTAMP_TZ parameter uses SQL_TYPE_TIMESTAMP DecimalDigits=6");
		SQL_TIMESTAMP_STRUCT ts {};
		std::memcpy(&ts, p_tstz.bind_buffer.data(), sizeof(SQL_TIMESTAMP_STRUCT));
		Require(ts.year == -1 && ts.month == 1 && ts.day == 1 && ts.hour == 12 && ts.fraction == 123456000,
		        "TIMESTAMPTZ BC parameter struct uses historical -1 and nanosecond fraction");

		ODBCParameter p_time("q", Value::TIME(Time::FromTime(3, 4, 5, 123456)));
		Require(p_time.buffer_type == SQL_VARCHAR && p_time.use_text_buffer && p_time.text_buffer == "03:04:05.123456",
		        "TIME parameter preserves microseconds via text");
	}
	RequireConversionError([&] { ODBCParameter p("q", Value::DATE(date_t::infinity())); },
	                       "infinity DATE param must throw");
	RequireConversionError([&] { ODBCParameter p("q", Value::DATE(Date::FromDate(-5000, 1, 1))); },
	                       "year < -4712 DATE param must throw");
	RequireConversionError([&] { ODBCParameter p("q", Value::TIME(Time::FromTime(24, 0, 0, 0))); },
	                       "24:00:00 TIME param must throw");
	RequireConversionError([&] { ODBCParameter p("q", Value::TIMESTAMP(timestamp_t::infinity())); },
	                       "infinity TIMESTAMP param must throw");

	std::cout << "PASS: oracle temporal session TZ, TSLTZ mapping, SYYYY BC conversion, fallback, and params\n";
	return 0;
}
