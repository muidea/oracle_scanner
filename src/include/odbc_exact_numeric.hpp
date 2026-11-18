//===----------------------------------------------------------------------===//
//                         DuckDB
//
// odbc_exact_numeric.hpp
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <stdexcept>

namespace duckdb {
// Text is the lossless carrier for exact source numbers outside DECIMAL(38).
// Comparison keys preserve numeric ordering/equality (including exponent text),
// without converting through binary floating point or expanding large exponents.
inline string ODBCExactNumericKey(const string &text) {
	if (text == "NaN" || text == "sNaN" || text == "+NaN" || text == "-NaN")
		return "5";
	if (text == "Infinity" || text == "+Infinity" || text == "INF" || text == "+INF")
		return "4";
	if (text == "-Infinity" || text == "-INF")
		return "0";
	if (text.size() > 200000)
		throw ConversionException("Exact numeric text exceeds supported length");
	const bool negative = !text.empty() && text[0] == '-';
	idx_t pos = !text.empty() && (text[0] == '-' || text[0] == '+') ? 1 : 0;
	string digits;
	int64_t whole = 0, exponent = 0;
	bool dot = false;
	for (; pos < text.size() && text[pos] != 'e' && text[pos] != 'E'; pos++) {
		char c = text[pos];
		if (c == '.' && !dot) {
			dot = true;
			continue;
		}
		if (c < '0' || c > '9')
			throw ConversionException("Invalid exact numeric text");
		digits += c;
		if (!dot)
			whole++;
	}
	if (digits.empty())
		throw ConversionException("Exact numeric text has no digits");
	if (pos < text.size()) {
		pos++;
		bool exponent_negative = pos < text.size() && text[pos] == '-';
		if (pos < text.size() && (text[pos] == '-' || text[pos] == '+'))
			pos++;
		if (pos == text.size())
			throw ConversionException("Exact numeric exponent has no digits");
		for (; pos < text.size(); pos++) {
			if (text[pos] < '0' || text[pos] > '9' || exponent > 1000000)
				throw ConversionException("Invalid exact numeric exponent");
			exponent = exponent * 10 + text[pos] - '0';
		}
		if (exponent_negative)
			exponent = -exponent;
	}
	const auto first = digits.find_first_not_of('0');
	if (first == string::npos)
		return "2";
	const auto last = digits.find_last_not_of('0');
	const int64_t magnitude = whole - static_cast<int64_t>(first) + exponent;
	if (magnitude < -1000000 || magnitude > 1000000)
		throw ConversionException("Exact numeric exponent exceeds supported range");
	uint32_t ordered = static_cast<uint32_t>(static_cast<int32_t>(magnitude)) ^ 0x80000000U;
	if (negative)
		ordered = UINT32_MAX - ordered;
	string key(1, negative ? '1' : '3');
	const char *hex = "0123456789abcdef";
	for (int shift = 28; shift >= 0; shift -= 4)
		key += hex[(ordered >> shift) & 15];
	for (idx_t i = first; i <= last; i++)
		key += negative ? '9' - (digits[i] - '0') : digits[i];
	key += negative ? ':' : '/';
	return key;
}
inline void ODBCExactNumericKeyFunction(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t value) {
		return StringVector::AddString(result, ODBCExactNumericKey(value.GetString()));
	});
}
inline unique_ptr<FunctionData> BindODBCExactNumericKey(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
	if (arguments.size() != 1 || arguments[0]->GetReturnType().id() != LogicalTypeId::VARCHAR)
		throw BinderException("Exact numeric collation requires text input");
	input.GetBoundFunction().GetArguments()[0] = arguments[0]->GetReturnType();
	return nullptr;
}
inline void RegisterODBCExactNumericCollation(ExtensionLoader &loader, const string &name) {
	ScalarFunction key(Identifier(name), {LogicalType::VARCHAR}, LogicalType::VARCHAR, ODBCExactNumericKeyFunction,
	                   BindODBCExactNumericKey);
	key.SetCollationHandling(FunctionCollationHandling::IGNORE_COLLATIONS);
	CreateCollationInfo info(Identifier(name), key, false, false);
	loader.RegisterCollation(info);
}
} // namespace duckdb
