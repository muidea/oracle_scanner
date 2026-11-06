#include "odbc_exact_numeric.hpp"
#include "odbc_types.hpp"
#include <iostream>
using namespace duckdb;
static void Require(bool ok) {
	if (!ok)
		throw std::runtime_error("exact numeric contract failed");
}
int main() {
	for (auto pair : vector<std::pair<string, string>> {
	         {"1", "1.000"}, {"10", "1E1"}, {"-0", "0.000E999"}, {"0.001", "1e-3"}, {"-123.45", "-12345e-2"}})
		Require(ODBCExactNumericKey(pair.first) == ODBCExactNumericKey(pair.second));
	vector<string> ordered = {"-Infinity",
	                          "-1e6144",
	                          "-1000",
	                          "-1.01",
	                          "-1",
	                          "-0.1",
	                          "-1e-6143",
	                          "0",
	                          "1e-6143",
	                          "0.001",
	                          "0.1",
	                          "1",
	                          "1.01",
	                          "10",
	                          "9007199254740992",
	                          "9007199254740993",
	                          "99999999999999999999999999999999999999",
	                          "1e6144",
	                          "Infinity",
	                          "NaN"};
	for (idx_t i = 1; i < ordered.size(); i++)
		Require(ODBCExactNumericKey(ordered[i - 1]) < ODBCExactNumericKey(ordered[i]));
	for (auto bad : vector<string> {"", ".", "+", "1e", "1e+", "1.2.3", "1e9999999", "a", "1x"}) {
		bool rejected = false;
		try {
			ODBCExactNumericKey(bad);
		} catch (const Exception &) {
			rejected = true;
		}
		Require(rejected);
	}
	ODBCTypeConfig config;
#ifdef EXACT_NUMERIC_ORACLE
	auto map = [&](int64_t p, int64_t s) {
		ODBCTypeData descriptor;
		descriptor.type_name = "NUMBER";
		descriptor.column_type = "NUMBER";
		descriptor.precision = p;
		descriptor.scale = s;
		return ODBCTypes::TypeToLogicalType(config, descriptor);
	};
	Require(map(0, -127).id() == LogicalTypeId::VARCHAR &&
	        StringType::GetCollation(map(0, -127)) == "oracle_exact_numeric");
	Require(map(10, -2) == LogicalType::DECIMAL(12, 0));
	Require(map(4, 5) == LogicalType::DECIMAL(5, 5));
	Require(map(38, 0) == LogicalType::DECIMAL(38, 0));
	Require(map(38, -1).id() == LogicalTypeId::VARCHAR);
	Require(ODBCTypes::FieldToLogicalType(config, SQL_FLOAT, 38, 0, "NUMBER").id() == LogicalTypeId::VARCHAR);
	Require(ODBCTypes::FieldToLogicalType(config, SQL_DECIMAL, 38, 0, "NUMBER") == LogicalType::DECIMAL(38, 0));
	Require(ODBCTypes::FieldToLogicalType(config, SQL_DOUBLE, 15, 0, "BINARY_DOUBLE") == LogicalType::DOUBLE);

#else
	auto map = [&](string name, int64_t p, int64_t s) {
		return ODBCTypes::TypeToLogicalType(config, {name, name, p, s});
	};
	Require(map("DECFLOAT", 34, 0).id() == LogicalTypeId::VARCHAR);
	Require(StringType::GetCollation(map("DECFLOAT", 34, 0)) == "odbc_exact_numeric");
	Require(map("DECIMAL", 38, 9) == LogicalType::DECIMAL(38, 9));
	Require(map("NUMERIC", 0, -1).id() == LogicalTypeId::VARCHAR);
	Require(map("DECIMAL", 40, 0).id() == LogicalTypeId::VARCHAR);
#endif
	std::cout << "PASS: lossless descriptors, exponent/decimal/negative/NaN ordering, invalid input\n";
}
