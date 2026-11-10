#include "oracle_sparse_metadata.hpp"
#include <functional>
#include <iostream>

using namespace duckdb;

static unique_ptr<DataChunk> Chunk(const vector<vector<Value>> &rows) {
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), vector<LogicalType>(rows[0].size(), LogicalType::VARCHAR));
	chunk->SetCardinality(rows.size());
	for (idx_t r = 0; r < rows.size(); r++)
		for (idx_t c = 0; c < rows[r].size(); c++)
			chunk->SetValue(c, r, rows[r][c]);
	return chunk;
}

static void Require(bool ok) {
	if (!ok)
		throw std::runtime_error("sparse Oracle metadata contract failed");
}

static void Reject(const std::function<void()> &fn) {
	bool rejected = false;
	try {
		fn();
	} catch (const IOException &) {
		rejected = true;
	}
	Require(rejected);
}

int main() {
	// Full text equality, including the CRLF case that made XML unsuitable.
	const string expression = "'指标'\r\n" + string(20000, 'x') + "  ";
	vector<unique_ptr<DataChunk>> base, sparse;
	base.push_back(Chunk({{"APP", "a:b", "c", "1"}, {"APP", "a", "b:c", "0"}, {"APP", "quoted ", "NULL", "1"}}));
	sparse.push_back(Chunk({{"APP", "quoted ", "NULL", Value()}, {"APP", "a:b", "c", expression}}));
	MergeOracleMetadataText(base, 3, {0, 1, 2}, sparse);
	Require(base[0]->GetValue(3, 0).GetValue<string>() == expression);
	Require(base[0]->GetValue(3, 1).IsNull() && base[0]->GetValue(3, 2).IsNull());
	// Missing, duplicate, extra, and changed identities must fail before bind
	// can publish a partial observation as complete.
	for (const auto &rows : vector<vector<vector<Value>>> {
	         {}, {{"APP", "t", "c", "x"}, {"APP", "t", "c", "y"}}, {{"OTHER", "t", "c", "x"}}}) {
		base.clear();
		sparse.clear();
		base.push_back(Chunk({{"APP", "t", "c", "1"}}));
		if (!rows.empty())
			sparse.push_back(Chunk(rows));
		Reject([&] { MergeOracleMetadataText(base, 3, {0, 1, 2}, sparse); });
	}
	base.clear();
	sparse.clear();
	base.push_back(Chunk({{"APP", "t", "c", "0"}}));
	sparse.push_back(Chunk({{"APP", "t", "c", "unexpected"}}));
	Reject([&] { MergeOracleMetadataText(base, 3, {0, 1, 2}, sparse); });
	base.clear();
	sparse.clear();
	base.push_back(Chunk({{"APP", "t", "c", "1"}, {"APP", "t", "c", "1"}}));
	Reject([&] { MergeOracleMetadataText(base, 3, {0, 1, 2}, sparse); });
	// Composite index identity includes position; ordinary keys stay NULL.
	base.clear();
	sparse.clear();
	base.push_back(Chunk({{"APP", "t", "APP:idx", "0", "2"}, {"APP", "t", "APP:idx", "1", "1"}}));
	sparse.push_back(Chunk({{"APP", "t", "APP:idx", "1", "upper(\"指标\")"}}));
	MergeOracleMetadataText(base, 3, {0, 1, 2, 4}, sparse);
	Require(base[0]->GetValue(3, 0).IsNull());
	Require(base[0]->GetValue(3, 1).ToString() == "upper(\"指标\")");
	std::cout << "PASS: sparse LONG identity, complete coverage, exact text and composite index positions\n";
}
