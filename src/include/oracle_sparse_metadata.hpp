//===----------------------------------------------------------------------===//
//                         DuckDB
//
// oracle_sparse_metadata.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

// LONG values stay raw ODBC text. Only the sparse, identity-checked subset
// carries LONG; ordinary directory rows can use the driver's normal prefetch.
inline bool OracleMetadataHasText(const vector<unique_ptr<DataChunk>> &chunks, idx_t column) {
	bool found = false;
	for (const auto &chunk : chunks) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto v = chunk->GetValue(column, r);
			if (v.IsNull() || (v.ToString() != "0" && v.ToString() != "1"))
				throw IOException("Oracle metadata returned an invalid sparse text marker");
			found |= v.ToString() == "1";
		}
	}
	return found;
}

inline string OracleMetadataTextKey(DataChunk &chunk, idx_t row, const vector<idx_t> &keys) {
	string key;
	for (auto column : keys) {
		auto value = chunk.GetValue(column, row);
		if (value.IsNull())
			throw IOException("Oracle metadata returned a NULL sparse text identity");
		auto text = value.GetValue<string>();
		key += std::to_string(text.size()) + ":" + text;
	}
	return key;
}

inline void MergeOracleMetadataText(vector<unique_ptr<DataChunk>> &chunks, idx_t column, const vector<idx_t> &keys,
                                    const vector<unique_ptr<DataChunk>> &sparse) {
	struct Target {
		DataChunk *chunk;
		idx_t row;
		bool expected, seen;
	};
	unordered_map<string, Target> targets;
	OracleMetadataHasText(chunks, column);
	for (auto &chunk : chunks) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto key = OracleMetadataTextKey(*chunk, r, keys);
			if (!targets.emplace(key, Target {chunk.get(), r, chunk->GetValue(column, r).ToString() == "1", false})
			         .second)
				throw IOException("Oracle metadata returned a duplicate sparse text identity");
		}
	}
	vector<idx_t> sparse_keys;
	for (idx_t i = 0; i < keys.size(); i++)
		sparse_keys.push_back(i);
	for (const auto &chunk : sparse) {
		if (chunk->ColumnCount() != keys.size() + 1)
			throw IOException("Oracle metadata returned an invalid sparse text descriptor");
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto key = OracleMetadataTextKey(*chunk, r, sparse_keys);
			auto found = targets.find(key);
			if (found == targets.end() || !found->second.expected || found->second.seen)
				throw IOException("Oracle metadata sparse text scope changed or returned duplicate values");
			auto &target = found->second;
			target.chunk->SetValue(column, target.row, chunk->GetValue(keys.size(), r));
			target.seen = true;
		}
	}
	for (auto &entry : targets) {
		auto &target = entry.second;
		if (target.expected && !target.seen)
			throw IOException("Oracle metadata sparse text observation is incomplete; retry collection");
		if (!target.expected)
			target.chunk->SetValue(column, target.row, Value());
	}
}
} // namespace duckdb
