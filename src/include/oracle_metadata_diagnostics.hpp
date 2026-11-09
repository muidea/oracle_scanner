//===----------------------------------------------------------------------===//
//                         DuckDB
//
// oracle_metadata_diagnostics.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "odbc_connection.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/string_util.hpp"
#include <chrono>
#include <cstdio>

namespace duckdb {

struct OracleMetadataPhases {
	// Bind can run during Prepare with no active query. Optional diagnostics
	// must never depend on execution-only ClientContext state.
	OracleMetadataPhases(const string &catalog_p, const string &owner_p, const string &category_p,
	                     idx_t tables_p) noexcept
	    : tables(tables_p) {
		try {
			catalog = catalog_p;
			owner = owner_p;
			category = category_p;
		} catch (...) { /* Keep collection independent of diagnostics. */
		}
	}
	string catalog, owner, category, sql, sql_fingerprint;
	idx_t tables;
	ODBCQueryMetrics probe, query;
	int probe_calls = 0, category_calls = 0;
	bool success = false;
	std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
	void SetSQL(const string &value) noexcept {
		try {
			sql = value;
			MD5Context digest;
			digest.Add(sql);
			sql_fingerprint = digest.FinishHex();
		} catch (...) { /* Diagnostics must never change source execution. */
		}
	}
	~OracleMetadataPhases() noexcept {
		try {
			auto elapsed =
			    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started)
			        .count();
			auto quote = [](string v) {
				v = StringUtil::Replace(v, "\\", "\\\\");
				v = StringUtil::Replace(v, "\"", "\\\"");
				v = StringUtil::Replace(v, "\n", "\\n");
				return StringUtil::Replace(v, "\r", "\\r");
			};
			std::fprintf(
			    stderr,
			    "oracle metadata native phases catalog=\"%s\" owner=\"%s\" category=\"%s\" target_count=%llu "
			    "probe_sql_count=%d category_sql_count=%d probe_execute_ms=%lld probe_materialize_ms=%lld "
			    "category_execute_ms=%lld category_materialize_ms=%lld category_fetch_calls=%llu "
			    "category_get_data_calls=%llu category_rows=%llu category_value_bytes=%llu category_fetch_us=%lld "
			    "category_get_data_us=%lld category_convert_us=%lld "
			    "connection_id=%llu connection_query_sequence=%llu "
			    "category_direct_us=%lld category_prepare_us=%lld category_parameter_us=%lld category_bind_us=%lld "
			    "category_execute_us=%lld category_result_info_us=%lld "
			    "category_direct_calls=%llu category_prepare_calls=%llu category_parameter_calls=%llu "
			    "category_bind_calls=%llu category_execute_calls=%llu "
			    "probe_direct_us=%lld probe_direct_calls=%llu sql_fingerprint=\"%s\" category_sql=\"%s\" total_ms=%lld "
			    "result=%s\n",
			    quote(catalog).c_str(), quote(owner).c_str(), quote(category).c_str(),
			    static_cast<unsigned long long>(tables), probe_calls, category_calls,
			    static_cast<long long>(probe.execute_ms), static_cast<long long>(probe.materialize_ms),
			    static_cast<long long>(query.execute_ms), static_cast<long long>(query.materialize_ms),
			    static_cast<unsigned long long>(query.fetch_calls),
			    static_cast<unsigned long long>(query.get_data_calls), static_cast<unsigned long long>(query.rows),
			    static_cast<unsigned long long>(query.value_bytes), static_cast<long long>(query.fetch_us),
			    static_cast<long long>(query.get_data_us), static_cast<long long>(query.convert_us),
			    static_cast<unsigned long long>(query.connection_id ? query.connection_id : probe.connection_id),
			    static_cast<unsigned long long>(query.connection_query_sequence ? query.connection_query_sequence
			                                                                    : probe.connection_query_sequence),
			    static_cast<long long>(query.direct_us), static_cast<long long>(query.prepare_us),
			    static_cast<long long>(query.parameter_us), static_cast<long long>(query.bind_us),
			    static_cast<long long>(query.execute_us), static_cast<long long>(query.result_info_us),
			    static_cast<unsigned long long>(query.direct_calls),
			    static_cast<unsigned long long>(query.prepare_calls),
			    static_cast<unsigned long long>(query.parameter_calls),
			    static_cast<unsigned long long>(query.bind_calls), static_cast<unsigned long long>(query.execute_calls),
			    static_cast<long long>(probe.direct_us), static_cast<unsigned long long>(probe.direct_calls),
			    quote(sql_fingerprint).c_str(), quote(sql).c_str(), static_cast<long long>(elapsed),
			    success ? "success" : "failed");
		} catch (...) { /* Diagnostics must never change source execution. */
		}
	}
};

} // namespace duckdb
