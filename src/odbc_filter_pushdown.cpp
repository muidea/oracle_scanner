#include "odbc_filter_pushdown.hpp"
#include "odbc_utils.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

namespace duckdb {

bool ODBCFilterPushdown::TryCreateExpression(const string &column_name, vector<unique_ptr<TableFilter>> &filters,
                                               const string &op, string &out) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		string new_filter;
		if (!TryTransformFilter(column_name, *filter, new_filter)) {
			out.clear();
			return false;
		}
		if (new_filter.empty()) {
			continue;
		}
		filter_entries.push_back(std::move(new_filter));
	}
	if (filter_entries.empty()) {
		out.clear();
		return true;
	}
	out = "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
	return true;
}

string ODBCFilterPushdown::TransformComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw NotImplementedException("Unsupported expression type");
	}
}

bool ODBCFilterPushdown::TryTransformFilter(const string &column_name, TableFilter &filter, string &out) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		out = column_name + " IS NULL";
		return true;
	case TableFilterType::IS_NOT_NULL:
		out = column_name + " IS NOT NULL";
		return true;
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = filter.Cast<ConjunctionAndFilter>();
		return TryCreateExpression(column_name, conjunction_filter.child_filters, "AND", out);
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_filter = filter.Cast<ConjunctionOrFilter>();
		return TryCreateExpression(column_name, conjunction_filter.child_filters, "OR", out);
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		string constant_string;
		if (!ODBCUtils::TryTransformConstant(constant_filter.constant, constant_string)) {
			out.clear();
			return false;
		}
		auto operator_string = TransformComparison(constant_filter.comparison_type);
		out = StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
		return true;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (!optional_filter.child_filter) {
			out.clear();
			return true;
		}
		if (!TryTransformFilter(column_name, *optional_filter.child_filter, out)) {
			// Optional pushdown hints may be safely omitted locally without changing query semantics.
			out.clear();
		}
		return true;
	}
	case TableFilterType::DYNAMIC_FILTER: {
		out.clear();
		return true;
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		if (in_filter.values.empty()) {
			out.clear();
			return false;
		}
		vector<string> groups;
		for (idx_t offset = 0; offset < in_filter.values.size(); offset += 1000) {
			string in_list;
			auto group_end = MinValue<idx_t>(offset + 1000, in_filter.values.size());
			for (idx_t index = offset; index < group_end; index++) {
				string constant_string;
				if (!ODBCUtils::TryTransformConstant(in_filter.values[index], constant_string)) {
					out.clear();
					return false;
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				in_list += constant_string;
			}
			groups.push_back(column_name + " IN (" + in_list + ")");
		}
		out = groups.size() == 1 ? groups[0] : "(" + StringUtil::Join(groups, " OR ") + ")";
		return true;
	}
	default:
		out.clear();
		return false;
	}
}

ODBCFilterPushdownResult ODBCFilterPushdown::TransformFiltersWithFallback(const vector<column_t> &column_ids,
                                                                          optional_ptr<TableFilterSet> filters,
                                                                          const vector<string> &names) {
	ODBCFilterPushdownResult res;
	if (!filters || filters->filters.empty()) {
		return res;
	}
	for (auto &entry : filters->filters) {
		if (entry.first >= column_ids.size()) {
			res.unpushed_filter_columns.push_back(entry.first);
			continue;
		}
		auto col_id = column_ids[entry.first];
		if (col_id >= names.size()) {
			res.unpushed_filter_columns.push_back(entry.first);
			continue;
		}
		auto column_name = ODBCUtils::WriteIdentifier(names[col_id]);
		auto &filter = *entry.second;
		string new_filter;
		if (!TryTransformFilter(column_name, filter, new_filter)) {
			res.unpushed_filter_columns.push_back(entry.first);
			continue;
		}
		if (new_filter.empty()) {
			continue;
		}
		if (!res.where_clause.empty()) {
			res.where_clause += " AND ";
		}
		res.where_clause += new_filter;
	}
	return res;
}

string ODBCFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                            const vector<string> &names) {
	return TransformFiltersWithFallback(column_ids, filters, names).where_clause;
}

} // namespace duckdb
