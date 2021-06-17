#include "orderby_parser_utils.h"

#include "parser/CalciteExpressionParsing.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include "parser/expression_utils.hpp"
#include <blazingdb/io/Util/StringUtil.h>

std::tuple< std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder>, int>
get_sort_vars(const std::string & query_part) {
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd);

	int num_sort_columns = count_string_occurrence(combined_expression, "sort");

	std::vector<int> sortColIndices(num_sort_columns);
	std::vector<voltron::compute::SortOrder> sortOrderTypes(num_sort_columns);
	std::vector<voltron::compute::NullOrder> sortOrderNulls(num_sort_columns);
	for(auto i = 0; i < num_sort_columns; i++) {
		sortColIndices[i] = get_index(get_named_expression(combined_expression, "sort" + std::to_string(i)));
		std::string sort_type = get_named_expression(combined_expression, "dir" + std::to_string(i));

		// defaults
		sortOrderTypes[i] = voltron::compute::SortOrder::ASCENDING;
		sortOrderNulls[i] = voltron::compute::NullOrder::AFTER;

		if (sort_type == ASCENDING_ORDER_SORT_TEXT_NULLS_FIRST) {
			sortOrderNulls[i] = voltron::compute::NullOrder::BEFORE;
		} else if (sort_type == DESCENDING_ORDER_SORT_TEXT_NULLS_LAST) {
			sortOrderTypes[i] = voltron::compute::SortOrder::DESCENDING;
			sortOrderNulls[i] = voltron::compute::NullOrder::BEFORE; // due to the descending
		} else if (sort_type == DESCENDING_ORDER_SORT_TEXT) {
			sortOrderTypes[i] = voltron::compute::SortOrder::DESCENDING;
		}
	}

	std::string limitRowsStr = get_named_expression(combined_expression, "fetch");
	int limitRows = !limitRowsStr.empty() ? std::stoi(limitRowsStr) : -1;

	return std::make_tuple(sortColIndices, sortOrderTypes, sortOrderNulls, limitRows);
}


// input: min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $3)], n_nationkey=[$0]
// output: < [1, 2], [cudf::ASCENDING, cudf::ASCENDING], [voltron::compute::NullOrder::AFTER, voltron::compute::NullOrder::AFTER] >
std::tuple< std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_vars_to_partition(const std::string & logical_plan) {
	std::vector<int> column_index;
	std::vector<voltron::compute::SortOrder> order_types;
	std::vector<voltron::compute::NullOrder> null_orders;
	const std::string partition_expr = "PARTITION BY ";

	// PARTITION BY $1, $2 ORDER BY $3
	std::string over_expression = get_first_over_expression_from_logical_plan(logical_plan, partition_expr);

	if (over_expression.size() == 0) {
		return std::make_tuple(column_index, order_types, null_orders);
	}

	size_t start_position = over_expression.find(partition_expr) + partition_expr.size();
	size_t end_position = over_expression.find("ORDER BY ");

	if (end_position == get_query_part(logical_plan).npos) {
		end_position = over_expression.size() + 1;
	}
	// $1, $2
	std::string values = over_expression.substr(start_position, end_position - start_position - 1);
	std::vector<std::string> column_numbers_string = StringUtil::split(values, ", ");
	for (size_t i = 0; i < column_numbers_string.size(); i++) {
		column_numbers_string[i] = StringUtil::replace(column_numbers_string[i], "$", "");
		column_index.push_back(std::stoi(column_numbers_string[i]));
		// by default
		order_types.push_back(voltron::compute::SortOrder::ASCENDING);
		null_orders.push_back(voltron::compute::NullOrder::AFTER);
	}

	return std::make_tuple(column_index, order_types, null_orders);
}


// input: min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $3, $1 DESC)], n_nationkey=[$0]
// output: < [3, 1], [cudf::ASCENDING, cudf::DESCENDING], [voltron::compute::NullOrder::AFTER, voltron::compute::NullOrder::AFTER] >
std::tuple< std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_vars_to_orders(const std::string & logical_plan) {
	std::vector<int> column_index;
	std::vector<voltron::compute::SortOrder> order_types;
	std::vector<voltron::compute::NullOrder> sorted_order_nulls;
	std::string order_expr = "ORDER BY ";

	// PARTITION BY $2 ORDER BY $3, $1 DESC
	std::string over_expression = get_first_over_expression_from_logical_plan(logical_plan, order_expr);

	if (over_expression.size() == 0) {
		return std::make_tuple(column_index, order_types, sorted_order_nulls);
	}

	size_t start_position = over_expression.find(order_expr) + order_expr.size();
	size_t end_position = over_expression.find("ROWS");
	if (end_position != over_expression.npos) {
		end_position = end_position - 1;
	} else {
		end_position = over_expression.size();
	}

	// $3, $1 DESC
	std::string values = over_expression.substr(start_position, end_position - start_position);
	std::vector<std::string> column_express = StringUtil::split(values, ", ");
	for (std::size_t i = 0; i < column_express.size(); ++i) {
		std::vector<std::string> split_parts = StringUtil::split(column_express[i], " ");
		if (split_parts.size() == 1) { // $x
			order_types.push_back(voltron::compute::SortOrder::ASCENDING);
			sorted_order_nulls.push_back(voltron::compute::NullOrder::AFTER);
		} else if (split_parts.size() == 2) { // $x DESC
			order_types.push_back(voltron::compute::SortOrder::DESCENDING);
			sorted_order_nulls.push_back(voltron::compute::NullOrder::AFTER);
		} else if (split_parts.size() == 3) {
			order_types.push_back(voltron::compute::SortOrder::ASCENDING);
			if (split_parts[2] == "FIRST") {  // $x NULLS FIRST  
				sorted_order_nulls.push_back(voltron::compute::NullOrder::BEFORE);
			} else { // $x NULLS LAST
				sorted_order_nulls.push_back(voltron::compute::NullOrder::AFTER);
			}
		}
		else {
			order_types.push_back(voltron::compute::SortOrder::DESCENDING);
			if (split_parts[3] == "FIRST") { // $x DESC NULLS FIRST
				sorted_order_nulls.push_back(voltron::compute::NullOrder::AFTER);
			} else { // $x DESC NULLS LAST
				sorted_order_nulls.push_back(voltron::compute::NullOrder::BEFORE);
			}
		}

		//split_parts[0] = StringUtil::replace(split_parts[0], "$", "");
		//column_index.push_back(std::stoi(split_parts[0]));
		column_index.push_back( get_index_from_expression_str(split_parts[0]) );
	}

	return std::make_tuple(column_index, order_types, sorted_order_nulls);
}


// input: min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $3 DESC)], n_nationkey=[$0]
// output: < [1, 2, 3], [cudf::ASCENDING, cudf::ASCENDING, cudf::DESCENDING], [voltron::compute::NullOrder::AFTER, voltron::compute::NullOrder::AFTER]>
std::tuple< std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_vars_to_partition_and_order(const std::string & query_part) {
	std::vector<int> column_index_partition, column_index_order;
	std::vector<voltron::compute::SortOrder> order_types_partition, order_types_order;
	std::vector<voltron::compute::NullOrder> order_by_null_part, order_by_null;

	std::tie(column_index_partition, order_types_partition, order_by_null_part) = get_vars_to_partition(query_part);
	std::tie(column_index_order, order_types_order, order_by_null) = get_vars_to_orders(query_part);

	column_index_partition.insert(column_index_partition.end(), column_index_order.begin(), column_index_order.end());
	order_types_partition.insert(order_types_partition.end(), order_types_order.begin(), order_types_order.end());
	order_by_null_part.insert(order_by_null_part.end(), order_by_null.begin(), order_by_null.end());

	return std::make_tuple(column_index_partition, order_types_partition, order_by_null_part);
}


std::tuple<std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_right_sorts_vars(const std::string & query_part) {
	std::vector<int> sortColIndices;
	std::vector<voltron::compute::SortOrder> sortOrderTypes;
	std::vector<voltron::compute::NullOrder> sortOrderNulls;
	int limitRows;

	if (is_window_function(query_part)) {
		// `order by` and `partition by`
		if (window_expression_contains_order_by(query_part) && window_expression_contains_partition_by(query_part)) {
			std::tie(sortColIndices, sortOrderTypes, sortOrderNulls) = get_vars_to_partition_and_order(query_part);
		}
		// only `partition by`
		else if (!window_expression_contains_order_by(query_part)) {
			std::tie(sortColIndices, sortOrderTypes, sortOrderNulls) = get_vars_to_partition(query_part);
		}
		// without `partition by`
		else {
			std::tie(sortColIndices, sortOrderTypes, sortOrderNulls) = get_vars_to_orders(query_part);
		}
	} else std::tie(sortColIndices, sortOrderTypes, sortOrderNulls, limitRows) = get_sort_vars(query_part);

	return std::make_tuple(sortColIndices, sortOrderTypes, sortOrderNulls);
}

bool has_limit_only(const std::string & query_part){
	std::vector<int> sortColIndices;
	std::tie(sortColIndices, std::ignore, std::ignore, std::ignore) = get_sort_vars(query_part);

	return sortColIndices.empty();
}

int64_t get_limit_rows_when_relational_alg_is_simple(const std::string & query_part){
	int64_t limitRows;
	std::tie(std::ignore, std::ignore, std::ignore, limitRows) = get_sort_vars(query_part);
	return limitRows;
}
