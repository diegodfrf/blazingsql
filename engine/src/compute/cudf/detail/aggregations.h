#pragma once

#include "blazing_table/BlazingTable.h"
#include <cudf/aggregation.hpp>
#include "Util/StringUtil.h"
#include <regex>
#include <numeric>
#include "parser/groupby_parser_utils.h"

std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
	std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<int> & group_column_indices);

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
		std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, const std::vector<std::string> & aggregation_input_expressions,
		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases);

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, const std::vector<std::string> & aggregation_input_expressions, const std::vector<AggregateKind> & aggregation_types,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices);
