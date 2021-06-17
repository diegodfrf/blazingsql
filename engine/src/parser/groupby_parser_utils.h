#pragma once

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <regex>

voltron::compute::AggregateKind get_aggregation_operation(std::string expression_in, bool is_window_operation = false);

/* Function used to name columns*/
std::string aggregator_to_string(voltron::compute::AggregateKind aggregation);

// TODO all these return types need to be revisited later. Right now we have issues with some aggregators that only
// support returning the same input type. Also pygdf does not currently support unsigned types (for example count should
// return and unsigned type)
std::shared_ptr<arrow::DataType> get_aggregation_output_type(std::shared_ptr<arrow::DataType> input_type, voltron::compute::AggregateKind aggregation, bool have_groupby);

std::vector<int> get_group_columns(std::string query_part);

std::tuple<std::vector<int>, std::vector<std::string>, std::vector<voltron::compute::AggregateKind>,std::vector<std::string>>
	parseGroupByExpression(const std::string & queryString, std::size_t num_cols);

std::tuple<std::vector<int>, std::vector<std::string>, std::vector<voltron::compute::AggregateKind>,	std::vector<std::string>>
	modGroupByParametersPostComputeAggregations(const std::vector<int> & group_column_indices,
		const std::vector<voltron::compute::AggregateKind> & aggregation_types, const std::vector<std::string> & merging_column_names);
