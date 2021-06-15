#pragma once

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <regex>

voltron::compute::AggregateKind get_aggregation_operation(std::string expression_in, bool is_window_operation = false);

/* Function used to name columns*/
std::string aggregator_to_string(voltron::compute::AggregateKind aggregation);

// offset param is needed for `LAG` and `LEAD` aggs
template<typename cudf_aggregation_type_T>
std::unique_ptr<cudf_aggregation_type_T> makeCudfAggregation(voltron::compute::AggregateKind input, int offset = 0){
  if(input == voltron::compute::AggregateKind::SUM){
    return cudf::make_sum_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::MEAN){
    return cudf::make_mean_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::MIN){
    return cudf::make_min_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::MAX){
    return cudf::make_max_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::ROW_NUMBER) {
    return cudf::make_row_number_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::COUNT_VALID){
    return cudf::make_count_aggregation<cudf_aggregation_type_T>(cudf::null_policy::EXCLUDE);
  }else if(input == voltron::compute::AggregateKind::COUNT_ALL){
    return cudf::make_count_aggregation<cudf_aggregation_type_T>(cudf::null_policy::INCLUDE);
  }else if(input == voltron::compute::AggregateKind::SUM0){
    return cudf::make_sum_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::LAG){
    return cudf::make_lag_aggregation<cudf_aggregation_type_T>(offset);
  }else if(input == voltron::compute::AggregateKind::LEAD){
    return cudf::make_lead_aggregation<cudf_aggregation_type_T>(offset);
  }else if(input == voltron::compute::AggregateKind::NTH_ELEMENT){
    // TODO: https://github.com/BlazingDB/blazingsql/issues/1531
    // return cudf::make_nth_element_aggregation<cudf_aggregation_type_T>(offset, cudf::null_policy::INCLUDE);	
  }else if(input == voltron::compute::AggregateKind::COUNT_DISTINCT){
    /* Currently this conditional is unreachable.
    Calcite transforms count distincts through the
    AggregateExpandDistinctAggregates rule, so in fact,
    each count distinct is replaced by some group by clauses. */
    // return cudf::make_nunique_aggregation<cudf_aggregation_type_T>();
  }
  throw std::runtime_error(
    "In makeCudfAggregation function: AggregateKind type not supported");
}

// TODO all these return types need to be revisited later. Right now we have issues with some aggregators that only
// support returning the same input type. Also pygdf does not currently support unsigned types (for example count should
// return and unsigned type)
cudf::type_id get_aggregation_output_type(cudf::type_id input_type, voltron::compute::AggregateKind aggregation, bool have_groupby);

std::vector<int> get_group_columns(std::string query_part);

std::tuple<std::vector<int>, std::vector<std::string>, std::vector<voltron::compute::AggregateKind>,std::vector<std::string>>
	parseGroupByExpression(const std::string & queryString, std::size_t num_cols);

std::tuple<std::vector<int>, std::vector<std::string>, std::vector<voltron::compute::AggregateKind>,	std::vector<std::string>>
	modGroupByParametersPostComputeAggregations(const std::vector<int> & group_column_indices,
		const std::vector<voltron::compute::AggregateKind> & aggregation_types, const std::vector<std::string> & merging_column_names);
