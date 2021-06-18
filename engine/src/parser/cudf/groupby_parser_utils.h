#pragma once

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <regex>

#include <cudf/aggregation.hpp>
#include <cudf/sorting.hpp>
#include <cudf/replace.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/reduction.hpp>
#include "operators/operators_definitions.h"
#include "check_types.h"

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


cudf::type_id get_aggregation_output_type(cudf::type_id input_type, voltron::compute::AggregateKind aggregation, bool have_groupby) {
  if(aggregation == voltron::compute::AggregateKind::COUNT_VALID || aggregation == voltron::compute::AggregateKind::COUNT_ALL) {
    return cudf::type_id::INT64;
  } else if(aggregation == voltron::compute::AggregateKind::SUM || aggregation == voltron::compute::AggregateKind::SUM0) {
    if(have_groupby)
      return input_type;  // current group by function can only handle this
    else {
      // we can assume it is numeric based on the oepration
      // to be safe we should enlarge to the greatest integer or float representation
      return is_type_float(input_type) ? cudf::type_id::FLOAT64 : cudf::type_id::INT64;
    }
  } else if(aggregation == voltron::compute::AggregateKind::MIN) {
    return input_type;
  } else if(aggregation == voltron::compute::AggregateKind::MAX) {
    return input_type;
  } else if(aggregation == voltron::compute::AggregateKind::MEAN) {
    return cudf::type_id::FLOAT64;
  } else if(aggregation == voltron::compute::AggregateKind::COUNT_DISTINCT) {
    /* Currently this conditional is unreachable.
       Calcite transforms count distincts through the
       AggregateExpandDistinctAggregates rule, so in fact,
       each count distinct is replaced by some group by clauses. */
    return cudf::type_id::INT64;
  } else {
    throw std::runtime_error(
        "In get_aggregation_output_type function: aggregation type not supported.");
  }
}

