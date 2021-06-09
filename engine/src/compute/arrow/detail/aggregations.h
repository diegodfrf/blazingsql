#pragma once

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "blazing_table/BlazingArrowTable.h"
#include <cudf/scalar/scalar_factories.hpp>

#include <arrow/compute/api.h>

std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
 	std::shared_ptr<arrow::Table> table, const std::vector<int> & group_column_indices);


std::shared_ptr<arrow::Scalar> arrow_reduce(std::shared_ptr<arrow::ChunkedArray> col,
                                std::unique_ptr<cudf::aggregation> const &agg,
                                cudf::data_type output_dtype);

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
 		std::shared_ptr<ral::frame::BlazingArrowTableView> table_view, const std::vector<std::string> & aggregation_input_expressions,
 		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases);
