#include "compute/cudf/detail/aggregations.h"

#include "compute/cudf/detail/interops.h"

#include "parser/expression_utils.hpp"
#include "blazing_table/BlazingCudfTable.h"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include "distribution_utils/primitives.h"
#include "utilities/CommonOperations.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <regex>

#include <cudf/aggregation.hpp>
#include <cudf/sorting.hpp>
#include <cudf/replace.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/reduction.hpp>
#include <cudf/groupby.hpp>
#include <cudf/detail/aggregation/aggregation.hpp>

inline std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
	std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<int> & group_column_indices) {
  auto ctable_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
	std::unique_ptr<cudf::table> output = cudf::drop_duplicates(ctable_view->view(),
		group_column_indices,
		cudf::duplicate_keep_option::KEEP_FIRST);

	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(output), table_view->column_names());
}

inline std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
		std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, const std::vector<std::string> & aggregation_input_expressions,
		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases)
{
	std::vector<std::unique_ptr<cudf::scalar>> reductions;
	std::vector<std::string> agg_output_column_names;
	for (size_t i = 0; i < aggregation_types.size(); i++){
		if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
			std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
			auto numeric_s = static_cast< cudf::scalar_type_t<int64_t>* >(scalar.get());
			numeric_s->set_value((int64_t)(table_view->num_rows()));
			reductions.emplace_back(std::move(scalar));
		} else {
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> aggregation_input_scope_holder;
			cudf::column_view aggregation_input;
			if(is_var_column(aggregation_input_expressions[i]) || is_number(aggregation_input_expressions[i])) {
				aggregation_input = table_view->view().column(get_index(aggregation_input_expressions[i]));
			} else {
				aggregation_input_scope_holder = evaluate_expressions(table_view->view(), {aggregation_input_expressions[i]});
				aggregation_input = aggregation_input_scope_holder[0]->view();
			}

			if( aggregation_types[i] == AggregateKind::COUNT_VALID) {
				std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
				auto numeric_s = static_cast< cudf::scalar_type_t<int64_t>* >(scalar.get());
				numeric_s->set_value((int64_t)(aggregation_input.size() - aggregation_input.null_count()));
				reductions.emplace_back(std::move(scalar));
			} else {
				std::unique_ptr<cudf::aggregation> agg = makeCudfAggregation<cudf::aggregation>(aggregation_types[i]);
				cudf::type_id output_type = get_aggregation_output_type(aggregation_input.type().id(), aggregation_types[i], false);
				std::unique_ptr<cudf::scalar> reduction_out = cudf::reduce(aggregation_input, agg, cudf::data_type(output_type));
				if (aggregation_types[i] == AggregateKind::SUM0 && !reduction_out->is_valid()){ // if this aggregation was a SUM0, and it was not valid, we want it to be a valid 0 instead
					std::unique_ptr<cudf::scalar> zero_scalar = get_scalar_from_string("0", reduction_out->type()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
					reductions.emplace_back(std::move(zero_scalar));
				} else {
					reductions.emplace_back(std::move(reduction_out));
				}
			}
		}

		// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
		if(aggregation_column_assigned_aliases[i] == "") {
			if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(*)");
			} else {
				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table_view->column_names().at(get_index(aggregation_input_expressions[i])) + ")");
			}
		} else {
			agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
		}
	}
	// convert scalars into columns
	std::vector<std::unique_ptr<cudf::column>> output_columns;
	for (size_t i = 0; i < reductions.size(); i++){
		std::unique_ptr<cudf::column> temp = cudf::make_column_from_scalar(*(reductions[i]), 1);
		output_columns.emplace_back(std::move(temp));
	}
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(std::make_unique<cudf::table>(std::move(output_columns))), agg_output_column_names);
}


inline std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, const std::vector<std::string> & aggregation_input_expressions, const std::vector<AggregateKind> & aggregation_types,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices) {

	// lets get the unique expressions. This is how many aggregation requests we will need
	std::vector<std::string> unique_expressions = aggregation_input_expressions;
	std::sort( unique_expressions.begin(), unique_expressions.end() );
	auto it = std::unique( unique_expressions.begin(), unique_expressions.end() );
	unique_expressions.resize( std::distance(unique_expressions.begin(),it) );

	// We will iterate over the unique expressions and create an aggregation request for each one.
	// We do it this way, because you could have something like min(colA), max(colA), sum(colA).
	// These three aggregations would all be in one request because they have the same input
	std::vector< std::unique_ptr<ral::frame::BlazingColumn> > aggregation_inputs_scope_holder;
	std::vector<cudf::groupby::aggregation_request> requests;
	std::vector<int> agg_out_indices;
	std::vector<std::string> agg_output_column_names;
	for (size_t u = 0; u < unique_expressions.size(); u++){
		std::string expression = unique_expressions[u];

		cudf::column_view aggregation_input; // this is the input from which we will crete the aggregation request
		bool got_aggregation_input = false;
		std::vector<std::unique_ptr<cudf::aggregation>> agg_ops_for_request;
		for (size_t i = 0; i < aggregation_input_expressions.size(); i++){
			if (expression == aggregation_input_expressions[i]){

				int column_index = -1;
				// need to calculate or determine the aggregation input only once
				if (!got_aggregation_input) {
					if(expression == "" && aggregation_types[i] == AggregateKind::COUNT_ALL ) { // this is COUNT(*). Lets just pick the first column
						aggregation_input = table_view->view().column(0);
					} else if(is_var_column(expression) || is_number(expression)) {
						column_index = get_index(expression);
						aggregation_input = table_view->view().column(column_index);
					} else {
						std::vector< std::unique_ptr<ral::frame::BlazingColumn> > computed_columns = evaluate_expressions(table_view->view(), {expression});
						aggregation_inputs_scope_holder.insert(aggregation_inputs_scope_holder.end(), std::make_move_iterator(computed_columns.begin()), std::make_move_iterator(computed_columns.end()));
						aggregation_input = aggregation_inputs_scope_holder.back()->view();
					}
					got_aggregation_input = true;
				}
				agg_ops_for_request.push_back(makeCudfAggregation<cudf::aggregation>(aggregation_types[i]));
				agg_out_indices.push_back(i);  // this is to know what is the desired order of aggregations output

				// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
				if(aggregation_column_assigned_aliases[i] == "") {
					if(aggregation_types[i] == AggregateKind::COUNT_ALL) {  // COUNT(*) case
						agg_output_column_names.push_back("COUNT(*)");
					} else {
						if (column_index == -1){
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + expression + ")");
						} else {
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table_view->column_names().at(column_index) + ")");
						}
					}
				} else {
					agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
				}
			}
		}
			requests.push_back(cudf::groupby::aggregation_request {.values = aggregation_input, .aggregations = std::move(agg_ops_for_request)});
	}

	cudf::table_view keys = table_view->view().select(group_column_indices);
	cudf::groupby::groupby group_by_obj(keys, cudf::null_policy::INCLUDE);
	std::pair<std::unique_ptr<cudf::table>, std::vector<cudf::groupby::aggregation_result>> result = group_by_obj.aggregate( requests );

	// output table is grouped columns and then aggregated columns
	std::vector< std::unique_ptr<cudf::column> > output_columns = result.first->release();
	output_columns.resize(agg_out_indices.size() + group_column_indices.size());

	// lets collect all the aggregated results from the results structure and then add them to output_columns
	std::vector< std::unique_ptr<cudf::column> > agg_cols_out;
	for (size_t i = 0; i < result.second.size(); i++){
		for (size_t j = 0; j < result.second[i].results.size(); j++){
			agg_cols_out.emplace_back(std::move(result.second[i].results[j]));
		}
	}
	for (size_t i = 0; i < agg_out_indices.size(); i++){
		if (aggregation_types[agg_out_indices[i]] == AggregateKind::SUM0 && agg_cols_out[i]->null_count() > 0){
			std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string("0", agg_cols_out[i]->type()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
			std::unique_ptr<cudf::column> temp = cudf::replace_nulls(agg_cols_out[i]->view(), *scalar );
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(temp);
		} else {
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(agg_cols_out[i]);
		}
	}
	std::unique_ptr<cudf::table> output_table = std::make_unique<cudf::table>(std::move(output_columns));

	// lets put together the output names
	std::vector<std::string> output_names;
	for (size_t i = 0; i < group_column_indices.size(); i++){
		output_names.push_back(table_view->column_names()[group_column_indices[i]]);
	}
	output_names.resize(agg_out_indices.size() + group_column_indices.size());
	for (size_t i = 0; i < agg_out_indices.size(); i++){
		output_names[agg_out_indices[i] + group_column_indices.size()] = agg_output_column_names[i];
	}
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(output_table), output_names);
}

