#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "groupby_parser_utils.h"
#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <regex>

#include <numeric>

voltron::compute::AggregateKind get_aggregation_operation(std::string expression_in, bool is_window_operation) {

	std::string operator_string = get_aggregation_operation_string(expression_in);
	std::string expression = get_string_between_outer_parentheses(expression_in);

	if (expression == "" && operator_string == "COUNT" && is_window_operation == false){
		return voltron::compute::AggregateKind::COUNT_ALL;
	} else if(operator_string == "SUM") {
		return voltron::compute::AggregateKind::SUM;
	} else if(operator_string == "$SUM0") {
		return voltron::compute::AggregateKind::SUM0;
	} else if(operator_string == "AVG") {
		return voltron::compute::AggregateKind::MEAN;
	} else if(operator_string == "MIN") {
		return voltron::compute::AggregateKind::MIN;
	} else if(operator_string == "MAX") {
		return voltron::compute::AggregateKind::MAX;
	} else if(operator_string == "ROW_NUMBER") {
		return voltron::compute::AggregateKind::ROW_NUMBER;
	} else if(operator_string == "COUNT") {
		return voltron::compute::AggregateKind::COUNT_VALID;
	} else if (operator_string == "LEAD") {
		return voltron::compute::AggregateKind::LEAD;
	} else if (operator_string == "LAG") {
		return voltron::compute::AggregateKind::LAG;
	} else if (operator_string == "FIRST_VALUE" || operator_string == "LAST_VALUE") {
		return voltron::compute::AggregateKind::NTH_ELEMENT;
	}else if(operator_string == "COUNT_DISTINCT") {
		/* Currently this conditional is unreachable.
		   Calcite transforms count distincts through the
		   AggregateExpandDistinctAggregates rule, so in fact,
		   each count distinct is replaced by some group by clauses. */
		return voltron::compute::AggregateKind::COUNT_DISTINCT;
	}

	throw std::runtime_error(
		"In get_aggregation_operation function: aggregation type not supported, " + operator_string);
}

std::string aggregator_to_string(voltron::compute::AggregateKind aggregation) {
	if(aggregation == voltron::compute::AggregateKind::COUNT_VALID || aggregation == voltron::compute::AggregateKind::COUNT_ALL) {
		return "count";
	} else if(aggregation == voltron::compute::AggregateKind::SUM) {
		return "sum";
	} else if(aggregation == voltron::compute::AggregateKind::SUM0) {
		return "sum0";
	} else if(aggregation == voltron::compute::AggregateKind::MIN) {
		return "min";
	} else if(aggregation == voltron::compute::AggregateKind::MAX) {
		return "max";
	} else if(aggregation == voltron::compute::AggregateKind::MEAN) {
		return "avg";
	} else if(aggregation == voltron::compute::AggregateKind::COUNT_DISTINCT) {
		/* Currently this conditional is unreachable.
		   Calcite transforms count distincts through the
		   AggregateExpandDistinctAggregates rule, so in fact,
		   each count distinct is replaced by some group by clauses. */
		return "count_distinct";
	} else {
		return "";  // FIXME: is really necessary?
	}
}

std::shared_ptr<arrow::DataType> get_aggregation_output_type(std::shared_ptr<arrow::DataType> input_type, voltron::compute::AggregateKind aggregation, bool have_groupby) {
	if(aggregation == voltron::compute::AggregateKind::COUNT_VALID || aggregation == voltron::compute::AggregateKind::COUNT_ALL) {
		return arrow::int64();
	} else if(aggregation == voltron::compute::AggregateKind::SUM || aggregation == voltron::compute::AggregateKind::SUM0) {
		if(have_groupby)
			return input_type;  // current group by function can only handle this
		else {
			// we can assume it is numeric based on the oepration
			// to be safe we should enlarge to the greatest integer or float representation
			return is_type_float(input_type) ? arrow::float64() : arrow::int64();
		}
	} else if(aggregation == voltron::compute::AggregateKind::MIN) {
		return input_type;
	} else if(aggregation == voltron::compute::AggregateKind::MAX) {
		return input_type;
	} else if(aggregation == voltron::compute::AggregateKind::MEAN) {
		return arrow::float64();
	} else if(aggregation == voltron::compute::AggregateKind::COUNT_DISTINCT) {
		/* Currently this conditional is unreachable.
		   Calcite transforms count distincts through the
		   AggregateExpandDistinctAggregates rule, so in fact,
		   each count distinct is replaced by some group by clauses. */
		return arrow::int64();
	} else {
		throw std::runtime_error(
			"In get_aggregation_output_type function: aggregation type not supported.");
	}
}

std::vector<int> get_group_columns(std::string query_part) {
	std::string temp_column_string = get_named_expression(query_part, "group");
	if(temp_column_string.size() <= 2) {
		return std::vector<int>();
	}

	// Now we have somethig like {0, 1}
	temp_column_string = temp_column_string.substr(1, temp_column_string.length() - 2);
	std::vector<std::string> column_numbers_string = StringUtil::split(temp_column_string, ",");
	std::vector<int> group_column_indices(column_numbers_string.size());
	for(size_t i = 0; i < column_numbers_string.size(); i++) {
		group_column_indices[i] = std::stoull(column_numbers_string[i], 0);
	}
	return group_column_indices;
}


std::tuple<std::vector<int>, std::vector<std::string>, std::vector<voltron::compute::AggregateKind>,std::vector<std::string>>
	parseGroupByExpression(const std::string & queryString, std::size_t num_cols){
	std::vector<voltron::compute::AggregateKind> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	std::vector<int> group_column_indices;

	// Get aggregations
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;

	auto rangeStart = queryString.find("(");
	auto rangeEnd = queryString.rfind(")") - rangeStart;
	std::string combined_expression = queryString.substr(rangeStart + 1, rangeEnd - 1);

	// in case UNION exists,
	if (combined_expression == "group=[{*}]") {
		StringUtil::findAndReplaceAll(combined_expression, "*", StringUtil::makeCommaDelimitedSequence(num_cols));
	}

	group_column_indices = get_group_columns(combined_expression);
	std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);
	for(std::string expr : expressions) {
		std::string expression = std::regex_replace(expr, std::regex("^ +| +$|( ) +"), "$1");
		if(expression.find("group=") == std::string::npos) {
			aggregation_expressions.push_back(expression);

			// if the aggregation has an alias, lets capture it here, otherwise we'll figure out what to call the
			// aggregation based on its input
			if(expression.find("EXPR$") == 0)
				aggregation_column_assigned_aliases.push_back("");
			else
				aggregation_column_assigned_aliases.push_back(expression.substr(0, expression.find("=[")));
		}
	}

	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation(expression));
		aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));
	}
	return std::make_tuple(std::move(group_column_indices), std::move(aggregation_input_expressions),
		std::move(aggregation_types), std::move(aggregation_column_assigned_aliases));
}

std::tuple<std::vector<int>, std::vector<std::string>, std::vector<voltron::compute::AggregateKind>,	std::vector<std::string>>
	modGroupByParametersPostComputeAggregations(const std::vector<int> & group_column_indices,
		const std::vector<voltron::compute::AggregateKind> & aggregation_types, const std::vector<std::string> & merging_column_names) {

	std::vector<voltron::compute::AggregateKind> mod_aggregation_types = aggregation_types;
	std::vector<std::string> mod_aggregation_input_expressions(aggregation_types.size());
	std::vector<std::string> mod_aggregation_column_assigned_aliases(mod_aggregation_types.size());
	std::vector<int> mod_group_column_indices(group_column_indices.size());
	std::iota(mod_group_column_indices.begin(), mod_group_column_indices.end(), 0);
	for (size_t i = 0; i < mod_aggregation_types.size(); i++){
		if (mod_aggregation_types[i] == voltron::compute::AggregateKind::COUNT_ALL || mod_aggregation_types[i] == voltron::compute::AggregateKind::COUNT_VALID){
			mod_aggregation_types[i] = voltron::compute::AggregateKind::SUM; // if we have a COUNT, we want to SUM the output of the counts from other nodes
		}
		mod_aggregation_input_expressions[i] = std::to_string(i + mod_group_column_indices.size()); // we just want to aggregate the input columns, so we are setting the indices here
		mod_aggregation_column_assigned_aliases[i] = merging_column_names[i + mod_group_column_indices.size()];
	}
	return std::make_tuple(std::move(mod_group_column_indices), std::move(mod_aggregation_input_expressions),
		std::move(mod_aggregation_types), std::move(mod_aggregation_column_assigned_aliases));
}
