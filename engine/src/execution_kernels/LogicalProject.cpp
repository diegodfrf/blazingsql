#include <spdlog/spdlog.h>
#include <cudf/copying.hpp>
#include <cudf/replace.hpp>
#include <cudf/strings/capitalize.hpp>
#include <cudf/strings/combine.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/replace_re.hpp>
#include <cudf/strings/replace.hpp>
#include <cudf/strings/substring.hpp>
#include <cudf/strings/case.hpp>
#include <cudf/strings/strip.hpp>
#include <cudf/strings/convert/convert_booleans.hpp>
#include <cudf/strings/convert/convert_datetime.hpp>
#include <cudf/strings/convert/convert_floats.hpp>
#include <cudf/strings/convert/convert_integers.hpp>
#include <cudf/unary.hpp>
#include "blazing_table/BlazingColumnOwner.h"
#include "LogicalProject.h"
#include "utilities/transform.hpp"
#include "Interpreter/interpreter_cpp.h"
#include "parser/expression_utils.hpp"
#include "compute/api.h"

namespace ral {
namespace processor {

// forward declaration
std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(const cudf::table_view & table, const std::vector<std::string> & expressions);

namespace strings {







} // namespace strings







std::string like_expression_to_regex_str(const std::string & like_exp) {
	if(like_exp.empty()) {
		return like_exp;
	}

	bool match_start = like_exp[0] != '%';
	bool match_end = like_exp[like_exp.size() - 1] != '%';

	std::string re = like_exp;
	static const std::regex any_string_re{R"(([^\\]?|\\{2})%)"};
	re = std::regex_replace(re, any_string_re, "$1(?:.*?)");

	static const std::regex any_char_re{R"(([^\\]?|\\{2})_)"};
	re = std::regex_replace(re, any_char_re, "$1(?:.)");

	return (match_start ? "^" : "") + re + (match_end ? "$" : "");
}

cudf::strings::strip_type map_trim_flag_to_strip_type(const std::string & trim_flag)
{
    if (trim_flag == "BOTH")
        return cudf::strings::strip_type::BOTH;
    else if (trim_flag == "LEADING")
        return cudf::strings::strip_type::LEFT;
    else if (trim_flag == "TRAILING")
        return cudf::strings::strip_type::RIGHT;
    else
        // Should not reach here
        assert(false);
}

std::string get_current_date_or_timestamp(std::string expression, blazingdb::manager::Context * context) {
    // We want `CURRENT_TIME` holds the same value as `CURRENT_TIMESTAMP`
	if (expression.find("CURRENT_TIME") != expression.npos) {
		expression = StringUtil::replace(expression, "CURRENT_TIME", "CURRENT_TIMESTAMP");
	}

	std::size_t date_pos = expression.find("CURRENT_DATE");
	std::size_t timestamp_pos = expression.find("CURRENT_TIMESTAMP");

	if (date_pos == expression.npos && timestamp_pos == expression.npos) {
		return expression;
	}

    // CURRENT_TIMESTAMP will return a `ms` format
	std::string	timestamp_str = context->getCurrentTimestamp().substr(0, 23);
    std::string str_to_replace = "CURRENT_TIMESTAMP";

	// In case CURRENT_DATE we want only the date value
	if (date_pos != expression.npos) {
		str_to_replace = "CURRENT_DATE";
        timestamp_str = timestamp_str.substr(0, 10);
	}

	return StringUtil::replace(expression, str_to_replace, timestamp_str);
}


expr_output_type_visitor::expr_output_type_visitor(const cudf::table_view & table) : table_{table} { }

void expr_output_type_visitor::visit(const ral::parser::operad_node& node)  {
  cudf::data_type output_type;
  if (is_literal(node.value)) {
    output_type = static_cast<const ral::parser::literal_node&>(node).type();
  } else {
          cudf::size_type idx = static_cast<const ral::parser::variable_node&>(node).index();
    output_type = table_.column(idx).type();

          // Also store the variable idx for later use
          variable_indices_.push_back(idx);
  }

  node_to_type_map_.insert({&node, output_type});
  expr_output_type_ = output_type;
}

void expr_output_type_visitor::visit(const ral::parser::operator_node& node)  {
  cudf::data_type output_type;
  operator_type op = map_to_operator_type(node.value);
  if(is_binary_operator(op)) {
    output_type = cudf::data_type{get_output_type(op, node_to_type_map_.at(node.children[0].get()).id(), node_to_type_map_.at(node.children[1].get()).id())};
  } else if (is_unary_operator(op)) {
    output_type = cudf::data_type{get_output_type(op, node_to_type_map_.at(node.children[0].get()).id())};
  }else{
          output_type = cudf::data_type{get_output_type(op)};
      }

  node_to_type_map_.insert({&node, output_type});
  expr_output_type_ = output_type;
}

cudf::data_type expr_output_type_visitor::get_expr_output_type() { return expr_output_type_; }

const std::vector<cudf::size_type> & expr_output_type_visitor::get_variable_indices() { return variable_indices_; }






std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::Context * context) {
    std::string combined_expression = get_query_part(query_part);

    std::vector<std::string> named_expressions = get_expressions_from_expression_list(combined_expression);
    std::vector<std::string> expressions(named_expressions.size());
    std::vector<std::string> out_column_names(named_expressions.size());
    for(size_t i = 0; i < named_expressions.size(); i++) {
        const std::string & named_expr = named_expressions[i];

        std::string name = named_expr.substr(0, named_expr.find("=["));
        std::string expression = named_expr.substr(named_expr.find("=[") + 2 , (named_expr.size() - named_expr.find("=[")) - 3);
        expression = fill_minus_op_with_zero(expression);
        expression = convert_concat_expression_into_multiple_binary_concat_ops(expression);
        expression = get_current_date_or_timestamp(expression, context);
        expression = convert_ms_to_ns_units(expression);
        expression = reinterpret_timestamp(expression, blazing_table_in->column_types());
        expression = apply_interval_conversion(expression, blazing_table_in->column_types());

        expressions[i] = expression;
        out_column_names[i] = name;
    }

    
    // TODO percy arrow delete these comments ltr
    // cudf functor
//    auto blazing_table_in_cudf = dynamic_cast<ral::frame::BlazingCudfTable*>(blazing_table_in.get());
//    std::unique_ptr<ral::frame::BlazingTable> evaluated_table = ral::execution::backend_dispatcher(
//      blazing_table_in->get_execution_backend(),
//      evaluate_expressions_wo_filter_functor(),
//      blazing_table_in_cudf->view(), expressions, out_column_names);
    
//    //auto evaluated_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(evaluated_table.get());
//    //return std::make_unique<ral::frame::BlazingCudfTable>(evaluated_table_ptr->view(), out_column_names);
//    return evaluated_table;
    
//    return ral::execution::backend_dispatcher(
//        blazing_table_in->get_execution_backend(),
//        evaluate_expressions_functor(),
//        blazing_table_in->to_table_view(), expressions);

        return ral::execution::backend_dispatcher(
          blazing_table_in->get_execution_backend(),
          evaluate_expressions_wo_filter_functor(),
          blazing_table_in->to_table_view(), expressions, out_column_names);
    
// original    
//    return ral::execution::backend_dispatcher(
//      blazing_table_in->get_execution_backend(),
//      process_project_functor(),
//      std::move(blazing_table_in), expressions, out_column_names);
}

} // namespace processor
} // namespace ral
