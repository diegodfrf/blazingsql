#include "project_parser_utils.h"

#include <numeric>

#include "parser/expression_utils.hpp"

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

#ifdef CUDF_SUPPORT
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
#endif

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

// Use get_projections and if there are no projections or expression is empty
// then returns a filled array with the sequence of all columns (0, 1, ..., n)
std::vector<int> get_projections_wrapper(size_t num_columns, const std::string &expression)
{
  if (expression.empty()) {
    std::vector<int> projections(num_columns);
    std::iota(projections.begin(), projections.end(), 0);
    return projections;
  }

  std::vector<int> projections = get_projections(expression);
  if(projections.size() == 0){
      projections.resize(num_columns);
      std::iota(projections.begin(), projections.end(), 0);
  }
  return projections;
}

