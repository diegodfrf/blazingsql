#include "project_parser_utils.h"

#include <numeric>

#include "parser/expression_utils.hpp"
#include "parser/cudf/types_parser_utils.h"

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
  std::shared_ptr<arrow::DataType> output_type;
  if (is_literal(node.value)) {
    output_type = static_cast<const ral::parser::literal_node&>(node).type();
  } else {
      cudf::size_type idx = static_cast<const ral::parser::variable_node&>(node).index();
      output_type = cudf_type_id_to_arrow_type(table_.column(idx).type().id());

          // Also store the variable idx for later use
          variable_indices_.push_back(idx);
  }

  node_to_type_map_.insert({&node, output_type});
  expr_output_type_ = output_type;
}

void expr_output_type_visitor::visit(const ral::parser::operator_node& node)  {
  std::shared_ptr<arrow::DataType> output_type;
  operator_type op = map_to_operator_type(node.value);
  if(is_binary_operator(op)) {
    output_type = get_output_type(op, node_to_type_map_.at(node.children[0].get()), node_to_type_map_.at(node.children[1].get()));
  } else if (is_unary_operator(op)) {
    output_type = get_output_type(op, node_to_type_map_.at(node.children[0].get()));
  }else{
    output_type = get_output_type(op);
  }

  node_to_type_map_.insert({&node, output_type});
  expr_output_type_ = output_type;
}

std::shared_ptr<arrow::DataType> expr_output_type_visitor::get_expr_output_type() { return expr_output_type_; }

const std::vector<cudf::size_type> & expr_output_type_visitor::get_variable_indices() { return variable_indices_; }