#include <spdlog/spdlog.h>
#include "LogicalProject.h"
#include "parser/expression_utils.hpp"
#include "parser/project_parser_utils.h"
#include "compute/api.h"

namespace ral {
namespace operators {

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

} // namespace operators
} // namespace ral
