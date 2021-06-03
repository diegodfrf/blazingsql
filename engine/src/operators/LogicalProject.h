#pragma once

#include <execution_graph/Context.h>
#include "blazing_table/BlazingTable.h"
#include "blazing_table/BlazingColumn.h"
#include "compute/backend_dispatcher.h"
#include <cudf/copying.hpp>
#include "utilities/error.hpp"
#include <cudf/stream_compaction.hpp>
#include <cudf/copying.hpp>
#include <regex>

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
#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"


namespace ral{
namespace operators{





std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::Context * context);


//struct evaluate_expressions_vector_functor {
//  template <typename T>
//  std::vector<std::unique_ptr<ral::frame::BlazingTable>> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view,
//  const std::vector<std::string> & expressions) const
//  {
//    // TODO percy arrow thrown error
//    return nullptr;
//  }
//};

//template <>
//std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_vector_functor::operator()<ral::frame::BlazingArrowTable>(
//  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
//{
//  ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table_view.get());
//  std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = ral::cpu::evaluate_expressions(table_view_ptr->view(), expressions);

//  // TODO percy arrow
//  //RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

//  return ral::cpu::applyBooleanFilter(table_view_ptr->view(), evaluated_table[0]);
//}

//template <>
//std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_vector_functor::operator()<ral::frame::BlazingCudfTable>(
//  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
//{
//    auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
//    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(cudf_table_view->view(), expressions);
//    RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");
//    return applyBooleanFilter(cudf_table_view, evaluated_table[0]->view());
//}

} // namespace operators
} // namespace ral
