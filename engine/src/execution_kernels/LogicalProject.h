#pragma once

#include <execution_graph/Context.h>
#include "LogicPrimitives.h"
#include "blazing_table/BlazingColumn.h"
#include "execution_graph/backend_dispatcher.h"
#include <cudf/copying.hpp>
#include "utilities/error.hpp"
#include <cudf/stream_compaction.hpp>
#include <cudf/copying.hpp>

namespace ral {
namespace cpu {

inline std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  std::shared_ptr<arrow::Table> table,
  std::shared_ptr<arrow::ChunkedArray> boolValues){
  //auto filteredTable = cudf::apply_boolean_mask(
  //  table.view(),boolValues);
  //return std::make_unique<ral::frame::BlazingTable>(std::move(
  //  filteredTable),table.column_names());
}

inline std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluate_expressions(
    std::shared_ptr<arrow::Table> table,
    const std::vector<std::string> & expressions) {
  std::cout << "FILTERRRRRRRRRRRRRRRRRR ARROWWWWWWWWWWWWWWWWWWWWWWWWWWWWW!!!!!!!\n";
}

} // namespace cpu
} // namespace ral

namespace ral{
namespace processor{

/**
 * @brief Evaluates multiple expressions consisting of arithmetic operations and
 * SQL functions.
 *
 * The computation of the results consist of two steps:
 * 1. We evaluate all complex operations operations one by one. Complex operations
 * are operations that can't be mapped as f(input_table[row]) => output_table[row]
 * for a given row in a table e.g. string functions
 *
 * 2. We batch all simple operations and evaluate all of them in a single GPU
 * kernel call. Simple operations are operations that can be mapped as
 * f(input_table[row]) => output_table[row] for a given row in a table e.g.
 * arithmetic operations and cast between primitive types
 */
std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
  const cudf::table_view & table, const std::vector<std::string> & expressions);

std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::Context * context);


inline std::unique_ptr<ral::frame::BlazingCudfTable> applyBooleanFilter(
  std::shared_ptr<ral::frame::BlazingCudfTableView> table_view,
  const cudf::column_view & boolValues){
  auto filteredTable = cudf::apply_boolean_mask(table_view->view(),boolValues);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(filteredTable), table_view->column_names());
}


struct build_only_schema {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view) const {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingArrowTableView>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingCudfTableView>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto *table_view_ptr = dynamic_cast<ral::frame::BlazingCudfTableView*>(table_view.get());
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::empty_like(table_view_ptr->view()), table_view_ptr->column_names());
}

/////////////////////////////////// build_only_schema end

/////////////////////////////////// evaluate_expression begin

struct evaluate_expressions_wo_filter_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view,
  const std::vector<std::string> & expressions) const
  {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_wo_filter_functor::operator()<ral::frame::BlazingArrowTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
  ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table_view.get());
  std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = ral::cpu::evaluate_expressions(table_view_ptr->view(), expressions);

  // TODO percy arrow
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_wo_filter_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
    auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(cudf_table_view->view(), expressions);
    return std::make_unique<ral::frame::BlazingCudfTable>(std::move(evaluated_table), table_view->column_names());
}


struct evaluate_expressions_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view,
  const std::vector<std::string> & expressions) const
  {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_functor::operator()<ral::frame::BlazingArrowTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
  ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table_view.get());
  std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = ral::cpu::evaluate_expressions(table_view_ptr->view(), expressions);

  // TODO percy arrow
  //RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

  return ral::cpu::applyBooleanFilter(table_view_ptr->view(), evaluated_table[0]);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
    auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(cudf_table_view->view(), expressions);
    //Must be applied only for filters
    //RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");
    return applyBooleanFilter(cudf_table_view, evaluated_table[0]->view());
}






//// eval return vector

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

} // namespace processor
} // namespace ral
