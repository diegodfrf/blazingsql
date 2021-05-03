#include <spdlog/spdlog.h>
#include <cudf/stream_compaction.hpp>
#include <cudf/copying.hpp>
#include "LogicalFilter.h"
#include "LogicalProject.h"
#include "parser/expression_utils.hpp"
#include "utilities/error.hpp"
#include "execution_graph/backend_dispatcher.h"
#include "blazing_table/BlazingColumn.h"
#include "blazing_table/BlazingColumnView.h"

// TODO percy arrow move code

namespace ral {
namespace cpu {

std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  std::shared_ptr<arrow::Table> table,
  std::shared_ptr<arrow::ChunkedArray> boolValues){
  //auto filteredTable = cudf::apply_boolean_mask(
  //  table.view(),boolValues);
  //return std::make_unique<ral::frame::BlazingTable>(std::move(
  //  filteredTable),table.column_names());
}

std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluate_expressions(
    std::shared_ptr<arrow::Table> table,
    const std::vector<std::string> & expressions) {
  std::cout << "FILTERRRRRRRRRRRRRRRRRR ARROWWWWWWWWWWWWWWWWWWWWWWWWWWWWW!!!!!!!\n";
}

} // namespace cpu
} // namespace ral

namespace ral {
namespace processor {
namespace {

const std::string LOGICAL_FILTER = "LogicalFilter";

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";

} // namespace

bool is_logical_filter(const std::string & query_part) {
  return query_part.find(LOGICAL_FILTER) != std::string::npos;
}

std::unique_ptr<ral::frame::BlazingCudfTable> applyBooleanFilter(
  std::shared_ptr<ral::frame::BlazingCudfTableView> table_view,
  const cudf::column_view & boolValues){
  auto filteredTable = cudf::apply_boolean_mask(table_view->view(),boolValues);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(filteredTable), table_view->column_names());
}

/// AKIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII

//{
    //return std::make_unique<ral::frame::BlazingTable>(cudf::empty_like(table_view.view()), table_view.column_names())

    // TODO percy rommel arrow
//    return nullptr;
  //}


/////////////////////////////////// build_only_schema begin

struct build_only_schema {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view) const {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingArrowTableView>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  return nullptr;
}

template <>
std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingCudfTableView>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto *table_view_ptr = dynamic_cast<ral::frame::BlazingCudfTableView*>(table_view.get());
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::empty_like(table_view_ptr->view()), table_view_ptr->column_names());
}

/////////////////////////////////// build_only_schema end

/////////////////////////////////// evaluate_expression begin

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
std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_functor::operator()<ral::frame::BlazingArrowTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
  ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table_view.get());
  std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = ral::cpu::evaluate_expressions(table_view_ptr->view(), expressions);

  // TODO percy arrow
  //RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

  return ral::cpu::applyBooleanFilter(table_view_ptr->view(), evaluated_table[0]);
}

template <>
std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
    auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(cudf_table_view->view(), expressions);
    RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");
    return applyBooleanFilter(cudf_table_view, evaluated_table[0]->view());
}

/////////////////////////////////// evaluate_expression end

std::unique_ptr<ral::frame::BlazingTable> process_filter(
  std::shared_ptr<ral::frame::BlazingTableView> table_view,
  const std::string & query_part,
  blazingdb::manager::Context * /*context*/) {

	if(table_view->num_rows() == 0) {
		return ral::execution::backend_dispatcher(table_view->get_execution_backend(), build_only_schema(), table_view);
	}

  std::string conditional_expression = get_named_expression(query_part, "condition");
	if(conditional_expression.empty()) {
		conditional_expression = get_named_expression(query_part, "filters");
	}

  auto evaluated_table = ral::execution::backend_dispatcher(table_view->get_execution_backend(), evaluate_expressions_functor(), table_view, {conditional_expression});

  RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

  return evaluated_table[0]);
}


  namespace{
    typedef std::pair<blazingdb::transport::Node, std::unique_ptr<ral::frame::BlazingTable> > NodeColumn;
    typedef std::pair<blazingdb::transport::Node, ral::frame::BlazingTableView > NodeColumnView;
  }

  
bool check_if_has_nulls(cudf::table_view const& input, std::vector<cudf::size_type> const& keys){
  auto keys_view = input.select(keys);
  if (keys_view.num_columns() != 0 && keys_view.num_rows() != 0 && cudf::has_nulls(keys_view)) {
      return true;
  }

  return false;
}

} // namespace processor
} // namespace ral
