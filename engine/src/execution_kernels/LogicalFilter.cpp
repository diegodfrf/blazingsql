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
  
  std::vector<std::string> conditional_expressions;
  conditional_expressions.push_back(conditional_expression);

  std::unique_ptr<ral::frame::BlazingTable> evaluated_table = ral::execution::backend_dispatcher(
    table_view->get_execution_backend(),
    evaluate_expressions_functor(),
    table_view, conditional_expressions);

  return evaluated_table;
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
