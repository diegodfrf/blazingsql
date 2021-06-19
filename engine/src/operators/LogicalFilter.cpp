#include <spdlog/spdlog.h>
#include "LogicalFilter.h"
#include "LogicalProject.h"
#include "parser/expression_utils.hpp"
#include "utilities/error.hpp"
#include "compute/backend_dispatcher.h"

#include "compute/api.h"


namespace ral {
namespace operators {
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

  

} // namespace operators
} // namespace ral
