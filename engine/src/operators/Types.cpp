#include "operators/Types.h"

#include "compute/api.h"
#include "compute/backend_dispatcher.h"

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(std::shared_ptr<ral::frame::BlazingTableView> table) {
  return ral::execution::backend_dispatcher(
           table->get_execution_backend(),
           create_empty_table_like_functor(),
           table);
}

void normalize_types(std::unique_ptr<ral::frame::BlazingTable> & table, const std::vector<std::shared_ptr<arrow::DataType>> & types,
  std::vector<cudf::size_type> column_indices) {
  ral::execution::backend_dispatcher(
           table->get_execution_backend(),
           normalize_types_functor(),
           table, types, column_indices);
}
