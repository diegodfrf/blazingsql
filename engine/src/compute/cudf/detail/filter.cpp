#include "compute/cudf/detail/filter.h"

#include <cudf/stream_compaction.hpp>

std::unique_ptr<ral::frame::BlazingCudfTable> applyBooleanFilter(
  std::shared_ptr<ral::frame::BlazingCudfTableView> table_view,
  const cudf::column_view & boolValues){
  auto filteredTable = cudf::apply_boolean_mask(table_view->view(),boolValues);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(filteredTable), table_view->column_names());
}
