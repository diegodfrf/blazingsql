#pragma once

#include "blazing_table/BlazingCudfTable.h"

/**
Takes a table and applies a boolean filter to it
*/
std::unique_ptr<ral::frame::BlazingCudfTable> applyBooleanFilter(
  std::shared_ptr<ral::frame::BlazingCudfTableView> table_view,
  const cudf::column_view & boolValues);
