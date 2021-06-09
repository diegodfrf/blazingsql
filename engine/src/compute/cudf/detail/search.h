#pragma once

#include <cudf/sorting.hpp>

#include "blazing_table/BlazingCudfTable.h"

std::unique_ptr<ral::frame::BlazingTable> sorted_merger(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables,
                   const std::vector<cudf::order> & sortOrderTypes,
                   const std::vector<int> & sortColIndices, const std::vector<cudf::null_order> & sortOrderNulls);
