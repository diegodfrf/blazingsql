#pragma once

#include <cudf/sorting.hpp>
#include "types.h"
#include "operators/operators_definitions.h"

#include "blazing_table/BlazingCudfTable.h"

std::unique_ptr<ral::frame::BlazingTable> sorted_merger(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables,
                   const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
                   const std::vector<int> & sortColIndices, const std::vector<voltron::compute::NullOrder> & sortOrderNulls);
