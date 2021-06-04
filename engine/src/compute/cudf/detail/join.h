#pragma once

#include <cudf/table/table.hpp>

std::unique_ptr<cudf::table> reordering_columns_due_to_right_join(std::unique_ptr<cudf::table> table_ptr, size_t n_right_columns);
