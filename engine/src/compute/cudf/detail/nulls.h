#pragma once

#include <cudf/table/table.hpp>

bool check_if_has_nulls(cudf::table_view const& input, std::vector<cudf::size_type> const& keys);
