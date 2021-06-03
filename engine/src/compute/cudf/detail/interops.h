#pragma once

#include "blazing_table/BlazingColumn.h"

inline std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
    const cudf::table_view & table,
    const std::vector<std::string> & expressions);
