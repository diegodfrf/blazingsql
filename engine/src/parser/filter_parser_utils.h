#pragma once

#include <cudf/stream_compaction.hpp>
#include <cudf/copying.hpp>
#include "operators/LogicalFilter.h"
#include "operators/LogicalProject.h"
#include "parser/expression_utils.hpp"
#include "utilities/error.hpp"
#include "blazing_table/BlazingColumn.h"
#include "blazing_table/BlazingColumnView.h"

bool is_logical_filter(const std::string & query_part);
