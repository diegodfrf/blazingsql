#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <execution_graph/Context.h>
#include <string>
#include <vector>
#include <tuple>
#include "blazing_table/BlazingTable.h"
#include <cudf/copying.hpp>
#include <cudf/sorting.hpp>
#include <cudf/search.hpp>
#include <arrow/array/builder_primitive.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "operators/operators_definitions.h"

std::tuple<std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder>, cudf::size_type> get_sort_vars(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_vars_to_partition(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_vars_to_orders(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_vars_to_partition_and_order(const std::string & query_part);

bool has_limit_only(const std::string & query_part);

int64_t get_limit_rows_when_relational_alg_is_simple(const std::string & query_part);

std::tuple<std::vector<int>, std::vector<voltron::compute::SortOrder>, std::vector<voltron::compute::NullOrder> > get_right_sorts_vars(const std::string & query_part);
