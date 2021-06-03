#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <execution_graph/Context.h>
#include <string>
#include <vector>
#include <tuple>
#include "execution_kernels/LogicPrimitives.h"
#include "utilities/CommonOperations.h"
#include <cudf/copying.hpp>
#include <cudf/sorting.hpp>
#include <cudf/search.hpp>
#include <arrow/array/builder_primitive.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>

namespace ral {
namespace operators {

namespace {
  using blazingdb::manager::Context;
}

std::unique_ptr<ral::frame::BlazingTable> logicalSort(std::shared_ptr<ral::frame::BlazingTableView> table_view,
      const std::vector<int> & sortColIndices,	const std::vector<cudf::order> & sortOrderTypes,
      const std::vector<cudf::null_order> & sortOrderNulls);

std::tuple<std::vector<int>, std::vector<cudf::order>, std::vector<cudf::null_order>, cudf::size_type> get_sort_vars(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<cudf::order>, std::vector<cudf::null_order> > get_vars_to_partition(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<cudf::order>, std::vector<cudf::null_order> > get_vars_to_orders(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<cudf::order>, std::vector<cudf::null_order> > get_vars_to_partition_and_order(const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> sort(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part);

std::size_t compute_total_samples(std::size_t num_rows);

std::unique_ptr<ral::frame::BlazingTable> sample(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> generate_partition_plan(const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
    std::size_t table_num_rows, std::size_t avg_bytes_per_row, const std::string & query_part, Context * context);

std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partition_table(std::shared_ptr<ral::frame::BlazingTableView> partitionPlan,
	std::shared_ptr<ral::frame::BlazingTableView> sortedTable, const std::vector<cudf::order> & sortOrderTypes,	const std::vector<int> & sortColIndices,
  const std::vector<cudf::null_order> & sortOrderNulls);

bool has_limit_only(const std::string & query_part);

int64_t get_limit_rows_when_relational_alg_is_simple(const std::string & query_part);

std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t> limit_table(std::shared_ptr<ral::frame::BlazingTableView> table_view, int64_t num_rows_limit);

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitions_to_merge, const std::string & query_part);

}  // namespace operators
}  // namespace ral
