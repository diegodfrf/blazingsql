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
#include "operators_definitions.h"

namespace ral {
namespace operators {

namespace {
  using blazingdb::manager::Context;
}

std::unique_ptr<ral::frame::BlazingTable> logicalSort(std::shared_ptr<ral::frame::BlazingTableView> table_view,
      const std::vector<int> & sortColIndices,	const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
      const std::vector<voltron::compute::NullOrder> & sortOrderNulls);

std::unique_ptr<ral::frame::BlazingTable> sort(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part);

std::size_t compute_total_samples(std::size_t num_rows);

std::unique_ptr<ral::frame::BlazingTable> sample(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> generate_partition_plan(const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
    std::size_t table_num_rows, std::size_t avg_bytes_per_row, const std::string & query_part, Context * context);

std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partition_table(std::shared_ptr<ral::frame::BlazingTableView> partitionPlan,
	std::shared_ptr<ral::frame::BlazingTableView> sortedTable, const std::vector<voltron::compute::SortOrder> & sortOrderTypes,	const std::vector<int> & sortColIndices,
  const std::vector<voltron::compute::NullOrder> & sortOrderNulls);

std::unique_ptr<ral::frame::BlazingTable> getLimitedRows(std::shared_ptr<ral::frame::BlazingTableView> table_view, int num_rows, bool front=true);

std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t> limit_table(std::shared_ptr<ral::frame::BlazingTableView> table_view, int64_t num_rows_limit);

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitions_to_merge, const std::string & query_part);

}  // namespace operators
}  // namespace ral
