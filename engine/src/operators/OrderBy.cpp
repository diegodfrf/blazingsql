#include "OrderBy.h"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include "communication/CommunicationData.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <random>
#include "parser/expression_utils.hpp"
#include <blazingdb/io/Util/StringUtil.h>
#include "compute/backend_dispatcher.h"
#include "compute/api.h"
#include "parser/orderby_parser_utils.h"
#include "operators/Concatenate.h"
#include "operators/Distribution.h"

using namespace fmt::literals;

namespace ral {
namespace operators {

using blazingdb::manager::Context;
using blazingdb::transport::Node;
using ral::communication::CommunicationData;
using namespace ral::distribution;

/**---------------------------------------------------------------------------*
 * @brief Sorts the columns of the input table according the sortOrderTypes
 * and sortColIndices.
 *
 * @param[in] table             table whose rows need to be compared for ordering
 * @param[in] sortColIndices    The vector of selected column indices to perform
 *                              the sort.
 * @param[in] sortOrderTypes    The expected sort order for each column. Size
 *                              must be equal to `sortColIndices.size()` or empty.
 *
 * @returns A BlazingTable with rows sorted.
 *---------------------------------------------------------------------------**/
std::unique_ptr<ral::frame::BlazingTable> logicalSort(
  std::shared_ptr<ral::frame::BlazingTableView> table_view,
	const std::vector<int> & sortColIndices,
	const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
	const std::vector<voltron::compute::NullOrder> & sortOrderNulls) {

	std::shared_ptr<ral::frame::BlazingTableView> sortColumns = ral::execution::backend_dispatcher(
    table_view->get_execution_backend(), select_functor(), table_view, sortColIndices);

  return ral::execution::backend_dispatcher(table_view->get_execution_backend(), sorted_order_gather_functor(),
    table_view, sortColumns, sortOrderTypes, sortOrderNulls);
}

std::unique_ptr<BlazingTable> getLimitedRows(std::shared_ptr<BlazingTableView> table, int num_rows, bool front){
	if (num_rows == 0) {
    return ral::execution::backend_dispatcher(
          table->get_execution_backend(),
          create_empty_table_like_functor(),
          table);
	} else if (num_rows < table->num_rows()) {
		std::unique_ptr<ral::frame::BlazingTable> cudf_table;
		if (front){
			std::vector<int> splits = {num_rows};
			auto split_table = ral::execution::backend_dispatcher(table->get_execution_backend(), split_functor(), table, splits);
			cudf_table = ral::execution::backend_dispatcher(split_table[0]->get_execution_backend(), from_table_view_to_table_functor(), split_table[0]);
		} else { // back
			std::vector<int> splits;
      splits.push_back(table->num_rows() - num_rows);
      auto split_table = ral::execution::backend_dispatcher(table->get_execution_backend(), split_functor(), table, splits);
      cudf_table = ral::execution::backend_dispatcher(split_table[1]->get_execution_backend(), from_table_view_to_table_functor(), split_table[1]);
		}
		return cudf_table;
	} else {
    return ral::execution::backend_dispatcher(table->get_execution_backend(), from_table_view_to_table_functor(), table);
	}
}

std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t>
limit_table(std::shared_ptr<ral::frame::BlazingTableView> table_view, int64_t num_rows_limit) {

	int table_rows = table_view->num_rows();
	if (num_rows_limit <= 0) {
    std::unique_ptr<ral::frame::BlazingTable> empty = ral::execution::backend_dispatcher(
                                                        table_view->get_execution_backend(),
                                                        create_empty_table_like_functor(),
                                                        table_view);
		return std::make_tuple(std::move(empty), false, 0);
	} else if (num_rows_limit >= table_rows) {
    std::unique_ptr<ral::frame::BlazingTable> temp = ral::execution::backend_dispatcher(
                                                        table_view->get_execution_backend(),
                                                        from_table_view_to_table_functor(),
                                                        table_view);
		return std::make_tuple(std::move(temp), true, num_rows_limit - table_rows);
	} else {
		return std::make_tuple(getLimitedRows(table_view, num_rows_limit), false, 0);
	}
}

std::unique_ptr<ral::frame::BlazingTable> sort(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part){
	std::vector<voltron::compute::SortOrder> sortOrderTypes;
	std::vector<voltron::compute::NullOrder> sortOrderNulls;
	std::vector<int> sortColIndices;

	std::tie(sortColIndices, sortOrderTypes, sortOrderNulls) = get_right_sorts_vars(query_part);

	return logicalSort(table_view, sortColIndices, sortOrderTypes, sortOrderNulls);
}

std::size_t compute_total_samples(std::size_t num_rows) {
	std::size_t num_samples = std::ceil(num_rows * 0.1);
	std::size_t MAX_SAMPLES = 1000;
	std::size_t MIN_SAMPLES = 100;
	num_samples = std::min(num_samples, MAX_SAMPLES);  // max 1000 per batch
	num_samples = std::max(num_samples, MIN_SAMPLES);  // min 100 per batch
	num_samples = num_rows < num_samples ? num_rows : num_samples; // lets make sure that `num_samples` is not actually bigger than the batch

	return num_samples;
}

std::unique_ptr<ral::frame::BlazingTable> sample(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part){
	std::vector<voltron::compute::SortOrder> sortOrderTypes;
	std::vector<int> sortColIndices;
	
	if (is_window_function(query_part)){
//		if (window_expression_contains_partition_by(query_part)) {
//			std::tie(sortColIndices, sortOrderTypes, std::ignore) = get_vars_to_partition(query_part);
//		} else {
//			std::tie(sortColIndices, sortOrderTypes, std::ignore) = get_vars_to_orders(query_part);
//		}
	}
	else {
		std::tie(sortColIndices, sortOrderTypes, std::ignore, std::ignore) = get_sort_vars(query_part);
	}

	auto tableNames = table_view->column_names();
	std::vector<std::string> sortColNames(sortColIndices.size());
	std::transform(sortColIndices.begin(), sortColIndices.end(), sortColNames.begin(), [&](auto index) { return tableNames.size() > index ? tableNames[index] : "" ; });

	std::size_t num_samples = compute_total_samples(table_view->num_rows());
	auto samples = ral::execution::backend_dispatcher(
                   table_view->get_execution_backend(),
                   sample_functor(),
                   table_view,
                   num_samples, sortColNames, sortColIndices);

	return samples;
}

std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partition_table(std::shared_ptr<ral::frame::BlazingTableView> partitionPlan,
	std::shared_ptr<ral::frame::BlazingTableView> sortedTable,
	const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
	const std::vector<int> & sortColIndices,
	const std::vector<voltron::compute::NullOrder> & sortOrderNulls) {
	
	if (sortedTable->num_rows() == 0) {
		return {sortedTable};
	}

  std::shared_ptr<ral::frame::BlazingTableView> columns_to_search = ral::execution::backend_dispatcher(
    sortedTable->get_execution_backend(), select_functor(), sortedTable, sortColIndices);

  // TODO percy arrow 4
//  return ral::execution::backend_dispatcher(columns_to_search->get_execution_backend(), upper_bound_split_functor(),
//    sortedTable, columns_to_search, partitionPlan, sortOrderTypes, sortOrderNulls);
}

std::unique_ptr<ral::frame::BlazingTable> generate_partition_plan(
	const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
	std::size_t table_num_rows, std::size_t avg_bytes_per_row,
	const std::string & query_part, Context * context) {

	std::vector<voltron::compute::SortOrder> sortOrderTypes;
	std::vector<voltron::compute::NullOrder> sortOrderNulls;
	std::vector<int> sortColIndices;
	int limitRows;
	
	if (is_window_function(query_part)){
		if (window_expression_contains_partition_by(query_part)) {
			std::tie(sortColIndices, sortOrderTypes, sortOrderNulls) = get_vars_to_partition(query_part);
		} else {
			std::tie(sortColIndices, sortOrderTypes, sortOrderNulls) = get_vars_to_orders(query_part);
		}
	}
	else {
		std::tie(sortColIndices, sortOrderTypes, sortOrderNulls, std::ignore) = get_sort_vars(query_part);
	}
	
	std::unique_ptr<ral::frame::BlazingTable> partitionPlan;

	std::size_t num_bytes_per_order_by_partition = 400000000;
	int max_num_order_by_partitions_per_node = 8;
	std::map<std::string, std::string> config_options = context->getConfigOptions();
	auto it = config_options.find("NUM_BYTES_PER_ORDER_BY_PARTITION");
	if (it != config_options.end()){
		num_bytes_per_order_by_partition = std::stoull(config_options["NUM_BYTES_PER_ORDER_BY_PARTITION"]);
	}
	it = config_options.find("MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE");
	if (it != config_options.end()){
		max_num_order_by_partitions_per_node = std::stoi(config_options["MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE"]);
	}

	int num_nodes = context->getTotalNodes();
	int total_num_partitions = (double)table_num_rows*(double)avg_bytes_per_row/(double)num_bytes_per_order_by_partition;
	total_num_partitions = total_num_partitions <= 0 ? 1 : total_num_partitions;
	// want to make the total_num_partitions to be a multiple of the number of nodes to evenly distribute
	total_num_partitions = ((total_num_partitions + num_nodes - 1) / num_nodes) * num_nodes;
	total_num_partitions = total_num_partitions > max_num_order_by_partitions_per_node * num_nodes ? max_num_order_by_partitions_per_node * num_nodes : total_num_partitions;

	std::string info = "table_num_rows: " + std::to_string(table_num_rows) + " avg_bytes_per_row: " + std::to_string(avg_bytes_per_row) +
							" total_num_partitions: " + std::to_string(total_num_partitions) +
							" NUM_BYTES_PER_ORDER_BY_PARTITION: " + std::to_string(num_bytes_per_order_by_partition) +
							" MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE: " + std::to_string(max_num_order_by_partitions_per_node);

    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|||||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="Determining Number of Order By Partitions " + info);
    }

	if( checkIfConcatenatingStringsWillOverflow(samples)) {
	    if(logger){
            logger->warn("{query_id}|{step}|{substep}|{info}",
                            "query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
                            "step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
                            "substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
                            "info"_a="In generatePartitionPlans Concatenating Strings will overflow strings length");
	    }
	}

	partitionPlan = generatePartitionPlans(total_num_partitions, samples, sortOrderTypes, sortOrderNulls);
	context->incrementQuerySubstep();
	return partitionPlan;
}

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitions_to_merge, const std::string & query_part) {
	std::vector<voltron::compute::SortOrder> sortOrderTypes;
	std::vector<voltron::compute::NullOrder> sortOrderNulls;
	std::vector<int> sortColIndices;
	
	std::tie(sortColIndices, sortOrderTypes, sortOrderNulls) = get_right_sorts_vars(query_part);

	return ral::execution::backend_dispatcher(partitions_to_merge[0]->get_execution_backend(), sorted_merger_functor(),
		partitions_to_merge, sortOrderTypes, sortColIndices, sortOrderNulls);
}

}  // namespace operators
}  // namespace ral
