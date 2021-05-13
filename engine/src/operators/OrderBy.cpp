#include "OrderBy.h"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include "communication/CommunicationData.h"
#include "distribution_utils/primitives.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/copying.hpp>
#include <cudf/sorting.hpp>
#include <cudf/search.hpp>
#include <random>
#include "parser/expression_utils.hpp"
#include "utilities/CommonOperations.h"
#include <blazingdb/io/Util/StringUtil.h>
#include "execution_graph/backend_dispatcher.h"

using namespace fmt::literals;

namespace ral {
namespace cpu {
namespace operators {

std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t>
limit_table(std::shared_ptr<arrow::Table> table, int64_t num_rows_limit) {
	cudf::size_type table_rows = table->num_rows();
	if (num_rows_limit <= 0) {
		return std::make_tuple(std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns(), 0)), false, 0);
	} else if (num_rows_limit >= table_rows) {
		return std::make_tuple(std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns())), true, num_rows_limit - table_rows);
	} else {
		return std::make_tuple(ral::cpu::utilities::getLimitedRows(table, num_rows_limit), false, 0);
	}
}

}  // namespace operators
}  // namespace cpu

namespace operators {

using blazingdb::manager::Context;
using blazingdb::transport::Node;
using ral::communication::CommunicationData;
using namespace ral::distribution;

const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";

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
	const std::vector<cudf::order> & sortOrderTypes) {

	std::shared_ptr<ral::frame::BlazingTableView> sortColumns = ral::execution::backend_dispatcher(
    table_view->get_execution_backend(), select_functor(), table_view, sortColIndices);

	/*ToDo: Edit this according the Calcite output*/
	std::vector<cudf::null_order> null_orders(sortColIndices.size(), cudf::null_order::AFTER);

  return ral::execution::backend_dispatcher(table_view->get_execution_backend(), sorted_order_gather_functor(),
    table_view, sortColumns, sortOrderTypes, null_orders);
}


std::tuple<std::vector<int>, std::vector<cudf::order>, cudf::size_type>
get_sort_vars(const std::string & query_part) {
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd);

	int num_sort_columns = count_string_occurrence(combined_expression, "sort");

	std::vector<int> sortColIndices(num_sort_columns);
	std::vector<cudf::order> sortOrderTypes(num_sort_columns);
	for(auto i = 0; i < num_sort_columns; i++) {
		sortColIndices[i] = get_index(get_named_expression(combined_expression, "sort" + std::to_string(i)));
		sortOrderTypes[i] = (get_named_expression(combined_expression, "dir" + std::to_string(i)) == ASCENDING_ORDER_SORT_TEXT ? cudf::order::ASCENDING : cudf::order::DESCENDING);
	}

	std::string limitRowsStr = get_named_expression(combined_expression, "fetch");
	cudf::size_type limitRows = !limitRowsStr.empty() ? std::stoi(limitRowsStr) : -1;

	return std::make_tuple(sortColIndices, sortOrderTypes, limitRows);
}

// input: min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $3)], n_nationkey=[$0]
// output: < [1, 2], [cudf::ASCENDING, cudf::ASCENDING] >
std::tuple< std::vector<int>, std::vector<cudf::order> > get_vars_to_partition(const std::string & logical_plan) {
	std::vector<int> column_index;
	std::vector<cudf::order> order_types;
	const std::string partition_expr = "PARTITION BY ";

	// PARTITION BY $1, $2 ORDER BY $3
	std::string over_expression = get_first_over_expression_from_logical_plan(logical_plan, partition_expr);

	if (over_expression.size() == 0) {
		return std::make_tuple(column_index, order_types);
	}

	size_t start_position = over_expression.find(partition_expr) + partition_expr.size();
	size_t end_position = over_expression.find("ORDER BY ");

	if (end_position == get_query_part(logical_plan).npos) {
		end_position = over_expression.size() + 1;
	}
	// $1, $2
	std::string values = over_expression.substr(start_position, end_position - start_position - 1);
	std::vector<std::string> column_numbers_string = StringUtil::split(values, ", ");
	for (size_t i = 0; i < column_numbers_string.size(); i++) {
		column_numbers_string[i] = StringUtil::replace(column_numbers_string[i], "$", "");
		column_index.push_back(std::stoi(column_numbers_string[i]));
		order_types.push_back(cudf::order::ASCENDING);
	}

	return std::make_tuple(column_index, order_types);
}

// input: min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $3, $1 DESC)], n_nationkey=[$0]
// output: < [3, 1], [cudf::ASCENDING, cudf::DESCENDING] >
std::tuple< std::vector<int>, std::vector<cudf::order> > get_vars_to_orders(const std::string & logical_plan) {
	std::vector<int> column_index;
	std::vector<cudf::order> order_types;
	std::string order_expr = "ORDER BY ";

	// PARTITION BY $2 ORDER BY $3, $1 DESC
	std::string over_expression = get_first_over_expression_from_logical_plan(logical_plan, order_expr);

	if (over_expression.size() == 0) {
		return std::make_tuple(column_index, order_types);
	}

	size_t start_position = over_expression.find(order_expr) + order_expr.size();
	size_t end_position = over_expression.find("ROWS");
	if (end_position != over_expression.npos) {
		end_position = end_position - 1;
	} else {
		end_position = over_expression.size();
	}

	// $3, $1 DESC
	std::string values = over_expression.substr(start_position, end_position - start_position);
	std::vector<std::string> column_express = StringUtil::split(values, ", ");
	for (std::size_t i = 0; i < column_express.size(); ++i) {
		std::vector<std::string> split_parts = StringUtil::split(column_express[i], " ");
		if (split_parts.size() == 1) order_types.push_back(cudf::order::ASCENDING);
		else order_types.push_back(cudf::order::DESCENDING);

		split_parts[0] = StringUtil::replace(split_parts[0], "$", "");
		column_index.push_back(std::stoi(split_parts[0]));
	}

	return std::make_tuple(column_index, order_types);
}

// input: min_keys=[MIN($0) OVER (PARTITION BY $1, $2 ORDER BY $3 DESC)], n_nationkey=[$0]
// output: < [1, 2, 3], [cudf::ASCENDING, cudf::ASCENDING, cudf::DESCENDING] >
std::tuple< std::vector<int>, std::vector<cudf::order> > get_vars_to_partition_and_order(const std::string & query_part) {
	std::vector<int> column_index_partition, column_index_order;
	std::vector<cudf::order> order_types_partition, order_types_order;

	std::tie(column_index_partition, order_types_partition) = get_vars_to_partition(query_part);
	std::tie(column_index_order, order_types_order) = get_vars_to_orders(query_part);

	column_index_partition.insert(column_index_partition.end(), column_index_order.begin(), column_index_order.end());
	order_types_partition.insert(order_types_partition.end(), order_types_order.begin(), order_types_order.end());

	return std::make_tuple(column_index_partition, order_types_partition);
}

std::tuple<std::vector<int>, std::vector<cudf::order> > get_right_sorts_vars(const std::string & query_part) {
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;

	if (is_window_function(query_part)) {
		// `order by` and `partition by`
		if (window_expression_contains_order_by(query_part) && window_expression_contains_partition_by(query_part)) {
			std::tie(sortColIndices, sortOrderTypes) = get_vars_to_partition_and_order(query_part);
		}
		// only `partition by`
		else if (!window_expression_contains_order_by(query_part)) {
			std::tie(sortColIndices, sortOrderTypes) = get_vars_to_partition(query_part);
		}
		// without `partition by`
		else {
			std::tie(sortColIndices, sortOrderTypes) = get_vars_to_orders(query_part);
		}
	} else std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);

	return std::make_tuple(sortColIndices, sortOrderTypes);
}

bool has_limit_only(const std::string & query_part){
	std::vector<int> sortColIndices;
	std::tie(sortColIndices, std::ignore, std::ignore) = get_sort_vars(query_part);

	return sortColIndices.empty();
}

int64_t get_limit_rows_when_relational_alg_is_simple(const std::string & query_part){
	int64_t limitRows;
	std::tie(std::ignore, std::ignore, limitRows) = get_sort_vars(query_part);
	return limitRows;
}

std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t>
limit_table(std::shared_ptr<ral::frame::BlazingTableView> table_view, int64_t num_rows_limit) {

	cudf::size_type table_rows = table_view->num_rows();
	if (num_rows_limit <= 0) {
    std::unique_ptr<ral::frame::BlazingTable> empty = ral::execution::backend_dispatcher(
                                                        table_view->get_execution_backend(),
                                                        create_empty_table_functor(),
                                                        table_view);
		return std::make_tuple(std::move(empty), false, 0);
	} else if (num_rows_limit >= table_rows) {
    std::unique_ptr<ral::frame::BlazingTable> temp = ral::execution::backend_dispatcher(
                                                        table_view->get_execution_backend(),
                                                        from_table_view_to_table_functor(),
                                                        table_view);
		return std::make_tuple(std::move(temp), true, num_rows_limit - table_rows);
	} else {
		return std::make_tuple(ral::utilities::getLimitedRows(table_view, num_rows_limit), false, 0);
	}
}

std::unique_ptr<ral::frame::BlazingTable> sort(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part){
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;

	std::tie(sortColIndices, sortOrderTypes) = get_right_sorts_vars(query_part);

	return logicalSort(table_view, sortColIndices, sortOrderTypes);
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
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	
	if (is_window_function(query_part)){
		if (window_expression_contains_partition_by(query_part)) {
			std::tie(sortColIndices, sortOrderTypes) = get_vars_to_partition(query_part);
		} else {
			std::tie(sortColIndices, sortOrderTypes) = get_vars_to_orders(query_part);
		}
	}
	else {
		std::tie(sortColIndices, sortOrderTypes, std::ignore) = get_sort_vars(query_part);
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
	const std::vector<cudf::order> & sortOrderTypes,
	const std::vector<int> & sortColIndices) {
	
	if (sortedTable->num_rows() == 0) {
		return {sortedTable};
	}

	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);

  std::shared_ptr<ral::frame::BlazingTableView> columns_to_search = ral::execution::backend_dispatcher(
    sortedTable->get_execution_backend(), select_functor(), sortedTable, sortColIndices);

  return ral::execution::backend_dispatcher(columns_to_search->get_execution_backend(), upper_bound_split_functor(),
    sortedTable, columns_to_search, partitionPlan, sortOrderTypes, null_orders);
}

std::unique_ptr<ral::frame::BlazingTable> generate_partition_plan(
	const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
	std::size_t table_num_rows, std::size_t avg_bytes_per_row,
	const std::string & query_part, Context * context) {

	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	
	if (is_window_function(query_part)){
		if (window_expression_contains_partition_by(query_part)) {
			std::tie(sortColIndices, sortOrderTypes) = get_vars_to_partition(query_part);
		} else {
			std::tie(sortColIndices, sortOrderTypes) = get_vars_to_orders(query_part);
		}
	}
	else {
		std::tie(sortColIndices, sortOrderTypes, std::ignore) = get_sort_vars(query_part);
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
	cudf::size_type total_num_partitions = (double)table_num_rows*(double)avg_bytes_per_row/(double)num_bytes_per_order_by_partition;
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

	if( ral::utilities::checkIfConcatenatingStringsWillOverflow(samples)) {
	    if(logger){
            logger->warn("{query_id}|{step}|{substep}|{info}",
                            "query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
                            "step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
                            "substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
                            "info"_a="In generatePartitionPlans Concatenating Strings will overflow strings length");
	    }
	}

	partitionPlan = generatePartitionPlans(total_num_partitions, samples, sortOrderTypes);
	context->incrementQuerySubstep();
	return partitionPlan;
}

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitions_to_merge, const std::string & query_part) {
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	
	std::tie(sortColIndices, sortOrderTypes) = get_right_sorts_vars(query_part);

	return ral::execution::backend_dispatcher(partitions_to_merge[0]->get_execution_backend(), sorted_merger_functor(),
		partitions_to_merge, sortOrderTypes, sortColIndices);
}

}  // namespace operators
}  // namespace ral
