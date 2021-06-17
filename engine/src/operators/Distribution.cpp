#include "operators/Distribution.h"
#include "parser/CalciteExpressionParsing.h"
#include "communication/CommunicationData.h"

#include <cmath>

#include "operators/OrderBy.h"
#include "operators/Concatenate.h"
#include "compute/backend_dispatcher.h"

#include "utilities/error.hpp"
#include "utilities/ctpl_stl.h"
#include <numeric>
#include <spdlog/spdlog.h>

#include "compute/api.h"

using namespace fmt::literals;

namespace ral {
namespace distribution {

typedef ral::frame::BlazingTable BlazingTable;
typedef ral::frame::BlazingTableView BlazingTableView;
typedef blazingdb::manager::Context Context;
typedef blazingdb::transport::Node Node;

std::unique_ptr<BlazingTable> generatePartitionPlans(
	int number_partitions,
	const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
	const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
	const std::vector<voltron::compute::NullOrder> & sortOrderNulls) {

	// just to call concatTables
	std::vector<std::shared_ptr<BlazingTableView>> samplesView;
	for (std::size_t i = 0; i < samples.size(); i++){
		samplesView.push_back(samples[i]->to_table_view());
	}

	std::unique_ptr<BlazingTable> concatSamples = concatTables(samplesView);

	std::unique_ptr<ral::frame::BlazingTable> sortedSamples = ral::execution::backend_dispatcher(concatSamples->to_table_view()->get_execution_backend(), sorted_order_gather_functor(),
		concatSamples->to_table_view(), concatSamples->to_table_view(), sortOrderTypes, sortOrderNulls);

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < samples.size(); i++) {
		if (samples[i]->column_names().size() > 0){
			names = samples[i]->column_names();
			break;
		}
	}
	if(names.size() == 0){
		throw std::runtime_error("ERROR in generatePartitionPlans. names.size() == 0");
	}

	return getPivotPointsTable(number_partitions, sortedSamples->to_table_view());
}

// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to already be sorted according to the searchColIndices, sortOrderTypes and sortOrderNulls
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
std::vector<NodeColumnView> partitionData(Context * context,
	std::shared_ptr<BlazingTableView> table,
	std::shared_ptr<BlazingTableView> pivots,
	const std::vector<int> & searchColIndices,
	std::vector<voltron::compute::SortOrder> sortOrderTypes,
	const std::vector<voltron::compute::NullOrder> & sortOrderNulls) {

	RAL_EXPECTS(static_cast<size_t>(pivots->num_columns()) == searchColIndices.size(), "Mismatched pivots num_columns and searchColIndices");

	int num_rows = table->num_rows();
	if(num_rows == 0) {
		std::vector<NodeColumnView> array_node_columns;
		auto nodes = context->getAllNodes();
		for(std::size_t i = 0; i < nodes.size(); ++i) {
			array_node_columns.emplace_back(nodes[i], table);
		}
		return array_node_columns;
	}

	if(sortOrderTypes.size() == 0) {
		sortOrderTypes.assign(searchColIndices.size(), voltron::compute::SortOrder::ASCENDING);
	}

	std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitioned_data = ral::operators::partition_table(pivots, table, sortOrderTypes, searchColIndices, sortOrderNulls);

	std::vector<Node> all_nodes = context->getAllNodes();

	RAL_EXPECTS(all_nodes.size() <= partitioned_data.size(), "Number of table partitions is smalled than total nodes");

	int step = static_cast<int>(partitioned_data.size() / all_nodes.size());
	std::vector<NodeColumnView> partitioned_node_column_views;
	for (int i = 0; static_cast<size_t>(i) < partitioned_data.size(); i++){
		int node_idx = std::min(i / step, static_cast<int>(all_nodes.size() - 1));
		partitioned_node_column_views.emplace_back(all_nodes[node_idx], partitioned_data[i]);
	}

	return partitioned_node_column_views;
}

std::unique_ptr<BlazingTable> getPivotPointsTable(int number_partitions, std::shared_ptr<BlazingTableView> sortedSamples){

	int outputRowSize = sortedSamples->num_rows();
	int pivotsSize = outputRowSize > 0 ? number_partitions - 1 : 0;

	int32_t step = outputRowSize / number_partitions;

	std::vector<int32_t> sequence(pivotsSize);
	std::iota(sequence.begin(), sequence.end(), 1);
	std::transform(sequence.begin(), sequence.end(), sequence.begin(), [step](int32_t i){ return i*step;});

#ifdef CUDF_SUPPORT
	auto gather_map = vector_to_column(sequence, cudf::data_type(cudf::type_id::INT32));

	// TODO percy rommel arrow
	std::unique_ptr<ral::frame::BlazingTable> pivots = ral::execution::backend_dispatcher(sortedSamples->get_execution_backend(), gather_functor(),
													sortedSamples, std::move(gather_map), cudf::out_of_bounds_policy::DONT_CHECK, cudf::detail::negative_index_policy::NOT_ALLOWED);
	return std::move(pivots);
#else
  return nullptr; // TODO percy arrow 4
#endif
}

}  // namespace distribution
}  // namespace ral
