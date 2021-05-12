#include "distribution_utils/primitives.h"
#include "parser/CalciteExpressionParsing.h"
#include "communication/CommunicationData.h"

#include <cmath>

#include <cudf/search.hpp>
#include <cudf/sorting.hpp>
#include "cudf/copying.hpp"
#include <cudf/merge.hpp>
#include <cudf/utilities/traits.hpp>

#include "utilities/CommonOperations.h"
#include "operators/OrderBy.h"
#include "execution_graph/backend_dispatcher.h"

#include "utilities/error.hpp"
#include "utilities/ctpl_stl.h"
#include <numeric>

#include <spdlog/spdlog.h>

using namespace fmt::literals;

namespace ral {
namespace distribution {

typedef ral::frame::BlazingTable BlazingTable;
typedef ral::frame::BlazingTableView BlazingTableView;
typedef blazingdb::manager::Context Context;
typedef blazingdb::transport::Node Node;


std::unique_ptr<BlazingTable> generatePartitionPlans(
	cudf::size_type number_partitions,
	const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
	const std::vector<cudf::order> & sortOrderTypes) {

	// just to call concatTables
	std::vector<std::shared_ptr<BlazingTableView>> samplesView;
	for (std::size_t i = 0; i < samples.size(); i++){
		samplesView.push_back(samples[i]->to_table_view());
	}

	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::concatTables(samplesView);

	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);
	// TODO this is just a default setting. Will want to be able to properly set null_order
	//std::unique_ptr<cudf::column> sort_indices = cudf::sorted_order( concatSamples->view(), sortOrderTypes, null_orders);
	//std::unique_ptr<cudf::table> sortedSamples = cudf::detail::gather( concatSamples->view(), sort_indices->view(), cudf::out_of_bounds_policy::DONT_CHECK, cudf::detail::negative_index_policy::NOT_ALLOWED );

	std::unique_ptr<ral::frame::BlazingTable> sortedSamples = ral::execution::backend_dispatcher(concatSamples->to_table_view()->get_execution_backend(), ral::operators::sorted_order_gather_functor(),
		concatSamples->to_table_view(), concatSamples->to_table_view(), sortOrderTypes, null_orders);

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
// IMPORTANT: This function expects data to already be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
std::vector<NodeColumnView> partitionData(Context * context,
	std::shared_ptr<BlazingTableView> table,
	std::shared_ptr<BlazingTableView> pivots,
	const std::vector<int> & searchColIndices,
	std::vector<cudf::order> sortOrderTypes) {

	RAL_EXPECTS(static_cast<size_t>(pivots->num_columns()) == searchColIndices.size(), "Mismatched pivots num_columns and searchColIndices");

	cudf::size_type num_rows = table->num_rows();
	if(num_rows == 0) {
		std::vector<NodeColumnView> array_node_columns;
		auto nodes = context->getAllNodes();
		for(std::size_t i = 0; i < nodes.size(); ++i) {
			array_node_columns.emplace_back(nodes[i], table);
		}
		return array_node_columns;
	}

	if(sortOrderTypes.size() == 0) {
		sortOrderTypes.assign(searchColIndices.size(), cudf::order::ASCENDING);
	}

	std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitioned_data = ral::operators::partition_table(pivots, table, sortOrderTypes, searchColIndices);

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


std::unique_ptr<BlazingTable> getPivotPointsTable(cudf::size_type number_partitions, std::shared_ptr<BlazingTableView> sortedSamples){

	cudf::size_type outputRowSize = sortedSamples->num_rows();
	cudf::size_type pivotsSize = outputRowSize > 0 ? number_partitions - 1 : 0;

	int32_t step = outputRowSize / number_partitions;

	std::vector<int32_t> sequence(pivotsSize);
	std::iota(sequence.begin(), sequence.end(), 1);
	std::transform(sequence.begin(), sequence.end(), sequence.begin(), [step](int32_t i){ return i*step;});

	auto gather_map = ral::utilities::vector_to_column(sequence, cudf::data_type(cudf::type_id::INT32));

	// TODO percy rommel arrow
	std::unique_ptr<ral::frame::BlazingTable> pivots = ral::execution::backend_dispatcher(sortedSamples->get_execution_backend(), gather_functor(),
													sortedSamples, std::move(gather_map), cudf::out_of_bounds_policy::DONT_CHECK, cudf::detail::negative_index_policy::NOT_ALLOWED);
	return std::move(pivots);
}



}  // namespace distribution
}  // namespace ral
