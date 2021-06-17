#pragma once

#include "execution_graph/Context.h"
#include "communication/factory/MessageFactory.h"
#include <vector>
#include "blazing_table/BlazingTableView.h"
#include "operators_definitions.h"

namespace ral {
namespace distribution {

	namespace {
		using Context = blazingdb::manager::Context;
		using Node = blazingdb::transport::Node;
	}  // namespace

	typedef std::pair<blazingdb::transport::Node, std::unique_ptr<ral::frame::BlazingTable> > NodeColumn;
	typedef std::pair<blazingdb::transport::Node, std::shared_ptr<ral::frame::BlazingTableView> > NodeColumnView;
	using namespace ral::frame;

	std::unique_ptr<BlazingTable> generatePartitionPlans(
		int number_partitions,
		const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
		const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
		const std::vector<voltron::compute::NullOrder> & sortOrderNulls);

	std::unique_ptr<BlazingTable> getPartitionPlan(Context * context);

// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices, sortOrderTypes and sortOrderNulls
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
	std::vector<NodeColumnView> partitionData(Context * context,
		std::shared_ptr<BlazingTableView> table,
		std::shared_ptr<BlazingTableView> pivots,
		const std::vector<int> & searchColIndices,
		std::vector<voltron::compute::SortOrder> sortOrderTypes,
		const std::vector<voltron::compute::NullOrder> & sortOrderNulls);

	std::unique_ptr<BlazingTable> getPivotPointsTable(int number_pivots, std::shared_ptr<BlazingTableView> sortedSamples);

}  // namespace distribution
}  // namespace ral
