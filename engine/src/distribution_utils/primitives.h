#pragma once

#include "execution_graph/Context.h"
#include "communication/factory/MessageFactory.h"
#include <vector>
#include "execution_kernels/LogicPrimitives.h"
#include <cudf/merge.hpp>
#include "cudf/detail/gather.hpp"

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
		cudf::size_type number_partitions, const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
		const std::vector<cudf::order> & sortOrderTypes);

	std::unique_ptr<BlazingTable> getPartitionPlan(Context * context);

// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices and sortOrderTypes
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
	std::vector<NodeColumnView> partitionData(Context * context,
		std::shared_ptr<BlazingTableView> table,
		std::shared_ptr<BlazingTableView> pivots,
		const std::vector<int> & searchColIndices,
		std::vector<cudf::order> sortOrderTypes);

	std::unique_ptr<BlazingTable> getPivotPointsTable(cudf::size_type number_pivots, std::shared_ptr<BlazingTableView> sortedSamples);

	std::unique_ptr<BlazingTable> sortedMerger(std::vector<BlazingTableView> & tables,
		const std::vector<cudf::order> & sortOrderTypes, const std::vector<int> & sortColIndices);
	
struct sorted_merger_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices) const
  {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_merger_functor::operator()<ral::frame::BlazingArrowTable>(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices) const
{
  // TODO percy arrow
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_merger_functor::operator()<ral::frame::BlazingCudfTable>(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices) const
{
	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);

	std::vector<cudf::table_view> cudf_table_views(tables.size());
	for(size_t i = 0; i < tables.size(); i++) {
		cudf_table_views[i] = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tables[i])->view();
	}
	std::unique_ptr<cudf::table> merged_table = cudf::merge(cudf_table_views, sortColIndices, sortOrderTypes, null_orders);

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < tables.size(); i++) {
		if (tables[i]->column_names().size() > 0){
			names = tables[i]->column_names();
			break;
		}
	}
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(merged_table), names);
}

struct gather_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices) const
  {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> gather_functor::operator()<ral::frame::BlazingArrowTable>(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices) const
{
  // TODO percy arrow
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> gather_functor::operator()<ral::frame::BlazingCudfTable>(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices) const
{
  // TODO percy rommel arrow
//	std::unique_ptr<cudf::table> pivots = cudf::detail::gather( sortedSamples->view(), gather_map->view(), cudf::out_of_bounds_policy::DONT_CHECK, cudf::detail::negative_index_policy::NOT_ALLOWED );


//	// TODO this is just a default setting. Will want to be able to properly set null_order
//	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);

//	std::vector<cudf::table_view> cudf_table_views(tables.size());
//	for(size_t i = 0; i < tables.size(); i++) {
//		cudf_table_views[i] = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tables[i])->view();
//	}
//	std::unique_ptr<cudf::table> merged_table = cudf::merge(cudf_table_views, sortColIndices, sortOrderTypes, null_orders);

//	// lets get names from a non-empty table
//	std::vector<std::string> names;
//	for(size_t i = 0; i < tables.size(); i++) {
//		if (tables[i]->column_names().size() > 0){
//			names = tables[i]->column_names();
//			break;
//		}
//	}
//	return std::make_unique<ral::frame::BlazingTable>(std::move(merged_table), names);
}

}  // namespace distribution
}  // namespace ral
