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
		cudf::size_type number_partitions,
		const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<cudf::null_order> & sortOrderNulls);

	std::unique_ptr<BlazingTable> getPartitionPlan(Context * context);

// This function locates the pivots in the table and partitions the data on those pivot points.
// IMPORTANT: This function expects data to aready be sorted according to the searchColIndices, sortOrderTypes and sortOrderNulls
// IMPORTANT: The TableViews of the data returned point to the same data that was input.
	std::vector<NodeColumnView> partitionData(Context * context,
		std::shared_ptr<BlazingTableView> table,
		std::shared_ptr<BlazingTableView> pivots,
		const std::vector<int> & searchColIndices,
		std::vector<cudf::order> sortOrderTypes,
		const std::vector<cudf::null_order> & sortOrderNulls);

	std::unique_ptr<BlazingTable> getPivotPointsTable(cudf::size_type number_pivots, std::shared_ptr<BlazingTableView> sortedSamples);

struct sorted_merger_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices,
      const std::vector<cudf::null_order> & sortOrderNulls) const
  {
    throw std::runtime_error("ERROR: sorted_merger_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_merger_functor::operator()<ral::frame::BlazingArrowTable>(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices, const std::vector<cudf::null_order> & sortOrderNulls) const
{
  throw std::runtime_error("ERROR: sorted_merger_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_merger_functor::operator()<ral::frame::BlazingCudfTable>(
		std::vector<std::shared_ptr<BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices, const std::vector<cudf::null_order> & sortOrderNulls) const
{
	std::vector<cudf::table_view> cudf_table_views(tables.size());
	for(size_t i = 0; i < tables.size(); i++) {
		cudf_table_views[i] = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tables[i])->view();
	}
	std::unique_ptr<cudf::table> merged_table = cudf::merge(cudf_table_views, sortColIndices, sortOrderTypes, sortOrderNulls);

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
		std::shared_ptr<BlazingTableView> table,
		std::unique_ptr<cudf::column> column,
		cudf::out_of_bounds_policy out_of_bounds_policy,
		cudf::detail::negative_index_policy negative_index_policy) const
  {
    throw std::runtime_error("ERROR: gather_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};


}  // namespace distribution
}  // namespace ral
