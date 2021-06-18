#include "compute/cudf/detail/search.h"
#include <cudf/merge.hpp>

std::unique_ptr<ral::frame::BlazingTable> sorted_merger(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables,
                   const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
                   const std::vector<int> & sortColIndices, const std::vector<voltron::compute::NullOrder> & sortOrderNulls)
{
  std::vector<cudf::table_view> cudf_table_views(tables.size());
	for(size_t i = 0; i < tables.size(); i++) {
		cudf_table_views[i] = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tables[i])->view();
	}

	std::vector<cudf::order> cudfOrderTypes = voltron::compute::cudf_backend::types::toCudfOrderTypes(sortOrderTypes);
	std::vector<cudf::null_order> cudfNullOrderTypes = voltron::compute::cudf_backend::types::toCudfNullOrderTypes(sortOrderNulls);
	std::unique_ptr<cudf::table> merged_table = cudf::merge(cudf_table_views, sortColIndices, cudfOrderTypes, cudfNullOrderTypes);

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
