#include "BlazingArrowTable.h"

#ifdef CUDF_SUPPORT
#include "blazing_table/BlazingCudfTable.h"
#endif

namespace ral {
namespace frame {

BlazingArrowTable::BlazingArrowTable(std::shared_ptr<arrow::Table> arrow_table)
  : BlazingTable(execution::backend_id::ARROW, true)
	, BlazingArrowTableView(arrow_table) {
}

#ifdef CUDF_SUPPORT
BlazingArrowTable::BlazingArrowTable(std::unique_ptr<BlazingCudfTable> blazing_cudf_table)
  : BlazingTable(execution::backend_id::ARROW, true), BlazingArrowTableView(nullptr) {

	std::vector<cudf::column_metadata> arrow_metadata(blazing_cudf_table->num_columns());
	for(int i = 0; i < blazing_cudf_table->num_columns(); ++i){
		arrow_metadata[i].name = blazing_cudf_table->column_names()[i];
	}
	// TODO this also takes in an arrow::MemoryPool.
	this->arrow_table = cudf::to_arrow(blazing_cudf_table->view(), arrow_metadata);
}
#endif

std::unique_ptr<BlazingTable> BlazingArrowTable::clone() const {
  return this->to_table_view()->clone();
}

std::unique_ptr<BlazingArrowTable> BlazingArrowTable::clone() {
  return this->to_table_view()->clone();
}

std::shared_ptr<ral::frame::BlazingTableView> BlazingArrowTable::to_table_view() const {
	return std::make_shared<BlazingArrowTableView>(BlazingArrowTableView::view());
}

std::shared_ptr<BlazingArrowTableView> BlazingArrowTable::to_table_view() {
	return std::make_shared<BlazingArrowTableView>(BlazingArrowTableView::view());
}

}  // namespace frame
}  // namespace ral
