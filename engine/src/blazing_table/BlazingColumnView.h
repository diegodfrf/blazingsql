#pragma once

#include "blazing_table/BlazingColumn.h"
namespace ral {
namespace frame {
class BlazingColumnView : public BlazingColumn {
	public:
		BlazingColumnView() =default;
		BlazingColumnView(const BlazingColumn&) =delete;
		BlazingColumnView& operator=(const BlazingColumnView&) =delete;
		BlazingColumnView(const cudf::column_view & column) : column(column) {};
		~BlazingColumnView() = default;
		cudf::column_view view() const {
			return column;
		}
		// release of a BlazingColumnView will make a copy since its not the owner and therefore cannot transfer ownership
		std::unique_ptr<cudf::column> release() { return std::make_unique<cudf::column>(column); }
		blazing_column_type type() { return blazing_column_type::VIEW; }
		
	private:
		cudf::column_view column;
};

}  // namespace frame
}  // namespace ral