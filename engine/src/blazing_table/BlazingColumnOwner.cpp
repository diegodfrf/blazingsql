#include "blazing_table/BlazingColumnOwner.h"

namespace ral {

namespace frame {

BlazingColumnOwner::BlazingColumnOwner(std::unique_ptr<cudf::column> column) 
	: column(std::move(column)) {}


}  // namespace frame

}  // namespace ral