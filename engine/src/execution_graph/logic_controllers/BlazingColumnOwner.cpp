#pragma once

#include "execution_graph/logic_controllers/BlazingColumnOwner.h"

namespace ral {

namespace frame {

BlazingColumnOwner::BlazingColumnOwner() 
	: column(nullptr) {}

BlazingColumnOwner::BlazingColumnOwner(std::unique_ptr<CudfColumn> column) 
	: column(std::move(column)) {}


}  // namespace frame

}  // namespace ral