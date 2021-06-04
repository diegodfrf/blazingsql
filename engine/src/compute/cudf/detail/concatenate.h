#pragma once

#include "blazing_table/BlazingCudfTableView.h"

bool checkIfConcatenatingStringsWillOverflow_gpu(const std::vector<std::shared_ptr<ral::frame::BlazingCudfTableView>> & tables);
