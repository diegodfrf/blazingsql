#pragma once

#include "blazing_table/BlazingTable.h"

bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables);
bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & tables);
std::unique_ptr<ral::frame::BlazingTable> concatTables(const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables);
