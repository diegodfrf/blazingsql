#pragma once

// Types: table, column and scalar builders with fixed types/schema

#include <arrow/chunked_array.h>
#include "blazing_table/BlazingTable.h"

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(std::shared_ptr<ral::frame::BlazingTableView> table);

void normalize_types(std::unique_ptr<ral::frame::BlazingTable> & table,  const std::vector<cudf::data_type> & types,
	 		std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );
