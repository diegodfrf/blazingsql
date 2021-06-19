#ifndef METADATA_H_
#define METADATA_H_

#include "blazing_table/BlazingTable.h"

std::unique_ptr<ral::frame::BlazingTable> make_dummy_metadata_table_from_col_names(std::vector<std::string> col_names);

std::shared_ptr<arrow::Array> make_arrow_array_from_vector(
	std::shared_ptr<arrow::DataType> dtype, std::vector<int64_t> &vector);

#endif	// METADATA_H_
