#ifndef METADATA_H_
#define METADATA_H_

#include "blazing_table/BlazingTable.h"

std::unique_ptr<ral::frame::BlazingTable> make_dummy_metadata_table_from_col_names(std::vector<std::string> col_names);

std::unique_ptr<cudf::column> make_cudf_column_from_vector(
	cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size);

std::shared_ptr<arrow::Array> make_arrow_array_from_vector(
	std::shared_ptr<arrow::DataType> dtype, std::vector<int64_t> &vector);

std::unique_ptr<cudf::column> make_empty_column(cudf::data_type type);

std::basic_string<char> get_typed_vector_content(
	cudf::type_id dtype, std::vector<int64_t> &vector);

#endif	// METADATA_H_
