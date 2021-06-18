
#include "orc_metadata.h"

#include <iostream>

#include "blazing_table/BlazingArrowTable.h"
#include "compute/arrow/detail/types.h"
#include <arrow/array/builder_primitive.h>


std::unique_ptr<ral::frame::BlazingTable> make_dummy_metadata_table_from_col_names(std::vector<std::string> col_names) {
	const int ncols = col_names.size();
	std::vector<std::string> metadata_col_names;
	// + 2: due to `file_handle_index` and `stripe_index` columns
	metadata_col_names.resize(ncols * 2 + 2);

	int metadata_col_index = -1;
	for (int colIndex = 0; colIndex < ncols; ++colIndex){
		std::string col_name = col_names[colIndex];
		std::string col_name_min = "min_" + std::to_string(colIndex) + "_" + col_name;
		std::string col_name_max = "max_" + std::to_string(colIndex)  + "_" + col_name;

		metadata_col_names[++metadata_col_index] = col_name_min;
		metadata_col_names[++metadata_col_index] = col_name_max;
	}

	metadata_col_names[++metadata_col_index] = "file_handle_index";
	metadata_col_names[++metadata_col_index] = "row_group_index";  // as `stripe_index` when ORC

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.reserve(metadata_col_names.size());

	std::vector<std::shared_ptr<arrow::Field>> fields;
	fields.reserve(metadata_col_names.size());

	for (std::size_t i = 0; i < metadata_col_names.size(); ++i) {
		arrow::Int32Builder int32Builder;
		int32Builder.Append(-1);
		std::shared_ptr<arrow::Array> int32Array;
		int32Builder.Finish(&int32Array);
		arrays.emplace_back(int32Array);

		fields.emplace_back(arrow::field(metadata_col_names[i], arrow::int32()));
	}

	std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
	std::shared_ptr<arrow::Table> metadata_table = arrow::Table::Make(schema, arrays);

	return std::make_unique<ral::frame::BlazingArrowTable>(metadata_table);
}

std::shared_ptr<arrow::Array> make_arrow_array_from_vector(std::shared_ptr<arrow::DataType> dtype, std::vector<int64_t> &vector){
	std::shared_ptr<arrow::Array> array;
	std::unique_ptr<arrow::ArrayBuilder> builder = MakeArrayBuilder(dtype);

	AppendValues<int64_t>(vector, builder, dtype);
	builder->Finish(&array);
	return array;
}
