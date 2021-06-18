#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_COMMON_METADATA_CPP_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_COMMON_METADATA_CPP_H_

#include "orc_metadata.h"
#include "blazing_table/BlazingCudfTable.h"

// TODO percy arrow delete this include cudf details should never he here
#include "compute/cudf/detail/types.h"
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

std::unique_ptr<cudf::column> make_cudf_column_from_vector(cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size) {
	size_t width_per_value = cudf::size_of(dtype);
	if (vector.size() != 0) {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(vector.data(), buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, std::move(gpu_buffer));
	} else {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, buffer_size);
	}
}

std::shared_ptr<arrow::Array> make_arrow_array_from_vector(std::shared_ptr<arrow::DataType> dtype, std::vector<int64_t> &vector){
	std::shared_ptr<arrow::Array> array;
	std::unique_ptr<arrow::ArrayBuilder> builder = MakeArrayBuilder(dtype);

	AppendValues<int64_t>(vector, builder, dtype);
	builder->Finish(&array);
	return array;
}

std::basic_string<char> get_typed_vector_content(cudf::type_id dtype, std::vector<int64_t> &vector) {
  std::basic_string<char> output;
  switch (dtype) {
	case cudf::type_id::INT8:{
		std::vector<char> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(char));
		break;
	}
	case cudf::type_id::UINT8:{
		std::vector<uint8_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(uint8_t));
		break;
	}
	case cudf::type_id::INT16: {
		std::vector<int16_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int16_t));
		break;
	}
	case cudf::type_id::UINT16:{
		std::vector<uint16_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(uint16_t));
		break;
	}
	case cudf::type_id::INT32:{
		std::vector<int32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int32_t));
		break;
	}
	case cudf::type_id::UINT32:{
		std::vector<uint32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(uint32_t));
		break;
	}
	case cudf::type_id::INT64: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::UINT64: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(uint64_t));
		break;
	}
	case cudf::type_id::FLOAT32: {
		std::vector<float> typed_v(vector.size());
		for(size_t I=0;I<vector.size();I++){
			typed_v[I] = *(reinterpret_cast<float*>(&(vector[I])));
		}
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(float));
		break;
	}
	case cudf::type_id::FLOAT64: {
		double* casted_metadata = reinterpret_cast<double*>(&(vector[0]));
		output = std::basic_string<char>((char *)casted_metadata, vector.size() * sizeof(double));
		break;
	}
	case cudf::type_id::BOOL8: {
		std::vector<int8_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int8_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_DAYS: {
		std::vector<int32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int32_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_SECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_MILLISECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_MICROSECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_NANOSECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	default: {
		// default return type since we're throwing an exception.
		std::cerr << "Invalid gdf_dtype in create_host_column" << std::endl;
		throw std::runtime_error("Invalid gdf_dtype in create_host_column");
	}
  }
  return output;
}

#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_COMMON_METADATA_CPP_H_
