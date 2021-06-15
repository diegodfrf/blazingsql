/*
 * CSVParser.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#include "CSVParser.h"
#include "parser/types_parser_utils.h"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <numeric>

#include <sys/types.h>

#include <blazingdb/io/Library/Logging/Logger.h>
#include "ArgsUtil.h"
#include "compute/api.h"

#define checkError(error, txt)                                                                                         \
	if(error != GDF_SUCCESS) {                                                                                         \
		std::cerr << "ERROR:  " << error << "  in " << txt << std::endl;                                               \
		return error;                                                                                                  \
	}

namespace ral {
namespace io {

csv_parser::csv_parser(std::map<std::string, std::string> args_map_) 
  : args_map{args_map_} {}

csv_parser::~csv_parser() {}

std::unique_ptr<ral::frame::BlazingTable> csv_parser::parse_batch(ral::execution::execution_backend preferred_compute,
	ral::io::data_handle handle,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups) {
	std::shared_ptr<arrow::io::RandomAccessFile> file = handle.file_handle;

	if(file == nullptr) {
    return ral::execution::backend_dispatcher(preferred_compute,
                                           create_empty_table_functor(),
                                           schema.get_names(), schema.get_dtypes(), column_indices);
	}

	if(column_indices.size() > 0) {
    return ral::execution::backend_dispatcher(
          preferred_compute,
          io_read_file_data_functor<ral::io::DataType::CSV>(),
          file, column_indices, schema.get_names(), row_groups, this->args_map);
	}
	return nullptr;
}

void csv_parser::parse_schema(ral::execution::execution_backend preferred_compute,
	ral::io::data_handle handle, ral::io::Schema & schema) {
  auto file = handle.file_handle;

  ral::execution::backend_dispatcher(
        preferred_compute,
        io_parse_file_schema_functor<ral::io::DataType::CSV>(),
        schema,
        file,
        this->args_map);
}

size_t csv_parser::max_bytes_chunk_size() const {
	auto iter = args_map.find("max_bytes_chunk_read");
	if(iter == args_map.end()) {
		return 0;
	}

	return std::stoll(iter->second);
}

int64_t GetFileSize(std::string filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? (int64_t)stat_buf.st_size : -1;
}

std::unique_ptr<ral::frame::BlazingTable> csv_parser::get_metadata(ral::execution::execution_backend preferred_compute,
	std::vector<ral::io::data_handle> handles, int offset,
	std::map<std::string, std::string> args_map)
{
	int64_t size_max_batch;
	if (args_map.find("max_bytes_chunk_read") != args_map.end()) {
		// At this level `size_max_batch` should be diff to 0
		size_max_batch = (int64_t)to_int(args_map.at("max_bytes_chunk_read"));
	}

	std::vector<int64_t> num_total_bytes_per_file(handles.size());
	std::vector<size_t> num_batches_per_file(handles.size());
	for (size_t i = 0; i < handles.size(); ++i) {
		num_total_bytes_per_file[i] = GetFileSize(handles[i].uri.toString(true));
		num_batches_per_file[i] = std::ceil((double) num_total_bytes_per_file[i] / size_max_batch);
	}
	
	std::vector<int> file_index_values, row_group_values;
	for (int i = 0; i < num_batches_per_file.size(); ++i) {
		for (int j = 0; j < num_batches_per_file[i]; ++j) {
			file_index_values.push_back(i + offset);
			row_group_values.push_back(j);
		}
	}

#ifdef CUDF_SUPPORT
	std::vector< std::unique_ptr<cudf::column> > columns;
	columns.emplace_back( vector_to_column(file_index_values, cudf::data_type(cudf::type_id::INT32)) );
	columns.emplace_back( vector_to_column(row_group_values, cudf::data_type(cudf::type_id::INT32)) );

	std::vector<std::string> metadata_names = {"file_handle_index", "row_group_index"};
	auto metadata_table = std::make_unique<cudf::table>(std::move(columns));

	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(metadata_table), metadata_names);
#endif
}

} /* namespace io */
} /* namespace ral */
