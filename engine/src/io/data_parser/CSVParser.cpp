/*
 * CSVParser.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#include "CSVParser.h"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <numeric>
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
		return schema.makeEmptyBlazingCudfTable(column_indices);
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
        file);
}

size_t csv_parser::max_bytes_chunk_size() const {
	auto iter = args_map.find("max_bytes_chunk_read");
	if(iter == args_map.end()) {
		return 0;
	}

	return std::stoll(iter->second);
}

} /* namespace io */
} /* namespace ral */
