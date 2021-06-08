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

#include <blazingdb/io/Library/Logging/Logger.h>
#include "ArgsUtil.h"

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

		// copy column_indices into use_col_indexes (at the moment is ordered only)
		auto arrow_source = cudf::io::arrow_io_source{file};
		cudf::io::csv_reader_options args = getCsvReaderOptions(args_map, arrow_source);
		args.set_use_cols_indexes(column_indices);

		if (args.get_header() > 0) {
			args.set_header(args.get_header());
		} else if (args_map["has_header_csv"] == "True") {
			args.set_header(0);
		} else {
			args.set_header(-1);
		}

		// Overrride `_byte_range_size` param to read first `max_bytes_chunk_read` bytes (note: always reads complete rows)
		auto iter = args_map.find("max_bytes_chunk_read");
		if(iter != args_map.end()) {
			auto chunk_size = std::stoll(iter->second);
			args.set_byte_range_size(chunk_size);
		}

		cudf::io::table_with_metadata csv_table = cudf::io::read_csv(args);

		if(csv_table.tbl->num_columns() <= 0)
			Library::Logging::Logger().logWarn("csv_parser::parse no columns were read");

		// column_indices may be requested in a specific order (not necessarily sorted), but read_csv will output the
		// columns in the sorted order, so we need to put them back into the order we want
		std::vector<size_t> idx(column_indices.size());
		std::iota(idx.begin(), idx.end(), 0);
		// sort indexes based on comparing values in column_indices
		std::sort(idx.begin(), idx.end(), [&column_indices](size_t i1, size_t i2) {
			return column_indices[i1] < column_indices[i2];
		});

		std::vector< std::unique_ptr<cudf::column> > columns_out;
		std::vector<std::string> column_names_out;

		columns_out.resize(column_indices.size());
		column_names_out.resize(column_indices.size());

		std::vector< std::unique_ptr<cudf::column> > table = csv_table.tbl->release();

		for(size_t i = 0; i < column_indices.size(); i++) {
			columns_out[idx[i]] = std::move(table[i]);
			column_names_out[idx[i]] = csv_table.metadata.column_names[i];
		}

		std::unique_ptr<cudf::table> cudf_tb = std::make_unique<cudf::table>(std::move(columns_out));
		return std::make_unique<ral::frame::BlazingCudfTable>(std::move(cudf_tb), column_names_out);
	}
	return nullptr;
}

void csv_parser::parse_schema(ral::execution::execution_backend preferred_compute,
	ral::io::data_handle handle, ral::io::Schema & schema) {

  auto file = handle.file_handle;
	auto arrow_source = cudf::io::arrow_io_source{file};
	cudf::io::csv_reader_options args = getCsvReaderOptions(args_map, arrow_source);

	// if names were not passed when create_table
	if (args.get_header() == 0) {
		schema.set_has_header_csv(true);
	}

	int64_t num_bytes = file->GetSize().ValueOrDie();

	// lets only read up to 48192 bytes. We are assuming that a full row will always be less than that
	if(num_bytes > 48192) {
		args.set_nrows(1);
		args.set_skipfooter(0);
	}
	cudf::io::table_with_metadata table_out = cudf::io::read_csv(args);
	file->Close();

	for(int i = 0; i < table_out.tbl->num_columns(); i++) {
		arrow::Type::type type = cudf_type_id_to_arrow_type(table_out.tbl->get_column(i).type().id());
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = table_out.metadata.column_names.at(i);
		schema.add_column(name, type, file_index, is_in_file);
	}
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
