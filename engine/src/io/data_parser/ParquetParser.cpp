
#include "metadata/parquet_metadata.h"

#include "ParquetParser.h"

#include <numeric>

#include "ExceptionHandling/BlazingThread.h"
#include "compute/api.h"

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

parquet_parser::parquet_parser(){
	// TODO Auto-generated constructor stub
}

parquet_parser::~parquet_parser() {
	// TODO Auto-generated destructor stub
}

std::unique_ptr<ral::frame::BlazingTable> parquet_parser::parse_batch(
    ral::execution::execution_backend preferred_compute,
	ral::io::data_handle handle,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups)
{
	std::shared_ptr<arrow::io::RandomAccessFile> file = handle.file_handle;
	if(file == nullptr) {
		return schema.makeEmptyBlazingCudfTable(column_indices);
	}
	if(column_indices.size() > 0) {
    std::vector<std::string> col_names(column_indices.size());
		for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
			col_names[column_i] = schema.get_name(column_indices[column_i]);
		}

    return ral::execution::backend_dispatcher(
          preferred_compute,
          io_read_file_data_functor<ral::io::DataType::PARQUET>(),
          file, column_indices, col_names, row_groups);
	}
	return nullptr;
}

void parquet_parser::parse_schema(ral::execution::execution_backend preferred_compute,ral::io::data_handle handle, ral::io::Schema & schema) {
  auto file = handle.file_handle;
	auto parquet_reader = parquet::ParquetFileReader::Open(file);
	if (parquet_reader->metadata()->num_rows() == 0) {
		parquet_reader->Close();
		return; // if the file has no rows, we dont want cudf_io to try to read it
	}

  auto fields = ral::execution::backend_dispatcher(
                  preferred_compute,
                  io_read_file_schema_functor<ral::io::DataType::PARQUET>(),
                  file);

	for(int i = 0; i < fields.size(); ++i) {
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(fields[i].first, fields[i].second, file_index, is_in_file);
	}
}

std::unique_ptr<ral::frame::BlazingTable> parquet_parser::get_metadata(ral::execution::execution_backend preferred_compute,
	std::vector<ral::io::data_handle> handles, int offset){
  // TODO percy arrow
	std::vector<size_t> num_row_groups(handles.size());
	BlazingThread threads[handles.size()];
	std::vector<std::unique_ptr<parquet::ParquetFileReader>> parquet_readers(handles.size());
	for(size_t file_index = 0; file_index < handles.size(); file_index++) {
		threads[file_index] = BlazingThread([&, file_index]() {
		  parquet_readers[file_index] =
			  std::move(parquet::ParquetFileReader::Open(handles[file_index].file_handle));
		  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_readers[file_index]->metadata();
		  num_row_groups[file_index] = file_metadata->num_row_groups();
		});
	}

	for(size_t file_index = 0; file_index < handles.size(); file_index++) {
		threads[file_index].join();
	}

	size_t total_num_row_groups =
		std::accumulate(num_row_groups.begin(), num_row_groups.end(), size_t(0));

	auto minmax_metadata_table = get_minmax_metadata(parquet_readers, total_num_row_groups, offset);
	for (auto &reader : parquet_readers) {
		reader->Close();
	}
	return minmax_metadata_table;
}

} /* namespace io */
} /* namespace ral */
