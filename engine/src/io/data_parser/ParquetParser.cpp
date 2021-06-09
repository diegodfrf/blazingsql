
#include "metadata/parquet_metadata.h"

#include "ParquetParser.h"
#include "utilities/CommonOperations.h"

#include <numeric>

#include <arrow/io/file.h>
#include "ExceptionHandling/BlazingThread.h"

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <arrow/io/api.h>
#include <cudf/detail/interop.hpp>
#include <cudf/io/parquet.hpp>

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

std::unique_ptr<ral::frame::BlazingTable> read_parquet_cudf(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::vector<int> column_indices,
    std::vector<std::string> col_names,
    std::vector<cudf::size_type> row_groups)
{
  // when we set `get_metadata=False` we need to send and empty full_row_groups
  std::vector<std::vector<cudf::size_type>> full_row_groups;
  if (row_groups.size() != 0) {
    full_row_groups = std::vector<std::vector<cudf::size_type>>(1, row_groups);
  }
  // Fill data to pq_args
  auto arrow_source = cudf_io::arrow_io_source{file};
  cudf_io::parquet_reader_options pq_args = cudf_io::parquet_reader_options::builder(cudf_io::source_info{&arrow_source});
  pq_args.enable_convert_strings_to_categories(false);
  pq_args.enable_use_pandas_metadata(false);
  pq_args.set_columns(col_names);
  pq_args.set_row_groups(full_row_groups);
  auto result = cudf::io::read_parquet(pq_args);
  auto result_table = std::move(result.tbl);
  if (result.metadata.column_names.size() > column_indices.size()) {
    auto columns = result_table->release();
    // Assuming columns are in the same order as column_indices and any extra columns (i.e. index column) are put last
    columns.resize(column_indices.size());
    result_table = std::make_unique<cudf::table>(std::move(columns));
  }
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(result_table), result.metadata.column_names);
}

std::unique_ptr<ral::frame::BlazingTable> read_parquet_arrow(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::vector<int> column_indices,
    std::vector<std::string> col_names,
    std::vector<cudf::size_type> row_groups)
{
  // Open Parquet file reader
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  auto st = parquet::arrow::OpenFile(file, pool, &arrow_reader);
  if (!st.ok()) {
    // TODO percy thrown error
  }

  std::vector<int> myrow_groups;
  myrow_groups.resize(row_groups.size());
  std::copy(row_groups.begin(), row_groups.end(), myrow_groups.begin());

  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> table;
  st = arrow_reader->ReadRowGroups(myrow_groups, column_indices, &table);
  if (!st.ok()) {
    // TODO percy thrown error
    // Handle error reading Parquet data...
  }

  // TODO percy arrow assert col names = table->fields

  if (table->schema()->fields().size() > column_indices.size()) {
    // TODO percy arrow
    //auto columns = result_table->release();
    // Assuming columns are in the same order as column_indices and any extra columns (i.e. index column) are put last
    //columns.resize(column_indices.size());
    //result_table = std::make_unique<cudf::table>(std::move(columns));
  }
  return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

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

		std::unique_ptr<ral::frame::BlazingTable> ret = nullptr;
    if (preferred_compute.id() == ral::execution::backend_id::CUDF) {
      ret = read_parquet_cudf(file, column_indices, col_names, row_groups);
    } else if (preferred_compute.id() == ral::execution::backend_id::ARROW) {
      ret = read_parquet_arrow(file, column_indices, col_names, row_groups);
    }
    return ret;
	}
	return nullptr;
}

std::vector<std::pair<std::string, cudf::type_id>> parse_schema_cudf(std::shared_ptr<arrow::io::RandomAccessFile> file) {
  auto arrow_source = cudf_io::arrow_io_source{file};
	cudf_io::parquet_reader_options pq_args = cudf_io::parquet_reader_options::builder(cudf_io::source_info{&arrow_source});
	pq_args.enable_convert_strings_to_categories(false);
	pq_args.enable_use_pandas_metadata(false);
	pq_args.set_num_rows(1);  // we only need the metadata, so one row is fine
	cudf_io::table_with_metadata table_out = cudf_io::read_parquet(pq_args);

  std::vector<std::pair<std::string, cudf::type_id>> ret;
  ret.resize(table_out.tbl->num_columns());
  for(int i = 0; i < table_out.tbl->num_columns(); i++) {
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		std::string name = table_out.metadata.column_names.at(i);
    ret[i] = std::make_pair(name, type);
  }
  return ret;
}

std::vector<std::pair<std::string, cudf::type_id>> parse_schema_arrow(std::shared_ptr<parquet::FileMetaData> parquet_metadata) {
  std::shared_ptr<::arrow::Schema> arrow_schema;
  parquet::ArrowReaderProperties props;
  // TODO percy arrow handle error
  parquet::arrow::FromParquetSchema(parquet_metadata->schema(), props, &arrow_schema);
  std::vector<std::pair<std::string, cudf::type_id>> ret;
  ret.resize(arrow_schema->fields().size());
  for(int i = 0; i < arrow_schema->fields().size(); i++) {
		cudf::type_id type = cudf::detail::arrow_to_cudf_type(*arrow_schema->field(i)->type()).id();
		std::string name = arrow_schema->field(i)->name();
    ret[i] = std::make_pair(name, type);
  }
  return ret;
}

void parquet_parser::parse_schema(ral::execution::execution_backend preferred_compute,ral::io::data_handle handle, ral::io::Schema & schema) {
  auto file = handle.file_handle;
	auto parquet_reader = parquet::ParquetFileReader::Open(file);
	if (parquet_reader->metadata()->num_rows() == 0) {
		parquet_reader->Close();
		return; // if the file has no rows, we dont want cudf_io to try to read it
	}

  std::vector<std::pair<std::string, cudf::type_id>> fields;
  if (preferred_compute.id() == ral::execution::backend_id::CUDF) {
    fields = parse_schema_cudf(file);
  } else if (preferred_compute.id() == ral::execution::backend_id::ARROW) {
    fields = parse_schema_arrow(parquet_reader->metadata());
  }

	for(int i = 0; i < fields.size(); ++i) {
		size_t file_index = i;
		bool is_in_file = true;
		schema.add_column(fields[i].first, fields[i].second, file_index, is_in_file);
	}
}

std::unique_ptr<ral::frame::BlazingTable> parquet_parser::get_metadata(ral::execution::execution_backend preferred_compute,
	std::vector<ral::io::data_handle> handles, int offset,
	std::map<std::string, std::string> args_map)
{
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
