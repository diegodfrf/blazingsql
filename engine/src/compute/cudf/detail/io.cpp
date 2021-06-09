#include "compute/cudf/detail/io.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/datasource.hpp>

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
  auto arrow_source = cudf::io::arrow_io_source{file};
  cudf::io::parquet_reader_options pq_args = cudf::io::parquet_reader_options::builder(cudf::io::source_info{&arrow_source});
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

std::vector<std::pair<std::string, cudf::type_id>> parse_schema_cudf(std::shared_ptr<arrow::io::RandomAccessFile> file) {
  auto arrow_source = cudf::io::arrow_io_source{file};
	cudf::io::parquet_reader_options pq_args = cudf::io::parquet_reader_options::builder(cudf::io::source_info{&arrow_source});
	pq_args.enable_convert_strings_to_categories(false);
	pq_args.enable_use_pandas_metadata(false);
	pq_args.set_num_rows(1);  // we only need the metadata, so one row is fine
	cudf::io::table_with_metadata table_out = cudf::io::read_parquet(pq_args);

  std::vector<std::pair<std::string, cudf::type_id>> ret;
  ret.resize(table_out.tbl->num_columns());
  for(int i = 0; i < table_out.tbl->num_columns(); i++) {
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		std::string name = table_out.metadata.column_names.at(i);
    ret[i] = std::make_pair(name, type);
  }
  return ret;
}
