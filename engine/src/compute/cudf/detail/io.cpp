#include "compute/cudf/detail/io.h"

#include <sys/types.h>

#include <numeric>

#include <cudf/io/parquet.hpp>
#include <cudf/io/datasource.hpp>
#include <cudf/io/csv.hpp>

#include <blazingdb/io/Library/Logging/Logger.h>

#include "io/data_parser/ArgsUtil.h"

namespace voltron {
namespace compute {
namespace cudf_backend {
namespace io {

cudf::io::csv_reader_options getCsvReaderOptions(const std::map<std::string, std::string> & args, cudf::io::arrow_io_source & arrow_source) {

	cudf::io::csv_reader_options reader_opts = cudf::io::csv_reader_options::builder(cudf::io::source_info{&arrow_source});
	if(ral::io::map_contains("compression", args)) {
		reader_opts.set_compression((cudf::io::compression_type) ral::io::to_int(args.at("compression")));
	}
	if(ral::io::map_contains("lineterminator", args)) {
		reader_opts.set_lineterminator(ral::io::ord(args.at("lineterminator")));
	}
	if(ral::io::map_contains("delimiter", args)) {
		reader_opts.set_delimiter(ral::io::ord(args.at("delimiter")));
	}
	if(ral::io::map_contains("windowslinetermination", args)) {
		reader_opts.enable_windowslinetermination(ral::io::to_bool(args.at("windowslinetermination")));
	}
	if(ral::io::map_contains("delim_whitespace", args)) {
		reader_opts.enable_delim_whitespace(ral::io::to_bool(args.at("delim_whitespace")));
	}
	if(ral::io::map_contains("skipinitialspace", args)) {
		reader_opts.enable_skipinitialspace(ral::io::to_bool(args.at("skipinitialspace")));
	}
	if(ral::io::map_contains("skip_blank_lines", args)) {
		reader_opts.enable_skip_blank_lines(ral::io::to_bool(args.at("skip_blank_lines")));
	}
	if(ral::io::map_contains("nrows", args)) {
		reader_opts.set_nrows((cudf::size_type) ral::io::to_int(args.at("nrows")));
	}
	if(ral::io::map_contains("skiprows", args)) {
		reader_opts.set_skiprows((cudf::size_type) ral::io::to_int(args.at("skiprows")));
	}
	if(ral::io::map_contains("skipfooter", args)) {
		reader_opts.set_skipfooter((cudf::size_type) ral::io::to_int(args.at("skipfooter")));
	}
	if(ral::io::map_contains("names", args)) {
		reader_opts.set_names(ral::io::to_vector_string(args.at("names")));
		reader_opts.set_header(-1);
	} else {
		reader_opts.set_header(0);
	}
	if(ral::io::map_contains("header", args)) {
		reader_opts.set_header((cudf::size_type) ral::io::to_int(args.at("header")));
	}
	if(ral::io::map_contains("dtype", args)) {
		reader_opts.set_dtypes(ral::io::to_vector_string(args.at("dtype")));
	}
	if(ral::io::map_contains("use_cols_indexes", args)) {
		reader_opts.set_use_cols_indexes(ral::io::to_vector_int(args.at("use_cols_indexes")));
	}
	if(ral::io::map_contains("use_cols_names", args)) {
		reader_opts.set_use_cols_names(ral::io::to_vector_string(args.at("use_cols_names")));
	}
	if(ral::io::map_contains("true_values", args)) {
		reader_opts.set_true_values(ral::io::to_vector_string(args.at("true_values")));
	}
	if(ral::io::map_contains("false_values", args)) {
		reader_opts.set_false_values(ral::io::to_vector_string(args.at("false_values")));
	}
	if(ral::io::map_contains("na_values", args)) {
		reader_opts.set_na_values(ral::io::to_vector_string(args.at("na_values")));
	}
	if(ral::io::map_contains("keep_default_na", args)) {
		reader_opts.enable_keep_default_na(ral::io::to_bool(args.at("keep_default_na")));
	}
	if(ral::io::map_contains("na_filter", args)) {
		reader_opts.enable_na_filter(ral::io::to_bool(args.at("na_filter")));
	}
	if(ral::io::map_contains("prefix", args)) {
		reader_opts.set_prefix(args.at("prefix"));
	}
	if(ral::io::map_contains("mangle_dupe_cols", args)) {
		reader_opts.enable_mangle_dupe_cols(ral::io::to_bool(args.at("mangle_dupe_cols")));
	}
	if(ral::io::map_contains("dayfirst", args)) {
		reader_opts.enable_dayfirst(ral::io::to_bool(args.at("dayfirst")));
	}
	if(ral::io::map_contains("thousands", args)) {
		reader_opts.set_thousands(ral::io::ord(args.at("thousands")));
	}
	if(ral::io::map_contains("decimal", args)) {
		reader_opts.set_decimal(ral::io::ord(args.at("decimal")));
	}
	if(ral::io::map_contains("comment", args)) {
		reader_opts.set_comment(ral::io::ord(args.at("comment")));
	}
	if(ral::io::map_contains("quotechar", args)) {
		reader_opts.set_quotechar(ral::io::ord(args.at("quotechar")));
	}
	// if (map_contains("quoting", args)) {
	//    reader_opts.quoting = args.at("quoting"]
	if(ral::io::map_contains("doublequote", args)) {
		reader_opts.enable_doublequote(ral::io::to_bool(args.at("doublequote")));
	}
	if(ral::io::map_contains("byte_range_offset", args)) {
		reader_opts.set_byte_range_offset((size_t) ral::io::to_int(args.at("byte_range_offset")));
	}
	if(ral::io::map_contains("byte_range_size", args)) {
		reader_opts.set_byte_range_size((size_t) ral::io::to_int(args.at("byte_range_size")));
	}
	if(ral::io::map_contains("out_time_unit", args)) {
		// TODO
		// reader_opts.out_time_unit = args.at("out_time_unit");
	}
	return reader_opts;
}

std::unique_ptr<ral::frame::BlazingTable> read_parquet_file(
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

std::unique_ptr<ral::frame::BlazingTable> read_csv_file(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::vector<int> column_indices,
    std::vector<std::string> col_names,
    std::vector<cudf::size_type> row_groups,
    const std::map<std::string, std::string> &args_map)
{
  
  // copy column_indices into use_col_indexes (at the moment is ordered only)
  auto arrow_source = cudf::io::arrow_io_source{file};
  cudf::io::csv_reader_options args = getCsvReaderOptions(args_map, arrow_source);
  args.set_use_cols_indexes(column_indices);

  if (args.get_header() > 0) {
    args.set_header(args.get_header());
  } else if (args_map.at("has_header_csv") == "True") {
    args.set_header(0);
  } else {
    args.set_header(-1);
  }

  // Overrride `_byte_range_size` param to read first `max_bytes_chunk_read` bytes (note: always reads complete rows)
  auto iter = args_map.find("max_bytes_chunk_read");
  if(iter != args_map.end()) {
    auto chunk_size = std::stoll(iter->second);
    if (chunk_size > 0) {
      args.set_byte_range_offset(chunk_size * row_groups[0]);
      args.set_byte_range_size(chunk_size);
    }
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

void parse_parquet_schema(
    ral::io::Schema & schema_out,
    std::shared_ptr<arrow::io::RandomAccessFile> file)
{
  auto arrow_source = cudf::io::arrow_io_source{file};
	cudf::io::parquet_reader_options pq_args = cudf::io::parquet_reader_options::builder(cudf::io::source_info{&arrow_source});
	pq_args.enable_convert_strings_to_categories(false);
	pq_args.enable_use_pandas_metadata(false);
	pq_args.set_num_rows(1);  // we only need the metadata, so one row is fine
	cudf::io::table_with_metadata table_out = cudf::io::read_parquet(pq_args);

  for(int i = 0; i < table_out.tbl->num_columns(); i++) {
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		std::string name = table_out.metadata.column_names.at(i);
    size_t file_index = i;
		bool is_in_file = true;
		schema_out.add_column(
          name,
          type,
          file_index, is_in_file);
  }
}

void parse_csv_schema(
    ral::io::Schema & schema_out,
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    const std::map<std::string, std::string> &args_map)
{
  auto arrow_source = cudf::io::arrow_io_source{file};
  cudf::io::csv_reader_options args = getCsvReaderOptions(args_map, arrow_source);

	// if names were not passed when create_table
	if (args.get_header() == 0) {
		schema_out.set_has_header_csv(true);
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
		cudf::type_id type = table_out.tbl->get_column(i).type().id();
		size_t file_index = i;
		bool is_in_file = true;
		std::string name = table_out.metadata.column_names.at(i);
		schema_out.add_column(name, type, file_index, is_in_file);
	}
}

} // namespace io
} // namespace cudf_backend
} // namespace compute
} // namespace voltron
