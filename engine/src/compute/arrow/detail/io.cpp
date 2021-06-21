#include "compute/arrow/detail/io.h"

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>

#include "io/data_parser/ArgsUtil.h"
#include "parser/types_parser_utils.h"

#include <arrow/csv/api.h>
#include <arrow/json/api.h>
#include <arrow/adapters/orc/adapter.h>

namespace voltron {
namespace compute {
namespace arrow_backend {
namespace io {

void getCsvReaderOptions(const std::map<std::string, std::string> &args,
                         arrow::csv::ReadOptions &read_options,
                         arrow::csv::ParseOptions &parse_options,
                         arrow::csv::ConvertOptions &convert_options)
{
    if(ral::io::map_contains("delimiter", args)) {
        parse_options.delimiter = ral::io::ord(args.at("delimiter"));
    }
    if(ral::io::map_contains("windowslinetermination", args)) {
        parse_options.newlines_in_values = ral::io::to_bool(args.at("windowslinetermination"));
    }
    if(ral::io::map_contains("skip_blank_lines", args)) {
        parse_options.ignore_empty_lines = ral::io::to_bool(args.at("skip_blank_lines"));
    }
    if(ral::io::map_contains("skiprows", args)) {
        read_options.skip_rows = ral::io::to_int(args.at("skiprows"));
    }
    if(ral::io::map_contains("names", args)) {
        read_options.column_names = ral::io::to_vector_string(args.at("names"));
    }
    else
    {
        read_options.autogenerate_column_names = true;
    }
    if(ral::io::map_contains("dtype", args)) {
        auto dtypes = ral::io::to_vector_string(args.at("dtype"));
        for(std::size_t i = 0; i < read_options.column_names.size(); ++i){
            std::string name = read_options.column_names.at(i);
            std::shared_ptr<arrow::DataType> datatype = string_to_arrow_datatype(dtypes.at(i));
            convert_options.column_types.insert({name, datatype});
        }
    }
    if(ral::io::map_contains("true_values", args)) {
        convert_options.true_values = ral::io::to_vector_string(args.at("true_values"));
    }
    if(ral::io::map_contains("false_values", args)) {
        convert_options.false_values = ral::io::to_vector_string(args.at("false_values"));
    }
    if(ral::io::map_contains("na_values", args)) {
        convert_options.null_values = ral::io::to_vector_string(args.at("na_values"));
        convert_options.strings_can_be_null = true;
    }
    if(ral::io::map_contains("quotechar", args)) {
        parse_options.quote_char = ral::io::ord(args.at("quotechar"));
        parse_options.quoting = true;
    }
    if(ral::io::map_contains("doublequote", args)) {
        parse_options.double_quote = ral::io::to_bool(args.at("doublequote"));
        parse_options.quoting = true;
    }
}

std::unique_ptr<ral::frame::BlazingTable> read_parquet_file(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::vector<int> column_indices,
    std::vector<std::string> /*col_names*/,
    std::vector<int> row_groups)
{
  // Open Parquet file reader
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  arrow::Status st = parquet::arrow::OpenFile(file, pool, &arrow_reader);
  if (!st.ok()) {
      throw std::runtime_error("Error read_parquet_file: " + st.message());
  }

  std::vector<int> myrow_groups;
  myrow_groups.resize(row_groups.size());
  std::copy(row_groups.begin(), row_groups.end(), myrow_groups.begin());

  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> table;

  // TODO percy arrow rommel skip data use group info here  
  //st = arrow_reader->ReadRowGroups(myrow_groups, column_indices, &table);
  st = arrow_reader->ReadTable(column_indices, &table);

  if (!st.ok()) {
      throw std::runtime_error("Error read_parquet_file: " + st.message());
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

std::unique_ptr<ral::frame::BlazingTable> read_csv_file(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> /*column_indices*/,
        std::vector<std::string> /*col_names*/,
        std::vector<int> /*row_groups*/,
        const std::map<std::string, std::string> &args_map)
{
    arrow::io::IOContext io_context;

    arrow::csv::ReadOptions    read_options;
    arrow::csv::ParseOptions   parse_options;
    arrow::csv::ConvertOptions convert_options;

    getCsvReaderOptions(args_map, read_options, parse_options, convert_options);

    auto maybe_reader =
            arrow::csv::TableReader::Make(io_context,
                                          file,
                                          read_options,
                                          parse_options,
                                          convert_options);
    if (!maybe_reader.ok()) {
        throw std::runtime_error("Error read_csv_file: " + maybe_reader.status().message());
    }
    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
        throw std::runtime_error("Error read_csv_file: " + maybe_table.status().message());
    }
    std::shared_ptr<arrow::Table> table = *maybe_table;
    return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

std::unique_ptr<ral::frame::BlazingTable> read_orc_file(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> /*column_indices*/,
        std::vector<std::string> /*col_names*/,
        std::vector<int> /*row_groups*/)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader;
    arrow::Status st = arrow::adapters::orc::ORCFileReader::Open(file, pool, &reader);
    if(!st.ok()){
        throw std::runtime_error("Error read_orc_file: " + st.message());
    }

    std::shared_ptr<arrow::Table> table;
    st = reader->Read(&table);
    if (!st.ok()) {
        throw std::runtime_error("Error read_orc_file: " + st.message());
    }

    return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

std::unique_ptr<ral::frame::BlazingTable> read_json_file(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> /*column_indices*/,
        std::vector<std::string> /*col_names*/,
        std::vector<int> /*row_groups*/)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    arrow::json::ReadOptions  read_options;
    arrow::json::ParseOptions parse_options;

    auto maybe_reader =
            arrow::json::TableReader::Make(pool,
                                           file,
                                           read_options,
                                           parse_options);
    if (!maybe_reader.ok()) {
        throw std::runtime_error("Error read_json_file: " + maybe_reader.status().message());
    }
    std::shared_ptr<arrow::json::TableReader> reader = *maybe_reader;

    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
        throw std::runtime_error("Error read_json_file: " + maybe_table.status().message());
    }
    std::shared_ptr<arrow::Table> table = *maybe_table;
    return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

void parse_parquet_schema(
    ral::io::Schema & schema_out,
    std::shared_ptr<arrow::io::RandomAccessFile> file)
{
  // TODO percy arrow move common parquet reader code here (avoid open the file twice)
  auto parquet_reader = parquet::ParquetFileReader::Open(file);
  std::shared_ptr<parquet::FileMetaData> parquet_metadata = parquet_reader->metadata();
  std::shared_ptr<::arrow::Schema> arrow_schema;
  parquet::ArrowReaderProperties props;

  arrow::Status st = parquet::arrow::FromParquetSchema(parquet_metadata->schema(), props, &arrow_schema);
  if(!st.ok()){
      throw std::runtime_error("Error parse_parquet_schema: " + st.message());
  }

  for(std::size_t i = 0; i < arrow_schema->fields().size(); ++i) {
      std::string name = arrow_schema->field(i)->name();
      size_t file_index = i;
      bool is_in_file = true;
      schema_out.add_column(
              name,
              arrow_schema->field(i)->type(),
              file_index, is_in_file);
  }
}

void parse_csv_schema(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map)
{
    arrow::io::IOContext io_context;

    arrow::csv::ReadOptions    read_options;
    arrow::csv::ParseOptions   parse_options;
    arrow::csv::ConvertOptions convert_options;

    getCsvReaderOptions(args_map, read_options, parse_options, convert_options);

    auto maybe_reader =
            arrow::csv::TableReader::Make(io_context,
                                          file,
                                          read_options,
                                          parse_options,
                                          convert_options);
    if (!maybe_reader.ok()) {
        throw std::runtime_error("Error parse_csv_schema: " + maybe_reader.status().message());
    }
    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
        throw std::runtime_error("Error parse_csv_schema: " + maybe_table.status().message());
    }
    std::shared_ptr<arrow::Table> table   = *maybe_table;
    std::shared_ptr<arrow::Schema> schema = table->schema();

    std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();

    for(std::size_t i = 0; i < fields.size(); ++i)
    {
        std::string name       = fields[i]->name();
        auto type = fields[i]->type();
        size_t file_index      = i;
        bool is_in_file        = true;

        schema_out.add_column(name, type, file_index, is_in_file);
    }
}

void parse_orc_schema(
        ral::io::Schema &schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &/*args_map*/)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader;
    arrow::Status st = arrow::adapters::orc::ORCFileReader::Open(file, pool, &reader);
    if(!st.ok()){
        throw std::runtime_error("Error parse_orc_schema: " + st.message());
    }

    std::shared_ptr<arrow::Schema> schema;
    st = reader->ReadSchema(&schema);
    if (!st.ok()) {
        throw std::runtime_error("Error parse_orc_schema: " + st.message());
    }

    std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();

    for(std::size_t i = 0; i < fields.size(); ++i)
    {
        std::string name       = fields[i]->name();
        auto type = fields[i]->type();
        size_t file_index      = i;
        bool is_in_file        = true;

        schema_out.add_column(name, type, file_index, is_in_file);
    }
}

void parse_json_schema(
        ral::io::Schema &schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &/*args_map*/)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    arrow::json::ReadOptions read_options;
    arrow::json::ParseOptions parse_options;

    auto maybe_reader = arrow::json::TableReader::Make(pool,
                                                       file,
                                                       read_options,
                                                       parse_options);
    if (!maybe_reader.ok()) {
        throw std::runtime_error("Error parse_json_schema: " + maybe_reader.status().message());
    }
    std::shared_ptr<arrow::json::TableReader> reader = *maybe_reader;

    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
        throw std::runtime_error("Error parse_json_schema: " + maybe_table.status().message());
    }
    std::shared_ptr<arrow::Table> table   = *maybe_table;
    std::shared_ptr<arrow::Schema> schema = table->schema();

    std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();

    for(std::size_t i = 0; i < fields.size(); ++i)
    {
        std::string name       = fields[i]->name();
        auto type = fields[i]->type();
        size_t file_index      = i;
        bool is_in_file        = true;

        schema_out.add_column(name, type, file_index, is_in_file);
    }
}

} // namespace io
} // namespace arrow_backend
} // namespace compute
} // namespace voltron
