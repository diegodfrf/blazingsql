#include "compute/arrow/detail/io.h"

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>

namespace voltron {
namespace compute {
namespace arrow_backend {
namespace io {

std::unique_ptr<ral::frame::BlazingTable> read_parquet_file(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::vector<int> column_indices,
    std::vector<std::string> col_names,
    std::vector<int> row_groups)

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

  // TODO percy arrow rommel skip data use group info here  
  //st = arrow_reader->ReadRowGroups(myrow_groups, column_indices, &table);
  st = arrow_reader->ReadTable(column_indices, &table);

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

void parse_parquet_schema(
    ral::io::Schema & schema_out,
    std::shared_ptr<arrow::io::RandomAccessFile> file)
{
  // TODO percy arrow move common parquet reader code here (avoid open the file twice)
  auto parquet_reader = parquet::ParquetFileReader::Open(file);
  std::shared_ptr<parquet::FileMetaData> parquet_metadata = parquet_reader->metadata();
  std::shared_ptr<::arrow::Schema> arrow_schema;
  parquet::ArrowReaderProperties props;
  // TODO percy arrow handle error
  parquet::arrow::FromParquetSchema(parquet_metadata->schema(), props, &arrow_schema);
  for(int i = 0; i < arrow_schema->fields().size(); i++) {
		std::string name = arrow_schema->field(i)->name();
    size_t file_index = i;
		bool is_in_file = true;
    schema_out.add_column(
          name,
          arrow_schema->field(i)->type(),
          file_index, is_in_file);
  }
}

} // namespace io
} // namespace arrow_backend
} // namespace compute
} // namespace voltron
