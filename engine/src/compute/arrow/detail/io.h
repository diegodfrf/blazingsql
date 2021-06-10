#pragma once

#include <arrow/io/api.h>

#include "blazing_table/BlazingArrowTable.h"
#include "io/Schema.h"

namespace voltron {
namespace compute {
namespace arrow_backend {
namespace io {

std::unique_ptr<ral::frame::BlazingTable> read_parquet_file(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::vector<int> column_indices,
    std::vector<std::string> col_names,
    std::vector<cudf::size_type> row_groups);

void parse_parquet_schema(
    ral::io::Schema & schema_out,
    std::shared_ptr<arrow::io::RandomAccessFile> file);

} // namespace io
} // namespace arrow_backend
} // namespace compute
} // namespace voltron
