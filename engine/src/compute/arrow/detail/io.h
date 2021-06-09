#pragma once

#include <arrow/io/api.h>

#include "blazing_table/BlazingArrowTable.h"

std::unique_ptr<ral::frame::BlazingTable> read_parquet_arrow(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    std::vector<int> column_indices,
    std::vector<std::string> col_names,
    std::vector<cudf::size_type> row_groups);

//std::unique_ptr<ral::frame::BlazingTable> read_parquet_arrow(
//    std::shared_ptr<arrow::io::RandomAccessFile> file,
//    std::vector<int> column_indices,
//    std::vector<std::string> col_names,
//    std::vector<cudf::size_type> row_groups);


//std::vector<std::pair<std::string, cudf::type_id>> parse_schema_arrow(std::shared_ptr<parquet::FileMetaData> parquet_metadata);
std::vector<std::pair<std::string, cudf::type_id>> parse_schema_arrow(std::shared_ptr<arrow::io::RandomAccessFile> file);
