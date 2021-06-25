#pragma once

#include <map>
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
    std::vector<int> row_groups);

std::unique_ptr<ral::frame::BlazingTable> read_csv_file(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<int> row_groups,
        const std::map<std::string, std::string> &args_map);

std::unique_ptr<ral::frame::BlazingTable> read_orc_file(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<int> row_groups);

std::unique_ptr<ral::frame::BlazingTable> read_json_file(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<int> row_groups);

void parse_parquet_schema(
    ral::io::Schema & schema_out,
    std::shared_ptr<arrow::io::RandomAccessFile> file);

void parse_csv_schema(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map);

void parse_orc_schema(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map);

void parse_json_schema(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map);

} // namespace io
} // namespace arrow_backend
} // namespace compute
} // namespace voltron