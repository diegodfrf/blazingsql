#pragma once

#include "io/data_parser/CSVParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_provider/UriDataProvider.h"

namespace blazingdb {
namespace test {

using parquet_loader_tuple = std::tuple<std::shared_ptr<ral::io::parquet_parser>, std::shared_ptr<ral::io::uri_data_provider>, ral::io::Schema>;

parquet_loader_tuple load_table(const std::string& dataset_path, const std::string& table_name, ral::execution::execution_backend preferred_compute);

}  // namespace test
}  // namespace blazingdb
