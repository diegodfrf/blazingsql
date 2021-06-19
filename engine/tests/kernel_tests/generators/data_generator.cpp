#include "data_generator.h"
#include <Config/BlazingContext.h>
#include <map>
#include <vector>
#include "io/DataLoader.h"


namespace blazingdb {
namespace test {

std::vector<Uri> visitPartitionFolder(Uri folder_uri, const std::string& wildcard) {
  std::vector<Uri> uris;
  auto fs = BlazingContext::getInstance()->getFileSystemManager();

  auto matches = fs->list(folder_uri, wildcard);
  for (auto &&uri : matches) {
    auto status = fs->getFileStatus(uri);
    if (!status.isDirectory()) {
      uris.push_back(uri);
    }
  }
  return uris;
}

parquet_loader_tuple get_loader(const std::string &dir_path, const std::string& wildcard, ral::execution::execution_backend preferred_compute){
  std::vector<Uri> uris = visitPartitionFolder(dir_path, wildcard);
  auto parser = std::make_shared<ral::io::parquet_parser>();
  auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
  ral::io::Schema tableSchema;
  ral::io::data_loader loader(parser, provider);
  loader.get_schema(preferred_compute, tableSchema, {});

  std::vector<std::vector<int>> all_row_groups{};
  for (int index = 0; index < uris.size(); index++) {
    std::vector<int> row_group{0};
    all_row_groups.push_back(row_group);
  }
  ral::io::Schema schema(tableSchema.get_names(),
                         tableSchema.get_calcite_to_file_indices(),
                         tableSchema.get_dtypes(),
                         tableSchema.get_in_file(),
                         all_row_groups);
  return std::make_tuple(parser, provider, schema);
}
parquet_loader_tuple load_table(const std::string &dataset_path, const std::string& table_name, ral::execution::execution_backend preferred_compute) {
  return get_loader(dataset_path, table_name + "*.parquet", preferred_compute);
}

}  // namespace test
}  // namespace blazingdb