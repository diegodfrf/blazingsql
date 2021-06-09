#include "compute/arrow/detail/types.h"

#include <assert.h>

std::shared_ptr<arrow::Schema> build_arrow_schema(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>> &columns,
    const std::vector<std::string> &column_names,
    std::shared_ptr<const arrow::KeyValueMetadata> metadata)
{
  assert(columns.size() == column_names.size());
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.resize(columns.size());
  for (int i = 0; i < columns.size(); ++i) {
    fields[i] = arrow::field(
                  column_names[i],
                  columns[i]->type());
  }
  return arrow::schema(fields, metadata);
}
