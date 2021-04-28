#include "ArrowCacheData.h"
#include <cudf/interop.hpp>

namespace ral {
namespace cache {

size_t get_arrow_data_size(std::shared_ptr<arrow::ArrayData> data) {
  if (data == nullptr) return 0;
  size_t ret = 0;
  for (auto child : data->child_data) {
    ret += get_arrow_data_size(child);
  }
  for (auto buffer : data->buffers) {
    ret += buffer->size();
  }
  return ret;
}

ArrowCacheData::ArrowCacheData(std::shared_ptr<arrow::Table> table, ral::io::Schema schema)
    : CacheData(CacheDataType::ARROW, schema.get_names(), schema.get_data_types(), table->num_rows()), data{table} {}

std::unique_ptr<ral::frame::BlazingTable> ArrowCacheData::decache() {
  return std::make_unique<ral::frame::BlazingTable>(data);
}

size_t ArrowCacheData::sizeInBytes() const {
  return 0;
  // TODO percy arrow
//  size_t ret = 0;
//  for (auto col : this->data->columns()) {
//    for (auto chunk : col->chunks()) {
//      ret += get_arrow_data_size(chunk->data());
//    }
//  }
//	return ret;
}

void ArrowCacheData::set_names(const std::vector<std::string> & names) {
	this->col_names = names;
}

ArrowCacheData::~ArrowCacheData() {}

} // namespace cache
} // namespace ral
