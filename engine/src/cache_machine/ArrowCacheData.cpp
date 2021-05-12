#include "ArrowCacheData.h"
#include <cudf/interop.hpp>
#include "parser/CalciteExpressionParsing.h"

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



std::unique_ptr<ral::frame::BlazingTable> ArrowCacheData::decache(execution::execution_backend backend) {
  if (backend.id() == ral::execution::backend_id::ARROW) {
    return std::move(data_);
  } else {
    return std::make_unique<ral::frame::BlazingCudfTable>(std::move(data_));
  }
}

size_t ArrowCacheData::size_in_bytes() const {
  // WSM TODO need to implement
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
	// WSM TODO need to implement
}

ArrowCacheData::~ArrowCacheData() {}

} // namespace cache
} // namespace ral
