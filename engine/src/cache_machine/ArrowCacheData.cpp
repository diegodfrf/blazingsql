#include "ArrowCacheData.h"
#include "parser/CalciteExpressionParsing.h"

namespace ral {
namespace cache {

std::unique_ptr<ral::frame::BlazingTable> ArrowCacheData::decache(execution::execution_backend backend) {
  if (backend.id() == ral::execution::backend_id::ARROW) {
    return std::move(data_);
  } else {
#ifdef CUDF_SUPPORT
    return std::make_unique<ral::frame::BlazingCudfTable>(std::move(data_));
#endif
  }
  return nullptr;
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

std::unique_ptr<CacheData> ArrowCacheData::clone() {
  return std::make_unique<ArrowCacheData>(this->data_->clone());
}

ArrowCacheData::~ArrowCacheData() {}

} // namespace cache
} // namespace ral
