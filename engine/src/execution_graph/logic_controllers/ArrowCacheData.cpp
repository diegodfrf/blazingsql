#include "ArrowCacheData.h"
#include <cudf/interop.hpp>
#include "CalciteExpressionParsing.h"

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

ArrowCacheData::ArrowCacheData(ral::io::data_handle handle,
	std::shared_ptr<ral::io::data_parser> parser,
	ral::io::Schema schema,
	std::vector<int> row_group_ids,
	std::vector<int> projections)
	: CacheData(CacheDataType::ARROW, schema.get_names(), schema.get_data_types(), handle.arrow_table->num_rows()),
	handle(handle), parser(parser), schema(schema),
	row_group_ids(row_group_ids),
	projections(projections)
	{

	}

std::unique_ptr<ral::frame::BlazingTable> ArrowCacheData::decache() {
  return parser->parse_batch(handle, schema, projections, row_group_ids);
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
	this->schema.set_names(names);
}

ArrowCacheData::~ArrowCacheData() {}

} // namespace cache
} // namespace ral
