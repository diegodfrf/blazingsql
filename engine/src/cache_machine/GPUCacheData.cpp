#include "GPUCacheData.h"

namespace ral {
namespace cache {

GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table)
    : CacheData(CacheDataType::GPU,table->column_names(), table->column_types(), table->num_rows()),  data{std::move(table)} {}


GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata)
: CacheData(CacheDataType::GPU,table->column_names(), table->column_types(), table->num_rows()),  data{std::move(table)} {
    this->metadata = metadata;
}

std::unique_ptr<ral::frame::BlazingTable> GPUCacheData::decache() {
    return std::move(data);
}

size_t GPUCacheData::size_in_bytes() const {
    return data->size_in_bytes();
}

void GPUCacheData::set_names(const std::vector<std::string> & names) {
    data->set_column_names(names);
}

std::shared_ptr<ral::frame::BlazingTableView> GPUCacheData::getTableView() {
  ral::frame::BlazingCudfTable *gpu_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(this->data.get());
  return gpu_table_ptr->to_table_view();
}

void GPUCacheData::set_data(std::unique_ptr<ral::frame::BlazingTable> table ) {
    this->data = std::move(table);
}

GPUCacheData::~GPUCacheData() {}

} // namespace cache
} // namespace ral