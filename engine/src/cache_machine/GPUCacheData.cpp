#include "GPUCacheData.h"

namespace ral {
namespace cache {

GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingCudfTable> table)
    : CacheData(CacheDataType::GPU,table->column_names(), table->column_types(), table->num_rows()),  data_{std::move(table)} {}


GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingCudfTable> table, const MetadataDictionary & metadata)
: CacheData(CacheDataType::GPU,table->column_names(), table->column_types(), table->num_rows()),  data_{std::move(table)} {
    this->metadata = metadata;
}

std::unique_ptr<ral::frame::BlazingTable> GPUCacheData::decache(execution::execution_backend backend) {
    return std::move(data_);
}

size_t GPUCacheData::size_in_bytes() const {
    return data_->size_in_bytes();
}

void GPUCacheData::set_names(const std::vector<std::string> & names) {
    data_->set_column_names(names);
}

std::shared_ptr<ral::frame::BlazingTableView> GPUCacheData::getTableView() {
  ral::frame::BlazingCudfTable *gpu_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(this->data_.get());
  return gpu_table_ptr->to_table_view();
}

void GPUCacheData::set_data(std::unique_ptr<ral::frame::BlazingCudfTable> table ) {
    this->data_ = std::move(table);
}

GPUCacheData::~GPUCacheData() {}

} // namespace cache
} // namespace ral