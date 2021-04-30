#include "GPUCacheData.h"

namespace ral {
namespace cache {

GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table)
    : CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}


GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata)
: CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {
    this->metadata = metadata;
}

std::unique_ptr<ral::frame::BlazingTable> GPUCacheData::decache() {
    return std::move(data);
}

size_t GPUCacheData::sizeInBytes() const {
    return data->sizeInBytes();
}

void GPUCacheData::set_names(const std::vector<std::string> & names) {
    data->setNames(names);
}

std::shared_ptr<ral::frame::BlazingCudfTableView> GPUCacheData::getTableView() {
    return this->data->to_table_view();
}

void GPUCacheData::set_data(std::unique_ptr<ral::frame::BlazingCudfTable> table ) {
    this->data = std::move(table);
}

GPUCacheData::~GPUCacheData() {}

} // namespace cache
} // namespace ral