#include "GPUCacheData.h"

#include "blazing_table/BlazingCudfTable.h"
#include "blazing_table/BlazingArrowTable.h"

namespace ral {
namespace cache {

GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingCudfTable> table)
    : CacheData(CacheDataType::GPU,table->column_names(), table->column_types(), table->num_rows()),  data_{std::move(table)} {}


GPUCacheData::GPUCacheData(std::unique_ptr<ral::frame::BlazingCudfTable> table, const MetadataDictionary & metadata)
: CacheData(CacheDataType::GPU,table->column_names(), table->column_types(), table->num_rows()),  data_{std::move(table)} {
    this->metadata = metadata;
}

std::unique_ptr<ral::frame::BlazingTable> GPUCacheData::decache(execution::execution_backend backend) {
    if (backend.id() == ral::execution::backend_id::CUDF) {
        return std::move(data_);
    } else {

#ifdef CUDF_SUPPORT
        return std::make_unique<ral::frame::BlazingArrowTable>(std::move(data_));
#endif
    }
}

size_t GPUCacheData::size_in_bytes() const {
    return data_->size_in_bytes();
}

void GPUCacheData::set_names(const std::vector<std::string> & names) {
    data_->set_column_names(names);
}

std::unique_ptr<CacheData> GPUCacheData::clone() {
    std::unique_ptr<ral::frame::BlazingCudfTable> cudf_table(dynamic_cast<ral::frame::BlazingCudfTable*>(this->data_->clone().release()));
    return std::make_unique<GPUCacheData>(std::move(cudf_table), this->metadata);
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