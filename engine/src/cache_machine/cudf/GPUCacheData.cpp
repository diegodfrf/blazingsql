#include "GPUCacheData.h"

#include "../common/CPUCacheData.h"
#include "bmr/BlazingMemoryResource.h"
#include "communication/CommunicationData.h"
#include "cache_machine/common/CacheDataLocalFile.h"
#include <spdlog/logger.h>
#include <spdlog/spdlog.h>
using namespace fmt::literals;

namespace ral {
namespace cache {

//////////////////////////////////////////////////////////////////////////////////////////
/**
 * Utility function which can take a CacheData and if its a standard GPU cache data, it
 * will downgrade it to CPU or Disk
 * @return If the input CacheData is not of a type that can be downgraded, it will just
 * return the original input, otherwise it will return the downgraded CacheData.
 */
std::unique_ptr<CacheData> downgradeGPUCacheData(std::unique_ptr<CacheData> cacheData, std::string id, std::shared_ptr<Context> ctx) {
  // if its not a GPU cacheData, then we can't downgrade it, so we can just return it
  if (cacheData->get_type() != ral::cache::CacheDataType::GPU) {
    return cacheData;
  } else {
    CodeTimer cacheEventTimer(false);
    cacheEventTimer.start();

    std::unique_ptr<ral::frame::BlazingTable> table = cacheData->decache(
        ral::execution::execution_backend(ral::execution::backend_id::CUDF));
    std::shared_ptr<spdlog::logger> cache_events_logger =
        spdlog::get("cache_events_logger");

    // lets first try to put it into CPU
    if (blazing_host_memory_resource::getInstance().get_memory_used() +
        table->size_in_bytes() <
        blazing_host_memory_resource::getInstance().get_memory_limit()) {
      auto CPUCache = std::make_unique<CPUCacheData>(std::move(table));

      cacheEventTimer.stop();
      if (cache_events_logger) {
        cache_events_logger->info(
            "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
            "type}|{timestamp_begin}|{timestamp_end}|{description}",
            "ral_id"_a = (ctx ? ctx->getNodeIndex(
                ral::communication::CommunicationData::getInstance()
                    .getSelfNode())
                              : -1),
            "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = "",
            "cache_id"_a = id, "num_rows"_a = (CPUCache ? CPUCache->num_rows() : -1),
            "num_bytes"_a = (CPUCache ? CPUCache->size_in_bytes() : -1),
            "event_type"_a = "DowngradeCacheData",
            "timestamp_begin"_a = cacheEventTimer.start_time(),
            "timestamp_end"_a = cacheEventTimer.end_time(),
            "description"_a = "Downgraded CacheData to CPU cache");
      }
      return CPUCache;
    } else {
      // want to get only cache directory where orc files should be saved
      std::string orc_files_path =
          ral::communication::CommunicationData::getInstance().get_cache_directory();

      auto localCache = std::make_unique<CacheDataLocalFile>(
          std::move(table), orc_files_path,
          (ctx ? std::to_string(ctx->getContextToken()) : "none"));

      cacheEventTimer.stop();
      if (cache_events_logger) {
        cache_events_logger->info(
            "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
            "type}|{timestamp_begin}|{timestamp_end}|{description}",
            "ral_id"_a = (ctx ? ctx->getNodeIndex(
                ral::communication::CommunicationData::getInstance()
                    .getSelfNode())
                              : -1),
            "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = "",
            "cache_id"_a = id, "num_rows"_a = (localCache ? localCache->num_rows() : -1),
            "num_bytes"_a = (localCache ? localCache->size_in_bytes() : -1),
            "event_type"_a = "DowngradeCacheData",
            "timestamp_begin"_a = cacheEventTimer.start_time(),
            "timestamp_end"_a = cacheEventTimer.end_time(),
            "description"_a =
                "Downgraded CacheData to Disk cache to path: " + orc_files_path);
      }

      return localCache;
    }
  }
}
//////////////////////////////////////////////////////////////////////////////////////////

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
        return std::make_unique<ral::frame::BlazingArrowTable>(std::move(data_));
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