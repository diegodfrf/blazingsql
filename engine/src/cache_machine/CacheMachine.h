#pragma once

#include <spdlog/spdlog.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "common/WaitingQueue.h"
#include "CacheData.h"
#include "blazing_table/BlazingHostTable.h"

namespace ral {
namespace cache {

using Context = blazingdb::manager::Context;

const int CACHE_LEVEL_AUTO = -1;
const int CACHE_LEVEL_GPU = 0;
const int CACHE_LEVEL_CPU = 1;
const int CACHE_LEVEL_DISK = 2;

class AbstractCacheMachine {
 public:
  AbstractCacheMachine(std::shared_ptr<Context> context, std::string cache_machine_name,
                       bool log_timeout = true, int cache_level_override = -1,
                       bool is_array_access = false);

  std::vector<std::unique_ptr<ral::cache::CacheData> > pull_all_cache_data();

  uint64_t get_num_bytes_added();

  uint64_t get_num_rows_added();

  uint64_t get_num_batches_added();

  void wait_until_finished();

  std::int32_t get_id() const;

  Context* get_context() const;

  bool wait_for_next();

  bool has_next_now();

  bool has_messages_now(std::vector<std::string> messages);

  std::unique_ptr<ral::cache::CacheData> pullAnyCacheData(
      const std::vector<std::string>& messages);

  std::size_t get_num_batches();

  std::vector<size_t> get_all_indexes();

  void wait_for_count(int count);

  bool has_data_in_index_now(size_t index);

  bool is_finished();

  void finish();

  void clear();

  std::unique_ptr<ral::frame::BlazingTable> get_or_wait(execution::execution_backend backend, size_t index);

  std::unique_ptr<ral::cache::CacheData> get_or_wait_CacheData(size_t index);

  bool addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> table,
                           std::string message_id = "");

  virtual std::unique_ptr<ral::frame::BlazingTable> pullFromCache(
      execution::execution_backend backend);

  virtual std::unique_ptr<ral::frame::BlazingTable> pullUnorderedFromCache(
      execution::execution_backend backend);

  virtual std::unique_ptr<ral::cache::CacheData> pullCacheData(std::string message_id);

  virtual std::unique_ptr<ral::cache::CacheData> pullCacheData();

  virtual std::unique_ptr<ral::cache::CacheData> pullCacheDataCopy();

 protected:
  /// This property represents a waiting queue object which stores all CacheData Objects
  std::unique_ptr<WaitingQueue<std::unique_ptr<message> > > waitingCache;

  std::atomic<std::size_t> num_bytes_added;
  std::atomic<uint64_t> num_rows_added;
  /// This variable is to keep track of if anything has been added to the cache. Its
  /// useful to keep from adding empty tables to the cache, where we might want an empty
  /// table at least to know the schema
  bool something_added;
  std::shared_ptr<Context> ctx;
  const std::size_t cache_id;
  int cache_level_override;
  std::string cache_machine_name;
  std::shared_ptr<spdlog::logger> cache_events_logger;
  bool is_array_access;
  int global_index;

 protected:
  static std::size_t cache_count;
};

/**
        @brief A class that represents a Cache Machine on a
        multi-tier (GPU memory, CPU memory, Disk memory) cache system.
*/
class CacheMachine : public AbstractCacheMachine {
 protected:
  CacheMachine(std::shared_ptr<Context> context, std::string cache_machine_name,
               bool log_timeout = true, int cache_level_override = -1,
               bool is_array_access = false);

 public:

  void put(size_t index, std::unique_ptr<ral::frame::BlazingTable> table) {
    this->addToCache(std::move(table), this->cache_machine_name + "_" + std::to_string(index), true);
  }

  void put(size_t index, std::unique_ptr<ral::cache::CacheData> cacheData) {
    this->addCacheData(std::move(cacheData), this->cache_machine_name + "_" + std::to_string(index), true);
  }

  virtual bool addToCache(std::unique_ptr<ral::frame::BlazingTable> table,
                          std::string message_id = "", bool always_add = false,
                          const MetadataDictionary& metadata = {},
                          bool use_pinned = false) {
    throw "addToCache method is not implemented yet.";
  }

  virtual bool addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data,
                            std::string message_id = "", bool always_add = false) {
    throw "addCacheData method is not implemented yet.";
  }

  // take the first cacheData in this CacheMachine that it can find (looking in reverse
  // order) that is in the GPU put it in RAM or Disk as oppropriate this function does not
  // change the order of the caches
  virtual size_t downgradeGPUCacheData() { throw "downgradeGPUCacheData method is not implemented yet."; }

 public:
  
  static std::shared_ptr<CacheMachine> make_single_machine(
      std::shared_ptr<Context> context, std::string cache_machine_name,
      bool log_timeout = true, int cache_level_override = -1,
      bool is_array_access = false);

  static std::shared_ptr<CacheMachine> make_concatenating_machine(
      std::shared_ptr<Context> context, std::size_t concat_cache_num_bytes,
      int num_bytes_timeout, bool concat_all, std::string cache_machine_name);
};


}  // namespace cache
}  // namespace ral
