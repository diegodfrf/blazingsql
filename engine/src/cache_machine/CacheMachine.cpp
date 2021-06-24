
#include "cache_machine/CacheMachine.h"

#include <Util/StringUtil.h>
#include <stdio.h>
#include <sys/stat.h>

#include <vector>
#include <random>

#include "Util/StringUtil.h"
#include "cache_machine/common/CPUCacheData.h"
#include "cache_machine/common/ConcatCacheData.h"
#include "communication/CommunicationData.h"
#include "compute/backend_dispatcher.h"
#include "operators/Concatenate.h"
#include "parser/CalciteExpressionParsing.h"
#include "compute/api.h"
#include "bmr/BlazingMemoryResource.h"
#include "common/CacheDataLocalFile.h"

#ifdef CUDF_SUPPORT
#include "blazing_table/BlazingCudfTable.h"
#include "cudf/GPUCacheData.h"
#endif
namespace ral {
namespace cache {

using namespace fmt::literals;
 
///////////////////////////////////////////////////////////////////////////////////////
CacheMachine::CacheMachine(std::shared_ptr<Context> context,
                           std::string cache_machine_name, bool log_timeout,
                           int cache_level_override, bool is_array_access)
    : AbstractCacheMachine(context, cache_machine_name, log_timeout, cache_level_override,
                           is_array_access) 
{
  this->memory_resources.push_back(&blazing_device_memory_resource::getInstance());
  this->memory_resources.push_back(&blazing_host_memory_resource::getInstance());
  this->memory_resources.push_back(&blazing_disk_memory_resource::getInstance());
}

CacheMachine::~CacheMachine() {}


bool CacheMachine::addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data,
                                    std::string message_id, bool always_add) {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  // we dont want to add empty tables to a cache, unless we have never added anything
  if ((!this->something_added || cache_data->num_rows() > 0) || always_add) {
    num_rows_added += cache_data->num_rows();
    num_bytes_added += cache_data->size_in_bytes();
    int cacheIndex = 0;
    ral::cache::CacheDataType type = cache_data->get_type();
    if (type == ral::cache::CacheDataType::GPU) {
      cacheIndex = 0;
    } else if (type == ral::cache::CacheDataType::CPU) {
      cacheIndex = 1;
    } else {
      cacheIndex = 2;
    }

    if (message_id == "") {
      message_id = this->cache_machine_name;
    }

    if (cacheIndex == 0) {
      auto item = std::make_unique<message>(std::move(cache_data), message_id);
      this->waitingCache->put(std::move(item));

      cacheEventTimer.stop();
      if (cache_events_logger) {
        cache_events_logger->info(
            "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
            "type}|{timestamp_begin}|{timestamp_end}|{description}",
            "ral_id"_a = (ctx ? ctx->getNodeIndex(
                                    ral::communication::CommunicationData::getInstance()
                                        .getSelfNode())
                              : -1),
            "query_id"_a = (ctx ? ctx->getContextToken() : -1),
            "message_id"_a = message_id, "cache_id"_a = cache_id,
            "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
            "event_type"_a = "AddCacheData",
            "timestamp_begin"_a = cacheEventTimer.start_time(),
            "timestamp_end"_a = cacheEventTimer.end_time(),
            "description"_a =
                "Add to CacheMachine general CacheData object into GPU cache");
      }
    } else if (cacheIndex == 1) {
      auto item = std::make_unique<message>(std::move(cache_data), message_id);
      this->waitingCache->put(std::move(item));

      cacheEventTimer.stop();
      if (cache_events_logger) {
        cache_events_logger->info(
            "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
            "type}|{timestamp_begin}|{timestamp_end}|{description}",
            "ral_id"_a = (ctx ? ctx->getNodeIndex(
                                    ral::communication::CommunicationData::getInstance()
                                        .getSelfNode())
                              : -1),
            "query_id"_a = (ctx ? ctx->getContextToken() : -1),
            "message_id"_a = message_id, "cache_id"_a = cache_id,
            "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
            "event_type"_a = "AddCacheData",
            "timestamp_begin"_a = cacheEventTimer.start_time(),
            "timestamp_end"_a = cacheEventTimer.end_time(),
            "description"_a =
                "Add to CacheMachine general CacheData object into CPU cache");
      }
    } else if (cacheIndex == 2) {
      // BlazingMutableThread t([cache_data = std::move(cache_data), this, cacheIndex,
      // message_id]() mutable {
      auto item = std::make_unique<message>(std::move(cache_data), message_id);
      this->waitingCache->put(std::move(item));
      // NOTE: Wait don't kill the main process until the last thread is finished!
      // }); t.detach();

      cacheEventTimer.stop();
      if (cache_events_logger) {
        cache_events_logger->info(
            "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
            "type}|{timestamp_begin}|{timestamp_end}|{description}",
            "ral_id"_a = (ctx ? ctx->getNodeIndex(
                                    ral::communication::CommunicationData::getInstance()
                                        .getSelfNode())
                              : -1),
            "query_id"_a = (ctx ? ctx->getContextToken() : -1),
            "message_id"_a = message_id, "cache_id"_a = cache_id,
            "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
            "event_type"_a = "AddCacheData",
            "timestamp_begin"_a = cacheEventTimer.start_time(),
            "timestamp_end"_a = cacheEventTimer.end_time(),
            "description"_a =
                "Add to CacheMachine general CacheData object into Disk cache");
      }
    }
    this->something_added = true;

    return true;
  }

  return false;
}

bool CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table,
                                  std::string message_id, bool always_add,
                                  const MetadataDictionary& metadata, bool use_pinned) {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  // we dont want to add empty tables to a cache, unless we have never added anything
  if (!this->something_added || table->num_rows() > 0 || always_add) {
    // WSM TODO do we want to use the backend_dispatcher here too? This is more business
    // logic, not data transformation
    if (table->get_execution_backend().id() == ral::execution::backend_id::CUDF) {
      #ifdef CUDF_SUPPORT
      ral::frame::BlazingCudfTable* cudf_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(table.get());
      for (auto col_ind = 0; col_ind < table->num_columns(); col_ind++) {
        if (cudf_table_ptr->view().column(col_ind).offset() > 0) {
          cudf_table_ptr->ensureOwnership();
          break;
        }
      }
      #endif
    }

    if (message_id == "") {
      message_id = this->cache_machine_name;
    }

    num_rows_added += table->num_rows();
    num_bytes_added += table->size_in_bytes();
    // WSM TODO do we want to use the backend_dispatcher here too? This is more business
    // logic, not data transformation
    size_t cacheIndex =
        table->get_execution_backend().id() == ral::execution::backend_id::CUDF ? 0 : 1;
    while (cacheIndex < memory_resources.size()) {
      auto memory_to_use = (this->memory_resources[cacheIndex]->get_memory_used() +
                            table->size_in_bytes());

      if (memory_to_use < this->memory_resources[cacheIndex]->get_memory_limit() ||
          cache_level_override != -1) {
        if (cache_level_override != -1) {
          cacheIndex = cache_level_override;
        }
        if (cacheIndex == 0 && table->get_execution_backend().id() == ral::execution::backend_id::CUDF) {
          #ifdef CUDF_SUPPORT
          std::unique_ptr<ral::frame::BlazingCudfTable> cudf_table(dynamic_cast<ral::frame::BlazingCudfTable*>(table.release()));
          // before we put into a cache, we need to make sure we fully own the table
          cudf_table->ensureOwnership();
          std::unique_ptr<CacheData> cache_data = std::make_unique<GPUCacheData>(std::move(cudf_table), metadata);
          auto item = std::make_unique<message>(std::move(cache_data), message_id);
          this->waitingCache->put(std::move(item));

          cacheEventTimer.stop();
          if (cache_events_logger) {
            cache_events_logger->info(
                "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{"
                "event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                "ral_id"_a =
                    (ctx ? ctx->getNodeIndex(
                               ral::communication::CommunicationData::getInstance()
                                   .getSelfNode())
                         : -1),
                "query_id"_a = (ctx ? ctx->getContextToken() : -1),
                "message_id"_a = message_id, "cache_id"_a = cache_id,
                "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
                "event_type"_a = "AddToCache",
                "timestamp_begin"_a = cacheEventTimer.start_time(),
                "timestamp_end"_a = cacheEventTimer.end_time(),
                "description"_a = "Add to CacheMachine into GPU cache");
          }
          #endif
        } else {
          if (cacheIndex == 1) {
            std::unique_ptr<CacheData> cache_data;
            // WSM TODO. Here we need to decide if we always want to put into a
            // CPUCacheData or not I think we want to put it into an ArrowCacheData and
            // only convert to CPUCacheDAta if we are actually going to do this for comms
            if (table->get_execution_backend().id() == ral::execution::backend_id::CUDF) {
              cache_data =
                  std::make_unique<CPUCacheData>(std::move(table), metadata, use_pinned);
            } else {
              std::unique_ptr<ral::frame::BlazingArrowTable> arrow_table(
                  dynamic_cast<ral::frame::BlazingArrowTable*>(table.release()));
              cache_data =
                  std::make_unique<ArrowCacheData>(std::move(arrow_table), metadata);
            }

            auto item = std::make_unique<message>(std::move(cache_data), message_id);
            this->waitingCache->put(std::move(item));

            cacheEventTimer.stop();
            if (cache_events_logger) {
              cache_events_logger->info(
                  "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{"
                  "event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                  "ral_id"_a =
                      (ctx ? ctx->getNodeIndex(
                                 ral::communication::CommunicationData::getInstance()
                                     .getSelfNode())
                           : -1),
                  "query_id"_a = (ctx ? ctx->getContextToken() : -1),
                  "message_id"_a = message_id, "cache_id"_a = cache_id,
                  "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
                  "event_type"_a = "AddToCache",
                  "timestamp_begin"_a = cacheEventTimer.start_time(),
                  "timestamp_end"_a = cacheEventTimer.end_time(),
                  "description"_a = "Add to CacheMachine into CPU cache");
            }

          } else if (cacheIndex == 2) {
            // BlazingMutableThread t([table = std::move(table), this, cacheIndex,
            // message_id]() mutable { want to get only cache directory where orc files
            // should be saved
            std::string orc_files_path =
                ral::communication::CommunicationData::getInstance()
                    .get_cache_directory();
            auto cache_data = std::make_unique<CacheDataLocalFile>(
                std::move(table), orc_files_path,
                (ctx ? std::to_string(ctx->getContextToken()) : "none"));
            cache_data->setMetadata(metadata);
            auto item = std::make_unique<message>(std::move(cache_data), message_id);
            this->waitingCache->put(std::move(item));
            // NOTE: Wait don't kill the main process until the last thread is finished!
            // });t.detach();

            cacheEventTimer.stop();
            if (cache_events_logger) {
              cache_events_logger->info(
                  "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{"
                  "event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                  "ral_id"_a =
                      (ctx ? ctx->getNodeIndex(
                                 ral::communication::CommunicationData::getInstance()
                                     .getSelfNode())
                           : -1),
                  "query_id"_a = (ctx ? ctx->getContextToken() : -1),
                  "message_id"_a = message_id, "cache_id"_a = cache_id,
                  "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
                  "event_type"_a = "AddToCache",
                  "timestamp_begin"_a = cacheEventTimer.start_time(),
                  "timestamp_end"_a = cacheEventTimer.end_time(),
                  "description"_a = "Add to CacheMachine into Disk cache");
            }
          }
        }
        break;
      }
      cacheIndex++;
    }
    this->something_added = true;

    return true;
  }

  return false;
}

// take the first cacheData in this CacheMachine that it can find (looking in reverse
// order) that is in the GPU put it in RAM or Disk as oppropriate this function does not
// change the order of the caches
size_t CacheMachine::downgradeGPUCacheData() {
  size_t bytes_downgraded = 0;
  #ifdef CUDF_SUPPORT
  std::unique_lock<std::mutex> lock = this->waitingCache->lock();
  std::vector<std::unique_ptr<message>> all_messages =
      this->waitingCache->get_all_unsafe();
  for (int i = all_messages.size() - 1; i >= 0; i--) {
    if (all_messages[i]->get_data().get_type() == CacheDataType::GPU) {
      std::string message_id = all_messages[i]->get_message_id();
      auto current_cache_data = all_messages[i]->release_data();
      bytes_downgraded += current_cache_data->size_in_bytes();
      auto new_cache_data = ral::cache::downgradeGPUCacheData(
          std::move(current_cache_data), message_id, ctx);

      auto new_message = std::make_unique<message>(std::move(new_cache_data), message_id);
      all_messages[i] = std::move(new_message);
    }
  }

  this->waitingCache->put_all_unsafe(std::move(all_messages));
  #endif
  return bytes_downgraded;
}

///////////////////////////////////////////////////////////////////////////////////////
/**
        @brief A class that represents a Cache Machine on a
        multi-tier cache system. Moreover, it only returns a single BlazingTable by
   concatenating all batches. This Cache Machine is used in the last Kernel (OutputKernel)
   in the ExecutionGraph.

        This ConcatenatingCacheMachine::pullFromCache method does not guarantee the
   relative order of the messages to be preserved
*/
class ConcatenatingCacheMachine : public CacheMachine {
 public:
  ConcatenatingCacheMachine(std::shared_ptr<Context> context,
                            std::string cache_machine_name)
      : CacheMachine(context, cache_machine_name) {}

  ConcatenatingCacheMachine(std::shared_ptr<Context> context,
                            std::size_t concat_cache_num_bytes, int num_bytes_timeout,
                            bool concat_all, std::string cache_machine_name)
      : CacheMachine(context, cache_machine_name) {
    this->concat_cache_num_bytes = concat_cache_num_bytes;
    this->num_bytes_timeout = num_bytes_timeout;
    this->concat_all = concat_all;
  }

  ~ConcatenatingCacheMachine() = default;

  std::unique_ptr<ral::cache::CacheData> pullCacheData() override;

  std::unique_ptr<ral::frame::BlazingTable> pullFromCache(
      execution::execution_backend backend) override;

  std::unique_ptr<ral::frame::BlazingTable> pullUnorderedFromCache(
      execution::execution_backend backend) override {
    return pullFromCache(backend);
  }

  size_t downgradeGPUCacheData()
      override {  // dont want to be able to downgrage concatenating caches
    return 0;
  }

 private:
  std::size_t concat_cache_num_bytes;
  int num_bytes_timeout;
  bool concat_all;
};

// This method does not guarantee the relative order of the messages to be preserved
// WSM TODO refactor this to duse pullCacheData and convert that to a ConcatCacheData
std::unique_ptr<ral::frame::BlazingTable> ConcatenatingCacheMachine::pullFromCache(
    execution::execution_backend backend) {
  CodeTimer cacheEventTimerGeneral;
  cacheEventTimerGeneral.start();

  if (concat_all) {
    this->waitingCache->wait_until_finished();
  } else {
    waitingCache->wait_until_num_bytes(this->concat_cache_num_bytes,
                                       this->num_bytes_timeout);
  }

  size_t total_bytes = 0;
  std::vector<std::unique_ptr<message>> collected_messages;
  std::unique_ptr<message> message_data;
  std::string message_id = "";

  do {
    if (concat_all || waitingCache->has_next_now()) {
      message_data = waitingCache->pop_or_wait();
    } else {
      message_data = nullptr;
    }
    if (message_data == nullptr) {
      break;
    }
    auto& cache_data = message_data->get_data();
    total_bytes += cache_data.size_in_bytes();
    message_id = message_data->get_message_id();
    collected_messages.push_back(std::move(message_data));

  } while (concat_all || (total_bytes + waitingCache->get_next_size_in_bytes() <=
                          this->concat_cache_num_bytes));

  std::unique_ptr<ral::frame::BlazingTable> output;
  size_t num_rows = 0;
  if (collected_messages.empty()) {
    output = nullptr;
  } else if (collected_messages.size() == 1) {
    auto data = collected_messages[0]->release_data();
    output = data->decache(backend);
    num_rows = output->num_rows();
  } else {
    std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_holder;
    std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views;
    for (size_t i = 0; i < collected_messages.size(); i++) {
      CodeTimer cacheEventTimer;
      cacheEventTimer.start();

      auto data = collected_messages[i]->release_data();
      tables_holder.push_back(data->decache(backend));
      table_views.push_back(tables_holder[i]->to_table_view());

      // if we dont have to concatenate all, lets make sure we are not overflowing, and if
      // we are, lets put one back
      if (!concat_all && backend.id() == ral::execution::backend_id::CUDF &&
          checkIfConcatenatingStringsWillOverflow(table_views)) {
        std::unique_ptr<CacheData> cache_data = ral::execution::backend_dispatcher(
            tables_holder.back()->get_execution_backend(), make_cachedata_functor(),
            std::move(tables_holder.back()));
        tables_holder.pop_back();
        table_views.pop_back();
        collected_messages[i] = std::make_unique<message>(
            std::move(cache_data), collected_messages[i]->get_message_id());
        for (; i < collected_messages.size(); i++) {
          this->waitingCache->put(std::move(collected_messages[i]));
        }

        cacheEventTimer.stop();
        if (cache_events_logger) {
          cache_events_logger->warn(
              "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
              "type}|{timestamp_begin}|{timestamp_end}|{description}",
              "ral_id"_a = (ctx ? ctx->getNodeIndex(
                                      ral::communication::CommunicationData::getInstance()
                                          .getSelfNode())
                                : -1),
              "query_id"_a = (ctx ? ctx->getContextToken() : -1),
              "message_id"_a = message_id, "cache_id"_a = cache_id,
              "num_rows"_a = num_rows, "num_bytes"_a = total_bytes,
              "event_type"_a = "PullFromCache",
              "timestamp_begin"_a = cacheEventTimer.start_time(),
              "timestamp_end"_a = cacheEventTimer.end_time(),
              "description"_a =
                  "In ConcatenatingCacheMachine::pullFromCache Concatenating could have "
                  "caused overflow strings length. Adding cache data back");
        }

        break;
      }
    }

    if (concat_all && checkIfConcatenatingStringsWillOverflow(
                          table_views)) {  // if we have to concatenate all, then lets
      // throw a warning if it will overflow strings
      CodeTimer cacheEventTimer;
      cacheEventTimer.start();
      cacheEventTimer.stop();
      if (cache_events_logger) {
        cache_events_logger->warn(
            "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
            "type}|{timestamp_begin}|{timestamp_end}|{description}",
            "ral_id"_a = (ctx ? ctx->getNodeIndex(
                                    ral::communication::CommunicationData::getInstance()
                                        .getSelfNode())
                              : -1),
            "query_id"_a = (ctx ? ctx->getContextToken() : -1),
            "message_id"_a = message_id, "cache_id"_a = cache_id, "num_rows"_a = num_rows,
            "num_bytes"_a = total_bytes, "event_type"_a = "PullFromCache",
            "timestamp_begin"_a = cacheEventTimer.start_time(),
            "timestamp_end"_a = cacheEventTimer.end_time(),
            "description"_a =
                "In ConcatenatingCacheMachine::pullFromCache Concatenating will overflow "
                "strings length");
      }
    }
    output = concatTables(table_views);

    num_rows = output->num_rows();
  }

  cacheEventTimerGeneral.stop();
  if (cache_events_logger) {
    cache_events_logger->trace(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = total_bytes,
        "event_type"_a = "PullFromCache",
        "timestamp_begin"_a = cacheEventTimerGeneral.start_time(),
        "timestamp_end"_a = cacheEventTimerGeneral.end_time(),
        "description"_a = "Pull from ConcatenatingCacheMachine");
  }

  return output;
}

std::unique_ptr<ral::cache::CacheData> ConcatenatingCacheMachine::pullCacheData() {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  if (concat_all) {
    waitingCache->wait_until_finished();
  } else {
    waitingCache->wait_until_num_bytes(this->concat_cache_num_bytes,
                                       this->num_bytes_timeout);
  }

  size_t total_bytes = 0;
  std::vector<std::unique_ptr<message>> collected_messages;
  std::unique_ptr<message> message_data;
  std::string message_id;

  do {
    if (concat_all || waitingCache->has_next_now()) {
      message_data = waitingCache->pop_or_wait();
    } else {
      message_data = nullptr;
    }
    if (message_data == nullptr) {
      break;
    }
    auto& cache_data = message_data->get_data();
    total_bytes += cache_data.size_in_bytes();
    message_id = message_data->get_message_id();
    collected_messages.push_back(std::move(message_data));
  } while (concat_all || (total_bytes + waitingCache->get_next_size_in_bytes() <=
                          this->concat_cache_num_bytes));

  std::unique_ptr<ral::cache::CacheData> output;
  size_t num_rows = 0;
  if (collected_messages.empty()) {
    output = nullptr;
  } else if (collected_messages.size() == 1) {
    output = collected_messages[0]->release_data();
    num_rows = output->num_rows();
  } else {
    std::vector<std::unique_ptr<ral::cache::CacheData>> cache_datas;
    for (std::size_t i = 0; i < collected_messages.size(); i++) {
      cache_datas.push_back(collected_messages[i]->release_data());
    }

    output = std::make_unique<ConcatCacheData>(std::move(cache_datas),
                                               cache_datas[0]->column_names(),
                                               cache_datas[0]->get_schema());
    num_rows = output->num_rows();
  }

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->trace(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = total_bytes,
        "event_type"_a = "PullCacheData",
        "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a = "Pull cache data from ConcatenatingCacheMachine");
  }

  return output;
}


std::shared_ptr<CacheMachine> CacheMachine::make_single_machine(
    std::shared_ptr<Context> context, std::string cache_machine_name,
    bool log_timeout/* = true*/, int cache_level_override/* = -1*/,
    bool is_array_access/* = false*/)
{
  return  std::make_shared<CacheMachine>(context, cache_machine_name, log_timeout, cache_level_override, is_array_access);
}

std::shared_ptr<CacheMachine> CacheMachine::make_concatenating_machine(
    std::shared_ptr<Context> context, std::size_t concat_cache_num_bytes,
    int num_bytes_timeout, bool concat_all, std::string cache_machine_name)
{
  return  std::make_shared<ConcatenatingCacheMachine>(context, concat_cache_num_bytes, num_bytes_timeout, concat_all, cache_machine_name);
}

}  // namespace cache
}  // namespace ral
