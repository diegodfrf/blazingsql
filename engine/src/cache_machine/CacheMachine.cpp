
#include "cache_machine/CacheMachine.h"

#include <Util/StringUtil.h>
#include <stdio.h>
#include <sys/stat.h>

#include <random>

#include "Util/StringUtil.h"
#include "cache_machine/common/CPUCacheData.h"
#include "cache_machine/common/ConcatCacheData.h"
#include "communication/CommunicationData.h"
#include "compute/backend_dispatcher.h"
#include "operators/Concatenate.h"
#include "parser/CalciteExpressionParsing.h"
#include "compute/api.h"
#include "cache_machine_api.h"

namespace ral {
namespace cache {

using namespace fmt::literals;

std::size_t AbstractCacheMachine::cache_count(900000000);

AbstractCacheMachine::AbstractCacheMachine(std::shared_ptr<Context> context,
                                           std::string cache_machine_name,
                                           bool log_timeout, int cache_level_override,
                                           bool is_array_access)
    : ctx(context),
      cache_id(AbstractCacheMachine::cache_count),
      cache_machine_name(cache_machine_name),
      cache_level_override(cache_level_override),
      cache_events_logger(spdlog::get("cache_events_logger")),
      is_array_access(is_array_access),
      global_index(-1) {
  AbstractCacheMachine::cache_count++;

  waitingCache = std::make_unique<WaitingQueue<std::unique_ptr<message>>>(
      cache_machine_name, 60000, log_timeout);
  this->num_bytes_added = 0;
  this->num_rows_added = 0;

  this->something_added = false;

  std::shared_ptr<spdlog::logger> kernels_logger = spdlog::get("kernels_logger");
  if (kernels_logger) {
    kernels_logger->info(
        "{ral_id}|{query_id}|{kernel_id}|{is_kernel}|{kernel_type}|{description}",
        "ral_id"_a =
            (context
                 ? context->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (context ? std::to_string(context->getContextToken()) : "null"),
        "kernel_id"_a = cache_id,
        "is_kernel"_a = 0,  // false
        "kernel_type"_a = "cache", "description"_a = cache_machine_name);
  }
}

std::vector<std::unique_ptr<ral::cache::CacheData>>
AbstractCacheMachine::pull_all_cache_data() {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  auto messages = this->waitingCache->get_all();
  std::vector<std::unique_ptr<ral::cache::CacheData>> new_messages(messages.size());
  int i = 0;
  for (auto& message_data : messages) {
    new_messages[i] = message_data->release_data();
    i++;
  }

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1),
        "message_id"_a = cache_machine_name, "cache_id"_a = cache_id,
        "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
        "event_type"_a = "PullAllCacheData",
        "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a = "Pull all cache data");
  }

  return new_messages;
}

bool AbstractCacheMachine::is_finished() { return this->waitingCache->is_finished(); }

void AbstractCacheMachine::finish() {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();
  this->waitingCache->finish();

  cacheEventTimer.stop();
  if (this->cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1),
        "message_id"_a = cache_machine_name, "cache_id"_a = cache_id,
        "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
        "event_type"_a = "Finish", "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a = "CacheMachine finish()");
  }
}

uint64_t AbstractCacheMachine::get_num_bytes_added() { return num_bytes_added.load(); }

uint64_t AbstractCacheMachine::get_num_rows_added() { return num_rows_added.load(); }

uint64_t AbstractCacheMachine::get_num_batches_added() {
  return this->waitingCache->processed_parts();
}

void AbstractCacheMachine::wait_until_finished() {
  return waitingCache->wait_until_finished();
}

std::int32_t AbstractCacheMachine::get_id() const { return (cache_id); }

Context* AbstractCacheMachine::get_context() const { return ctx.get(); }

bool AbstractCacheMachine::wait_for_next() { return this->waitingCache->wait_for_next(); }

bool AbstractCacheMachine::has_next_now() { return this->waitingCache->has_next_now(); }

bool AbstractCacheMachine::has_messages_now(std::vector<std::string> messages) {
  std::vector<std::string> current_messages = this->waitingCache->get_all_message_ids();
  for (auto& message : messages) {
    bool found = false;
    for (auto& cur_message : current_messages) {
      if (message == cur_message) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }
  }
  return true;
}

std::unique_ptr<ral::cache::CacheData> AbstractCacheMachine::pullAnyCacheData(
    const std::vector<std::string>& messages) {
  if (messages.size() == 0) {
    return nullptr;
  }

  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  std::unique_ptr<message> message_data = waitingCache->get_or_wait_any(messages);
  std::string message_id = message_data->get_message_id();

  size_t num_rows = message_data->get_data().num_rows();
  size_t num_bytes = message_data->get_data().size_in_bytes();
  int dataType = static_cast<int>(message_data->get_data().get_type());
  std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = num_bytes,
        "event_type"_a = "pullAnyCacheData",
        "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a =
            "Pull from CacheMachine CacheData object type {}"_format(dataType));
  }

  return output;
}

std::size_t AbstractCacheMachine::get_num_batches() { return cache_count; }

std::vector<size_t> AbstractCacheMachine::get_all_indexes() {
  std::vector<std::string> message_ids = this->waitingCache->get_all_message_ids();
  std::vector<size_t> indexes;
  indexes.reserve(message_ids.size());
  for (auto& message_id : message_ids) {
    std::string prefix = this->cache_machine_name + "_";
    assert(StringUtil::beginsWith(message_id, prefix));
    indexes.push_back(std::stoi(message_id.substr(prefix.size())));
  }
  return indexes;
}

void AbstractCacheMachine::wait_for_count(int count) {
  return this->waitingCache->wait_for_count(count);
}

bool AbstractCacheMachine::has_data_in_index_now(size_t index) {
  std::string message = this->cache_machine_name + "_" + std::to_string(index);
  std::vector<std::string> current_messages = this->waitingCache->get_all_message_ids();
  bool found = false;
  for (auto& cur_message : current_messages) {
    if (message == cur_message) {
      found = true;
      break;
    }
  }
  return found;
}

void AbstractCacheMachine::clear() {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  auto messages = this->waitingCache->get_all();
  this->waitingCache->finish();

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1),
        "message_id"_a = cache_machine_name, "cache_id"_a = cache_id,
        "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
        "event_type"_a = "Clear", "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a = "Clear CacheMachine");
  }
}

std::unique_ptr<ral::frame::BlazingTable> AbstractCacheMachine::get_or_wait(execution::execution_backend backend, size_t index) {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  std::unique_ptr<message> message_data =
      waitingCache->get_or_wait(this->cache_machine_name + "_" + std::to_string(index));
  if (message_data == nullptr) {
    return nullptr;
  }
  std::string message_id = message_data->get_message_id();
  size_t num_rows = message_data->get_data().num_rows();
  size_t num_bytes = message_data->get_data().size_in_bytes();
  std::unique_ptr<ral::frame::BlazingTable> output =
      message_data->get_data().decache(backend);

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = num_bytes,
        "event_type"_a = "GetOrWait", "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a = "CudfCacheMachine::get_or_wait pulling from cache ");
  }

  return output;
}

std::unique_ptr<ral::cache::CacheData> AbstractCacheMachine::get_or_wait_CacheData(size_t index) {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  std::unique_ptr<message> message_data =
      waitingCache->get_or_wait(this->cache_machine_name + "_" + std::to_string(index));
  if (message_data == nullptr) {
    return nullptr;
  }
  std::string message_id = message_data->get_message_id();
  size_t num_rows = message_data->get_data().num_rows();
  size_t num_bytes = message_data->get_data().size_in_bytes();
  std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = num_bytes,
        "event_type"_a = "GetOrWaitCacheData",
        "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a = "CudfCacheMachine::get_or_wait pulling CacheData from cache");
  }
  return output;
}

bool AbstractCacheMachine::addHostFrameToCache(
    std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string message_id) {
  // we dont want to add empty tables to a cache, unless we have never added anything
  if (!this->something_added || host_table->num_rows() > 0) {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

    num_rows_added += host_table->num_rows();
    num_bytes_added += host_table->size_in_bytes();

    if (message_id == "") {
      message_id = this->cache_machine_name;
    }

    auto cache_data = std::make_unique<CPUCacheData>(std::move(host_table));
    auto item = std::make_unique<message>(std::move(cache_data), message_id);
    this->waitingCache->put(std::move(item));
    this->something_added = true;

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
          "message_id"_a = cache_machine_name, "cache_id"_a = cache_id,
          "num_rows"_a = num_rows_added, "num_bytes"_a = num_bytes_added,
          "event_type"_a = "AddHostFrameToCache",
          "timestamp_begin"_a = cacheEventTimer.start_time(),
          "timestamp_end"_a = cacheEventTimer.end_time(),
          "description"_a = "Add to CacheMachine");
    }

    return true;
  }

  return false;
}

std::unique_ptr<ral::frame::BlazingTable> AbstractCacheMachine::pullFromCache(
    execution::execution_backend backend) {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  std::string message_id;

  std::unique_ptr<message> message_data = nullptr;
  if (is_array_access) {
    message_id = this->cache_machine_name + "_" + std::to_string(++global_index);
    message_data = waitingCache->get_or_wait(message_id);
  } else {
    message_data = waitingCache->pop_or_wait();
    message_id = message_data->get_message_id();
  }

  if (message_data == nullptr) {
    return nullptr;
  }

  size_t num_rows = message_data->get_data().num_rows();
  size_t num_bytes = message_data->get_data().size_in_bytes();
  int dataType = static_cast<int>(message_data->get_data().get_type());
  std::unique_ptr<ral::frame::BlazingTable> output =
      message_data->get_data().decache(backend);

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = num_bytes,
        "event_type"_a = "PullFromCache",
        "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a = "Pull from CacheMachine type {}"_format(dataType));
  }

  return output;
}

std::unique_ptr<ral::cache::CacheData> AbstractCacheMachine::pullCacheData(
    std::string message_id) {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  std::unique_ptr<message> message_data = waitingCache->get_or_wait(message_id);
  if (message_data == nullptr) {
    return nullptr;
  }
  size_t num_rows = message_data->get_data().num_rows();
  size_t num_bytes = message_data->get_data().size_in_bytes();
  int dataType = static_cast<int>(message_data->get_data().get_type());
  std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = num_bytes,
        "event_type"_a = "PullCacheData",
        "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a =
            "Pull from CacheMachine CacheData object type {}"_format(dataType));
  }
  return output;
}

std::unique_ptr<ral::cache::CacheData> AbstractCacheMachine::pullCacheDataCopy() {
  std::unique_ptr<ral::cache::CacheData> output;
  if (this->waitingCache->wait_for_next()) {
    std::unique_lock<std::mutex> lock = this->waitingCache->lock();
    std::vector<std::unique_ptr<message>> all_messages =
        this->waitingCache->get_all_unsafe();
    output = all_messages[0]->clone();
    this->waitingCache->put_all_unsafe(std::move(all_messages));
  } else {
    return nullptr;
  }

  return output;
}

std::unique_ptr<ral::frame::BlazingTable> AbstractCacheMachine::pullUnorderedFromCache(
    execution::execution_backend backend) {
  if (is_array_access) {
    return this->pullFromCache(backend);
  }

  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  std::unique_ptr<message> message_data = nullptr;
  {  // scope for lock
    std::unique_lock<std::mutex> lock = this->waitingCache->lock();
    std::vector<std::unique_ptr<message>> all_messages =
        this->waitingCache->get_all_unsafe();
    std::vector<std::unique_ptr<message>> remaining_messages;
    for (size_t i = 0; i < all_messages.size(); i++) {
      if (all_messages[i]->get_data().get_type() == CacheDataType::GPU &&
          message_data == nullptr) {
        message_data = std::move(all_messages[i]);
      } else {
        remaining_messages.push_back(std::move(all_messages[i]));
      }
    }
    this->waitingCache->put_all_unsafe(std::move(remaining_messages));
  }
  if (message_data) {
    std::string message_id = message_data->get_message_id();
    size_t num_rows = message_data->get_data().num_rows();
    size_t num_bytes = message_data->get_data().size_in_bytes();
    int dataType = static_cast<int>(message_data->get_data().get_type());
    std::unique_ptr<ral::frame::BlazingTable> output =
        message_data->get_data().decache(backend);

    cacheEventTimer.stop();
    if (cache_events_logger) {
      cache_events_logger->info(
          "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_"
          "type}|{timestamp_begin}|{timestamp_end}|{description}",
          "ral_id"_a = (ctx ? ctx->getNodeIndex(
                                  ral::communication::CommunicationData::getInstance()
                                      .getSelfNode())
                            : -1),
          "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
          "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = num_bytes,
          "event_type"_a = "PullUnorderedFromCache",
          "timestamp_begin"_a = cacheEventTimer.start_time(),
          "timestamp_end"_a = cacheEventTimer.end_time(),
          "description"_a = "Pull Unordered from CacheMachine type {}"_format(dataType));
    }

    return output;
  } else {
    return pullFromCache(backend);
  }
}

std::unique_ptr<ral::cache::CacheData> AbstractCacheMachine::pullCacheData() {
  CodeTimer cacheEventTimer;
  cacheEventTimer.start();

  std::unique_ptr<message> message_data = nullptr;
  std::string message_id;

  if (is_array_access) {
    message_id = this->cache_machine_name + "_" + std::to_string(++global_index);
    message_data = waitingCache->get_or_wait(message_id);
  } else {
    message_data = waitingCache->pop_or_wait();
    if (message_data == nullptr) {
      return nullptr;
    }
    message_id = message_data->get_message_id();
  }

  size_t num_rows = message_data->get_data().num_rows();
  size_t num_bytes = message_data->get_data().size_in_bytes();
  int dataType = static_cast<int>(message_data->get_data().get_type());
  std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

  cacheEventTimer.stop();
  if (cache_events_logger) {
    cache_events_logger->info(
        "{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|"
        "{timestamp_begin}|{timestamp_end}|{description}",
        "ral_id"_a =
            (ctx ? ctx->getNodeIndex(
                       ral::communication::CommunicationData::getInstance().getSelfNode())
                 : -1),
        "query_id"_a = (ctx ? ctx->getContextToken() : -1), "message_id"_a = message_id,
        "cache_id"_a = cache_id, "num_rows"_a = num_rows, "num_bytes"_a = num_bytes,
        "event_type"_a = "PullCacheData",
        "timestamp_begin"_a = cacheEventTimer.start_time(),
        "timestamp_end"_a = cacheEventTimer.end_time(),
        "description"_a =
            "Pull from CacheMachine CacheData object type {}"_format(dataType));
  }

  return output;
}

///////////////////////////////////////////////////////////////////////////////////////
CacheMachine::CacheMachine(std::shared_ptr<Context> context,
                           std::string cache_machine_name, bool log_timeout,
                           int cache_level_override, bool is_array_access)
    : AbstractCacheMachine(context, cache_machine_name, log_timeout, cache_level_override,
                           is_array_access) {}

///////////////////////////////////////////////////////////////////////////////////////

DEFINE_DISPATCH(make_single_machine_stub);
DEFINE_DISPATCH(make_concatenating_machine_stub);

std::shared_ptr<CacheMachine> CacheMachine::make_single_machine(
    std::shared_ptr<Context> context, std::string cache_machine_name,
    bool log_timeout/* = true*/, int cache_level_override/* = -1*/,
    bool is_array_access/* = false*/)
{
  return  make_single_machine_stub(context->preferred_compute(), context, cache_machine_name, log_timeout, cache_level_override, is_array_access);
}

std::shared_ptr<CacheMachine> CacheMachine::make_concatenating_machine(
    std::shared_ptr<Context> context, std::size_t concat_cache_num_bytes,
    int num_bytes_timeout, bool concat_all, std::string cache_machine_name)
{
  return  make_concatenating_machine_stub(context->preferred_compute(), context, concat_cache_num_bytes, num_bytes_timeout, concat_all, cache_machine_name);
}

}  // namespace cache
}  // namespace ral
