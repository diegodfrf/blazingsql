#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <map>

#include <spdlog/spdlog.h>
#include "cudf/types.hpp"
#include "utilities/error.hpp"
#include "utilities/CodeTimer.h"
#include "blazing_table/BlazingTable.h"
#include "execution_graph/Context.h"
#include <bmr/BlazingMemoryResource.h>
#include "communication/CommunicationData.h"
#include <exception>
#include "io/data_provider/DataProvider.h"
#include "io/data_parser/DataParser.h"

#include "GPUCacheData.h"
#include "ArrowCacheData.h"
#include "communication/messages/GPUComponentMessage.h"
#include "CacheData.h"
#include "WaitingQueue.h"

namespace ral {
namespace cache {

using Context = blazingdb::manager::Context;
using namespace fmt::literals;

const int CACHE_LEVEL_AUTO = -1;
const int CACHE_LEVEL_GPU = 0;
const int CACHE_LEVEL_CPU = 1;
const int CACHE_LEVEL_DISK = 2;


/**
	@brief A class that represents a Cache Machine on a
	multi-tier (GPU memory, CPU memory, Disk memory) cache system.
*/
class CacheMachine {
public:
	CacheMachine(std::shared_ptr<Context> context, std::string cache_machine_name, bool log_timeout = true, int cache_level_override = -1, bool is_array_access = false);

	~CacheMachine();

	virtual void put(size_t index, std::unique_ptr<ral::frame::BlazingTable> table);

	virtual void put(size_t index, std::unique_ptr<ral::cache::CacheData> cacheData);

	virtual std::unique_ptr<ral::frame::BlazingTable> get_or_wait(execution::execution_backend backend, size_t index);

	virtual std::unique_ptr<ral::cache::CacheData> get_or_wait_CacheData(size_t index);

	virtual void clear();

	virtual bool addToCache(std::unique_ptr<ral::frame::BlazingTable> table, std::string message_id = "", bool always_add = false, const MetadataDictionary & metadata = {}, bool use_pinned = false );

	virtual bool addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data, std::string message_id = "", bool always_add = false);

	virtual bool addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> table, std::string message_id = "");

	virtual void finish();

	virtual bool is_finished();

	uint64_t get_num_bytes_added();

	uint64_t get_num_rows_added();

	uint64_t get_num_batches_added();

	void wait_until_finished();

	std::int32_t get_id() const;

	Context * get_context() const;

	bool wait_for_next() {
		return this->waitingCache->wait_for_next();
	}

	bool has_next_now() {
		return this->waitingCache->has_next_now();
	}

	bool has_messages_now(std::vector<std::string> messages);

	std::unique_ptr<ral::cache::CacheData> pullAnyCacheData(const std::vector<std::string> & messages);

	std::size_t get_num_batches(){
		return cache_count;
	}

  virtual std::unique_ptr<ral::frame::BlazingTable> pullFromCache(execution::execution_backend backend);

	virtual std::unique_ptr<ral::frame::BlazingTable> pullUnorderedFromCache(execution::execution_backend backend);

	std::vector<std::unique_ptr<ral::cache::CacheData> > pull_all_cache_data();

	virtual std::unique_ptr<ral::cache::CacheData> pullCacheData(std::string message_id);

	virtual std::unique_ptr<ral::cache::CacheData> pullCacheData();

	virtual std::unique_ptr<ral::cache::CacheData> pullCacheDataCopy();

	std::vector<size_t> get_all_indexes();

	void wait_for_count(int count){
		return this->waitingCache->wait_for_count(count);
	}
	// take the first cacheData in this CacheMachine that it can find (looking in reverse order) that is in the GPU put it in RAM or Disk as oppropriate
	// this function does not change the order of the caches
	virtual size_t downgradeGPUCacheData();

    bool has_data_in_index_now(size_t index);

protected:
	static std::size_t cache_count;

	/// This property represents a waiting queue object which stores all CacheData Objects
	std::unique_ptr<WaitingQueue< std::unique_ptr<message> > > waitingCache;

	/// References to the properties of the multi-tier cache system
	std::vector<BlazingMemoryResource*> memory_resources;
	std::atomic<std::size_t> num_bytes_added;
	std::atomic<uint64_t> num_rows_added;
	/// This variable is to keep track of if anything has been added to the cache. Its useful to keep from adding empty tables to the cache, where we might want an empty table at least to know the schema
	bool something_added;
	std::shared_ptr<Context> ctx;
	const std::size_t cache_id;
	int cache_level_override;
	std::string cache_machine_name;
	std::shared_ptr<spdlog::logger> cache_events_logger;
    bool is_array_access;
    int global_index;
};


/**
	@brief A class that represents a Cache Machine on a
	multi-tier cache system. Moreover, it only returns a single BlazingTable by concatenating all batches.
	This Cache Machine is used in the last Kernel (OutputKernel) in the ExecutionGraph.

	This ConcatenatingCacheMachine::pullFromCache method does not guarantee the relative order
	of the messages to be preserved
*/
class ConcatenatingCacheMachine : public CacheMachine {
public:
	ConcatenatingCacheMachine(std::shared_ptr<Context> context, std::string cache_machine_name);

	ConcatenatingCacheMachine(std::shared_ptr<Context> context,
			std::size_t concat_cache_num_bytes, int num_bytes_timeout, bool concat_all, std::string cache_machine_name);

	~ConcatenatingCacheMachine() = default;

	std::unique_ptr<ral::frame::BlazingTable> pullFromCache(execution::execution_backend backend) override;

	std::unique_ptr<ral::frame::BlazingTable> pullUnorderedFromCache(execution::execution_backend backend) override {
		return pullFromCache(backend);
	}

	std::unique_ptr<ral::cache::CacheData> pullCacheData() override;

	size_t downgradeGPUCacheData() override { // dont want to be able to downgrage concatenating caches
		return 0;
	}

  private:
  	std::size_t concat_cache_num_bytes;
	int num_bytes_timeout;
	bool concat_all;

};

struct make_cachedata_functor {
	template <typename T>
	std::unique_ptr<CacheData> operator()(std::unique_ptr<ral::frame::BlazingTable> table){
		return nullptr;
	}
};

template<>
inline
std::unique_ptr<CacheData> make_cachedata_functor::operator()<ral::frame::BlazingArrowTable>(std::unique_ptr<ral::frame::BlazingTable> table){
	std::unique_ptr<ral::frame::BlazingArrowTable> arrow_table(dynamic_cast<ral::frame::BlazingArrowTable*>(table.release()));
	return std::make_unique<ArrowCacheData>(std::move(arrow_table));
}

template<>
inline
std::unique_ptr<CacheData> make_cachedata_functor::operator()<ral::frame::BlazingCudfTable>(std::unique_ptr<ral::frame::BlazingTable> table){
	std::unique_ptr<ral::frame::BlazingCudfTable> cudf_table(dynamic_cast<ral::frame::BlazingCudfTable*>(table.release()));
	return std::make_unique<GPUCacheData>(std::move(cudf_table));
}

}  // namespace cache
}  // namespace ral
