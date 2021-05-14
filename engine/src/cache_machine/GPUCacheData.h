#pragma once

#include "CacheData.h"

namespace ral {
namespace cache {

/**
* A CacheData that keeps its dataframe in GPU memory.
* This is a CacheData representation that wraps a ral::frame::BlazingTable. It
* is the most performant since its construction and decaching are basically
* no ops.
*/
class GPUCacheData : public CacheData {
public:
	GPUCacheData(const GPUCacheData& other) : data_(std::make_unique<ral::frame::BlazingCudfTable>(*other.data_)){
	}

	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	*/
	GPUCacheData(std::unique_ptr<ral::frame::BlazingCudfTable> table);

	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	* @param metadata The metadata that will be used in transport and planning.
	*/
	GPUCacheData(std::unique_ptr<ral::frame::BlazingCudfTable> table, const MetadataDictionary & metadata);

	/**
	* @brief Remove the payload from this CacheData.
	* This removes the payload for the CacheData. After this the CacheData will
	* almost always go out of scope and be destroyed.
	* @param backend the execution backend
	* @return a BlazingTable generated from the source of data for this CacheData. The type of BlazingTable returned will depend on the backend
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache(execution::execution_backend backend) override;

	/**
	* Get the amount of GPU memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingTable consumes.
	*/
	size_t size_in_bytes() const;

	/**
	* Set the names of the columns of a BlazingTable.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override;

	std::unique_ptr<CacheData> clone() override;

	/**
	* Destructor
	*/
	virtual ~GPUCacheData();

	/**
	* Get a ral::frame::BlazingTableView of the underlying data.
	* This allows you to read the data while it remains in cache.
	* @return a view of the data in this instance.
	*/
	std::shared_ptr<ral::frame::BlazingTableView> getTableView();

	void set_data(std::unique_ptr<ral::frame::BlazingCudfTable> table);

protected:
	std::unique_ptr<ral::frame::BlazingCudfTable> data_; /**< Stores the data to be returned in decache */
};

} // namespace cache
} // namespace ral