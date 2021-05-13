#pragma once

#include "CacheData.h"
#include <arrow/table.h>

namespace ral {
namespace cache {

/**
* A CacheData that keeps its dataframe as an Arrow table.
* This is a CacheData representation that wraps a arrow::Table.
*/
class ArrowCacheData : public CacheData {
public:

	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	*/
	ArrowCacheData(std::unique_ptr<ral::frame::BlazingArrowTable> table)
		: CacheData(CacheDataType::ARROW,table->column_names(), table->column_types(), table->num_rows()),  data_{std::move(table)} {
			
		}

	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	* @param metadata The metadata that will be used in transport and planning.
	*/
	ArrowCacheData(std::unique_ptr<ral::frame::BlazingArrowTable> table, const MetadataDictionary & metadata)
		: CacheData(CacheDataType::ARROW,table->column_names(), table->column_types(), table->num_rows()),  data_{std::move(table)} {
			this->metadata = metadata;
		}

	/**
	* @brief Remove the payload from this CacheData.
	* This removes the payload for the CacheData. After this the CacheData will
	* almost always go out of scope and be destroyed.
	* @param backend the execution backend
	* @return a BlazingTable generated from the source of data for this CacheData. The type of BlazingTable returned will depend on the backend
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache(execution::execution_backend backend) override;

	/**
	* Get the amount of memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingTable consumes.
	*/
	size_t size_in_bytes() const override;

	/**
	* Set the names of the columns of a BlazingTable.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override;

	/**
	* Destructor
	*/
	virtual ~ArrowCacheData();

private:
	std::unique_ptr<ral::frame::BlazingArrowTable> data_;
};

} // namespace cache
} // namespace ral