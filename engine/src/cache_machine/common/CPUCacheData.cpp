#include "CPUCacheData.h"
#include "compute/api.h"
#include "parser/types_parser_utils.h"

namespace ral {
namespace cache {

CPUCacheData::CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, bool use_pinned)
	: CacheData(CacheDataType::CPU, table->column_names(), table->column_types(), table->num_rows())
{

	this->host_table = ral::execution::backend_dispatcher(table->get_execution_backend(), 
                                                    make_blazinghosttable_functor(), std::move(table), use_pinned);
}

CPUCacheData::CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table,const MetadataDictionary & metadata, bool use_pinned)
	: CacheData(CacheDataType::CPU, table->column_names(), table->column_types(), table->num_rows())
{
  this->host_table = ral::execution::backend_dispatcher(table->get_execution_backend(), 
                                                    make_blazinghosttable_functor(), std::move(table), use_pinned);
  this->metadata = metadata;
}

CPUCacheData::CPUCacheData(const std::vector<blazingdb::transport::ColumnTransport> & column_transports,
			std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
			std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations,
			const MetadataDictionary & metadata)  {

	this->cache_type = CacheDataType::CPU;
	for(int i = 0; i < column_transports.size(); i++){
		this->col_names.push_back(std::string(column_transports[i].metadata.col_name));
		this->schema.push_back(get_arrow_datatype_from_int_value(column_transports[i].metadata.dtype));
	}
	if(column_transports.size() == 0){
		this->n_rows = 0;
	}else{
		this->n_rows = column_transports[0].metadata.size;
	}
	this->host_table = std::make_unique<ral::frame::BlazingHostTable>(column_transports,std::move(chunked_column_infos), std::move(allocations));
	this->metadata = metadata;
}

CPUCacheData::CPUCacheData(std::unique_ptr<ral::frame::BlazingHostTable> host_table)
	: CacheData(CacheDataType::CPU, host_table->column_names(), host_table->column_types(), host_table->num_rows()), host_table{std::move(host_table)}
{
}

std::unique_ptr<CacheData> CPUCacheData::clone() {
	std::unique_ptr<ral::frame::BlazingHostTable> table = this->host_table->clone();
	return std::make_unique<CPUCacheData>(std::move(table));
}

} // namespace cache
} // namespace ral