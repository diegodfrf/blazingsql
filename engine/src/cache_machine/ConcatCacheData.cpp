#include "ConcatCacheData.h"
#include "compute/backend_dispatcher.h"
#include "compute/api.h"
#include "operators/Concatenate.h"

namespace ral {
namespace cache {

ConcatCacheData::ConcatCacheData(std::vector<std::unique_ptr<CacheData>> cache_datas,
	const std::vector<std::string>& col_names,
	const std::vector<std::shared_ptr<arrow::DataType>>& schema)
	: CacheData(CacheDataType::CONCATENATING, col_names, std::move(schema), 0), _cache_datas{std::move(cache_datas)} {
	n_rows = 0;
	for (auto && cache_data : _cache_datas) {
		auto cache_schema = cache_data->get_schema();
		//RAL_EXPECTS(std::equal(schema.begin(), schema.end(), cache_schema.begin()), "Cache data has a different schema");
		n_rows += cache_data->num_rows();
	}
}

std::unique_ptr<ral::frame::BlazingTable> ConcatCacheData::decache(execution::execution_backend backend) {
	if(_cache_datas.empty()) {
		return ral::execution::backend_dispatcher(backend,
													create_empty_table_functor(),
													col_names, schema);		
	}

	if (_cache_datas.size() == 1)	{
		return _cache_datas[0]->decache(backend);
	}

	std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_holder;
	std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views;
	for (auto && cache_data : _cache_datas){
		tables_holder.push_back(cache_data->decache(backend));
		table_views.push_back(tables_holder.back()->to_table_view());

		RAL_EXPECTS(!checkIfConcatenatingStringsWillOverflow(table_views), "Concatenating tables will overflow");
	}

	std::unique_ptr<ral::frame::BlazingTable> temp_out =  concatTables(table_views);
	return temp_out;
}

size_t ConcatCacheData::size_in_bytes() const {
	size_t total_size = 0;
	for (auto && cache_data : _cache_datas) {
		total_size += cache_data->size_in_bytes();
	}
	return total_size;
};

void ConcatCacheData::set_names(const std::vector<std::string> & names) {
	for (size_t i = 0; i < _cache_datas.size(); ++i) {
		_cache_datas[i]->set_names(names);
	}
}

std::unique_ptr<CacheData> ConcatCacheData::clone() {
	//Todo clone implementation
	throw std::runtime_error("ConcatCacheData::clone not implemented");
}

std::vector<std::unique_ptr<CacheData>> ConcatCacheData::releaseCacheDatas(){
	return std::move(_cache_datas);
}

} // namespace cache
} // namespace ral