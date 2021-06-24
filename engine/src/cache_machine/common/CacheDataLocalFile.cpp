#include "CacheDataLocalFile.h"
#include <random>
#include "compute/backend_dispatcher.h"
#include <spdlog/logger.h>
#include <spdlog/spdlog.h>
#include "compute/api.h"

using namespace fmt::literals;


namespace ral {
namespace cache {

//TODO: Rommel Use randomeString from StringUtil
std::string randomString(std::size_t length) {
	const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	std::random_device random_device;
	std::mt19937 generator(random_device());
	std::uniform_int_distribution<> distribution(0, characters.size() - 1);

	std::string random_string;

	for(std::size_t i = 0; i < length; ++i) {
		random_string += characters[distribution(generator)];
	}

	return random_string;
}

CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path, std::string ctx_token)
	: CacheData(CacheDataType::LOCAL_FILE, table->column_names(), table->column_types(), table->num_rows())
{
	this->size_in_bytes_ = table->size_in_bytes();
	this->filePath_ = orc_files_path + "/.blazing-temp-" + ctx_token + "-" + randomString(64) + ".orc";

	// filling this->col_names
	for(auto name : table->column_names()) {
		this->col_names.push_back(name);
	}

	int attempts = 0;
	int attempts_limit = 10;
	std::shared_ptr<ral::frame::BlazingTableView> table_view = table->to_table_view();
	while(attempts <= attempts_limit){
		try {
			ral::execution::backend_dispatcher(table_view->get_execution_backend(), write_orc_functor(), table_view, this->filePath_);
			
		}
	#ifdef CUDF_SUPPORT
    catch (cudf::logic_error & err){
    #else
    catch (std::exception & err){
    #endif			
			std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
			if(logger) {
				logger->error("|||{info}||||rows|{rows}",
					"info"_a="Failed to create CacheDataLocalFile in path: " + this->filePath_ + " attempt " + std::to_string(attempts),
					"rows"_a=table->num_rows());
			}	
			attempts++;
			if (attempts == attempts_limit){
				throw;
			}
			std::this_thread::sleep_for (std::chrono::milliseconds(5 * attempts));
		}
	}
}

size_t CacheDataLocalFile::file_size_in_bytes() const {
	struct stat st;

	if(stat(this->filePath_.c_str(), &st) == 0)
		return (st.st_size);
	else
		throw;
}

size_t CacheDataLocalFile::size_in_bytes() const {
	return size_in_bytes_;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataLocalFile::decache(execution::execution_backend backend) {
  ral::execution::backend_dispatcher(backend, read_orc_functor(), this->filePath_, this->col_names);
}

std::unique_ptr<CacheData> CacheDataLocalFile::clone() {
	//Todo clone implementation
	throw std::runtime_error("CacheDataLocalFile::clone not implemented");
}

} // namespace cache
} // namespace ral