#include "CacheDataLocalFile.h"
#include <random>
#include "cudf/types.hpp" //cudf::io::metadata
#include <cudf/io/orc.hpp>
#include "execution_graph/backend_dispatcher.h"

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

//////////////////////////////////// write_orc_functor

struct write_orc_functor {
  template <typename T>
  void operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::string file_path) const
  {
    // TODO percy arrow thrown error
    throw std::runtime_error("ERROR: This default dispatcher operator should not be called.");
  }
};

template <>
inline void write_orc_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::string file_path) const
{
  // TODO WSM arrow
  throw std::runtime_error("ERROR: BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline void write_orc_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::string file_path) const
{
	auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);  

	cudf::io::table_metadata metadata;
	for(auto name : cudf_table_view->column_names()) {
		metadata.column_names.emplace_back(name);
	}

	cudf::io::orc_writer_options out_opts = cudf::io::orc_writer_options::builder(cudf::io::sink_info{file_path}, cudf_table_view->view())
		.metadata(&metadata);

	cudf::io::write_orc(out_opts);
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

			
		} catch (cudf::logic_error & err){
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

	if (backend.id() == ral::execution::backend_id::CUDF) {
		cudf::io::orc_reader_options read_opts = cudf::io::orc_reader_options::builder(cudf::io::source_info{this->filePath_});
		auto result = cudf::io::read_orc(read_opts);

		// Remove temp orc files
		const char *orc_path_file = this->filePath_.c_str();
		remove(orc_path_file);
		return std::make_unique<ral::frame::BlazingCudfTable>(std::move(result.tbl), this->col_names);
	} else {
		// WSM TODO need to implement this
	}
}

} // namespace cache
} // namespace ral