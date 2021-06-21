#include "CacheDataLocalFile.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <iostream>
#include <random>
#include <spdlog/logger.h>
#include <spdlog/spdlog.h>
using namespace fmt::literals;

#include "compute/backend_dispatcher.h"

namespace ral {
namespace cache {

namespace {


  // TODO: Rommel Use randomeString from StringUtil
  std::string randomString(std::size_t length) {
    const std::string characters =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, characters.size() - 1);

    std::string random_string;

    for (std::size_t i = 0; i < length; ++i) {
      random_string += characters[distribution(generator)];
    }

    return random_string;
  }
}

ArrowCacheDataLocalFile::ArrowCacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table,
                                       std::string orc_files_path, std::string ctx_token)
    : CacheData(CacheDataType::LOCAL_FILE, table->column_names(), table->column_types(),
                table->num_rows()) {
  this->size_in_bytes_ = table->size_in_bytes();
  this->filePath_ = orc_files_path + "/.blazing-temp-" + ctx_token + "-" +
                    randomString(64) + ".parquet";

  // filling this->col_names
  for (auto name : table->column_names()) {
    this->col_names.push_back(name);
  }

  int attempts = 0;
  int attempts_limit = 10;
  std::shared_ptr<ral::frame::BlazingTableView> table_view = table->to_table_view();
  while (attempts <= attempts_limit) {
    try {
      std::shared_ptr<::arrow::io::FileOutputStream> outfile;
      PARQUET_ASSIGN_OR_THROW(outfile,
                              ::arrow::io::FileOutputStream::Open(this->filePath_));
      auto arrow_table_view =
          std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);

      parquet::arrow::WriteTable(*arrow_table_view->view(), ::arrow::default_memory_pool(),
                                 outfile, 3);
    } catch (std::exception& err) {
      std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
      if (logger) {
        logger->error("|||{info}||||rows|{rows}",
                      "info"_a = "Failed to create ArrowCacheDataLocalFile in path: " +
                                 this->filePath_ + " attempt " + std::to_string(attempts),
                      "rows"_a = table->num_rows());
      }
      attempts++;
      if (attempts == attempts_limit) {
        throw;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5 * attempts));
    }
  }
}

size_t ArrowCacheDataLocalFile::file_size_in_bytes() const {
  struct stat st;

  if (stat(this->filePath_.c_str(), &st) == 0)
    return (st.st_size);
  else
    throw;
}

size_t ArrowCacheDataLocalFile::size_in_bytes() const { return size_in_bytes_; }

std::unique_ptr<ral::frame::BlazingTable> ArrowCacheDataLocalFile::decache(execution::execution_backend backend) {
  RAL_EXPECTS(backend.id() == ral::execution::backend_id::ARROW, "decache from arrow");

  std::shared_ptr<::arrow::io::ReadableFile> infile;
  const char *orc_path_file = this->filePath_.c_str();
  PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(orc_path_file, ::arrow::default_memory_pool()));

  std::unique_ptr<::parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(::parquet::arrow::OpenFile(infile, ::arrow::default_memory_pool(), &reader));
  std::shared_ptr<::arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

  remove(orc_path_file);
  return std::make_unique<ral::frame::BlazingArrowTable>(table);

}

std::unique_ptr<CacheData> ArrowCacheDataLocalFile::clone() {
  // Todo clone implementation
  throw std::runtime_error("ArrowCacheDataLocalFile::clone not implemented");
}
}  // namespace cache
}  // namespace ral