#include <arrow/io/file.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <numeric>

#include "ArgsUtil.h"
#include "JSONParser.h"
#include "parser/types_parser_utils.h"
#include "compute/api.h"

namespace ral {
namespace io {

json_parser::json_parser(std::map<std::string, std::string> args_map_)
  : args_map{args_map_} {}

json_parser::~json_parser() {

}

std::unique_ptr<ral::frame::BlazingTable> json_parser::parse_batch(ral::execution::execution_backend preferred_compute,ral::io::data_handle handle,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups) {
	std::shared_ptr<arrow::io::RandomAccessFile> file = handle.file_handle;
	if(file == nullptr) {
		return schema.makeEmptyBlazingCudfTable(column_indices);
	}

	if(column_indices.size() > 0) {
    // TODO percy arrow c cordova use projection pushdown when cudf::orc reader has that feature too
    std::vector<std::string> col_names(column_indices.size());

    return ral::execution::backend_dispatcher(
          preferred_compute,
          io_read_file_data_functor<ral::io::DataType::JSON>(),
          file, column_indices, col_names, row_groups);
	}

	return nullptr;
}

void json_parser::parse_schema(ral::execution::execution_backend preferred_compute,ral::io::data_handle handle, ral::io::Schema & schema) {
  auto file = handle.file_handle;
  ral::execution::backend_dispatcher(
        preferred_compute,
        io_parse_file_schema_functor<ral::io::DataType::JSON>(),
        schema,
        file,
        this->args_map);
}

} /* namespace io */
} /* namespace ral */
