#include "OrcParser.h"
#include "metadata/orc_metadata.h"
#include "parser/types_parser_utils.h"

#include <arrow/io/file.h>

#include <blazingdb/io/Library/Logging/Logger.h>

#include <numeric>
#include "compute/api.h"

namespace ral {
namespace io {

orc_parser::orc_parser(std::map<std::string, std::string> args_map_)
  : args_map{args_map_} {}

orc_parser::~orc_parser() {
	// TODO Auto-generated destructor stub
}

std::unique_ptr<ral::frame::BlazingTable> orc_parser::parse_batch(ral::execution::execution_backend preferred_compute,
	ral::io::data_handle handle,
	const Schema & schema,
	std::vector<int> column_indices,
	std::vector<cudf::size_type> row_groups)
{
	std::shared_ptr<arrow::io::RandomAccessFile> file = handle.file_handle;
	if(file == nullptr) {
    return ral::execution::backend_dispatcher(preferred_compute,
                                           create_empty_table_functor(),
                                           schema.get_names(), schema.get_dtypes(), column_indices);
	}
	if(column_indices.size() > 0) {
    std::vector<std::string> col_names;
    col_names.resize(column_indices.size());
  
    for(size_t column_i = 0; column_i < column_indices.size(); column_i++) {
      col_names[column_i] = schema.get_name(column_indices[column_i]);
    }

    return ral::execution::backend_dispatcher(
          preferred_compute,
          io_read_file_data_functor<ral::io::DataType::ORC>(),
          file, column_indices, col_names, row_groups);
	}
	return nullptr;
}

void orc_parser::parse_schema(ral::execution::execution_backend preferred_compute,
	ral::io::data_handle handle, ral::io::Schema & schema)
{
  auto file = handle.file_handle;

  ral::execution::backend_dispatcher(
        preferred_compute,
        io_parse_file_schema_functor<ral::io::DataType::ORC>(),
        schema,
        file,
        this->args_map);
}

std::unique_ptr<ral::frame::BlazingTable> orc_parser::get_metadata(ral::execution::execution_backend preferred_compute,
	std::vector<ral::io::data_handle> handles, int offset,
	std::map<std::string, std::string> args_map)
{
	std::vector<size_t> num_stripes(handles.size());
	std::vector<cudf::io::parsed_orc_statistics> statistics(handles.size());
	for(size_t file_index = 0; file_index < handles.size(); file_index++) {
		auto arrow_source = cudf::io::arrow_io_source{handles[file_index].file_handle};
		statistics[file_index] = cudf::io::read_parsed_orc_statistics(cudf::io::source_info{&arrow_source});
		num_stripes[file_index] = statistics[file_index].stripes_stats.size();
	}
	size_t total_num_stripes = std::accumulate(num_stripes.begin(), num_stripes.end(), size_t(0));

	std::unique_ptr<ral::frame::BlazingTable> minmax_metadata_table = get_minmax_metadata(statistics, total_num_stripes, offset);

	return minmax_metadata_table;
}

} /* namespace io */
} /* namespace ral */
