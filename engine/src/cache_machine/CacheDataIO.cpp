#include "CacheDataIO.h"
#include "parser/CalciteExpressionParsing.h"
#include "parser/types_parser_utils.h"
#include <arrow/api.h>

// TODO percy arrow delete this include, we should not use details here
//#include "compute/arrow/detail/types.h"

#include "compute/api.h"

namespace ral {
namespace cache {

CacheDataIO::CacheDataIO(ral::io::data_handle handle,
	std::shared_ptr<ral::io::data_parser> parser,
	ral::io::Schema schema,
	ral::io::Schema file_schema,
	std::vector<int> row_group_ids,
	std::vector<int> projections)
	: CacheData(CacheDataType::IO_FILE, schema.get_names(), schema.get_data_types(), 1),
	handle(handle), parser(parser), schema(schema),
	file_schema(file_schema), row_group_ids(row_group_ids),
	projections(projections)
	{

	}

size_t CacheDataIO::size_in_bytes() const{
	return 0;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataIO::decache(execution::execution_backend backend)
{
    if (schema.all_in_file()){
      return parser->parse_batch(backend,handle, file_schema, projections, row_group_ids);
    } else {
      std::vector<int> column_indices_in_file;  // column indices that are from files
      for (auto projection_idx : projections){
        if(schema.get_in_file()[projection_idx]) {
          column_indices_in_file.push_back(projection_idx);
        }
      }

      std::unique_ptr<ral::frame::BlazingTable> table = parser->parse_batch(backend, handle, file_schema, column_indices_in_file, row_group_ids);

      std::unique_ptr<ral::frame::BlazingTable> table_to_return = ral::execution::backend_dispatcher(
                                                                  backend,
                                                                  decache_io_functor(),
                                                                  std::move(table),
                                                                  this->projections,
                                                                  this->schema,
                                                                  column_indices_in_file,
                                                                  handle.column_values);

      return table_to_return;
   }
}

void CacheDataIO::set_names(const std::vector<std::string> & names) {
	this->schema.set_names(names);
}

std::unique_ptr<CacheData> CacheDataIO::clone() {
	//Todo clone implementation
	throw std::runtime_error("CacheDataIO::clone not implemented");
}

ral::io::DataType CacheDataIO::GetParserType() {
	return this->parser->type();
}

} // namespace cache
} // namespace ral
