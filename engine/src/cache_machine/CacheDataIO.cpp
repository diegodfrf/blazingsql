#include "CacheDataIO.h"
#include "parser/CalciteExpressionParsing.h"
#include "parser/types_parser_utils.h"
#include <arrow/api.h>

// TODO percy arrow delete this include, we should not use details here
#include "compute/arrow/detail/types.h"

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

std::unique_ptr<ral::frame::BlazingTable> CacheDataIO::decache(execution::execution_backend backend){
  if (schema.all_in_file()){
    return parser->parse_batch(backend,handle, file_schema, projections, row_group_ids);
  } else {
    std::vector<int> column_indices_in_file;  // column indices that are from files
    for (auto projection_idx : projections){
      if(schema.get_in_file()[projection_idx]) {
        column_indices_in_file.push_back(projection_idx);
      }
    }

    std::vector<std::unique_ptr<cudf::column>> all_columns(projections.size());
    std::vector<std::unique_ptr<cudf::column>> file_columns;

    // TODO percy arrow
    std::vector<std::shared_ptr<arrow::ChunkedArray>> all_columns_arrow(projections.size());
    std::vector<std::shared_ptr<arrow::ChunkedArray>> file_columns_arrow;
    std::shared_ptr<const arrow::KeyValueMetadata> arrow_metadata;

    std::vector<std::string> names;
    cudf::size_type num_rows;
    if (column_indices_in_file.size() > 0){
      std::unique_ptr<ral::frame::BlazingTable> current_blazing_table = parser->parse_batch(backend, handle, file_schema, column_indices_in_file, row_group_ids);
      names = current_blazing_table->column_names();
      if (backend.id() == ral::execution::backend_id::CUDF) {
        ral::frame::BlazingCudfTable* current_blazing_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(current_blazing_table.get());
        std::unique_ptr<cudf::table> current_table = current_blazing_table_ptr->releaseCudfTable();
        num_rows = current_table->num_rows();
        file_columns = current_table->release();
      } else if (backend.id() == ral::execution::backend_id::ARROW) {
        ral::frame::BlazingArrowTable* current_blazing_table_ptr = dynamic_cast<ral::frame::BlazingArrowTable*>(current_blazing_table.get());
        std::shared_ptr<arrow::Table> current_table = current_blazing_table_ptr->view();
        num_rows = current_table->num_rows();
        file_columns_arrow = current_table->columns();
        arrow_metadata = current_table->schema()->metadata();
      }
    } else { // all tables we are "loading" are from hive partitions, so we dont know how many rows we need unless we load something to get the number of rows
      std::vector<int> temp_column_indices = {0};
      std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(backend, handle, file_schema, temp_column_indices, row_group_ids);
      num_rows = loaded_table->num_rows();
    }

    int in_file_column_counter = 0;
    for(std::size_t i = 0; i < projections.size(); i++) {
      int col_ind = projections[i];
      if(!schema.get_in_file()[col_ind]) {
        std::string name = schema.get_name(col_ind);
        names.push_back(name);
        arrow::Type::type type = schema.get_dtype(col_ind);
        std::string literal_str = handle.column_values[name];
        cudf::data_type cudf_type = arrow_type_to_cudf_data_type(type);
        if (backend.id() == ral::execution::backend_id::CUDF) {
          std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string(literal_str, cudf_type, false);
          all_columns[i] = cudf::make_column_from_scalar(*scalar, num_rows);
        } else if (backend.id() == ral::execution::backend_id::ARROW) {
          auto scalar = get_scalar_from_string_arrow(literal_str, cudf_type, false);
          std::shared_ptr<arrow::Array> temp = arrow::MakeArrayFromScalar(*scalar, num_rows).ValueOrDie();
          all_columns_arrow[i] = std::make_shared<arrow::ChunkedArray>(temp);
        }
      } else {
        if (backend.id() == ral::execution::backend_id::CUDF) {
          all_columns[i] = std::move(file_columns[in_file_column_counter]);
        } else if (backend.id() == ral::execution::backend_id::ARROW) {
          all_columns_arrow[i] = file_columns_arrow[in_file_column_counter];
        }
        in_file_column_counter++;
      }
    }

    if (backend.id() == ral::execution::backend_id::CUDF) {
      auto unique_table = std::make_unique<cudf::table>(std::move(all_columns));
      return std::make_unique<ral::frame::BlazingCudfTable>(std::move(unique_table), names);
    } else if (backend.id() == ral::execution::backend_id::ARROW) {
      auto new_schema = build_arrow_schema(all_columns_arrow, names, arrow_metadata);
      return std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(new_schema, all_columns_arrow, num_rows));
    }
  }

  return nullptr;
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