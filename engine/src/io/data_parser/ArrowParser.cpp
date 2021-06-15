#include "ArrowParser.h"
#include "arrow/api.h"
#include "blazing_table/BlazingArrowTable.h"

namespace ral {
namespace io {

arrow_parser::arrow_parser() {}

arrow_parser::~arrow_parser() {}

std::unique_ptr<ral::frame::BlazingTable> arrow_parser::parse_batch(ral::execution::execution_backend preferred_compute,
		ral::io::data_handle data_handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> /*row_groups*/)
{
  if(schema.get_num_columns() == 0) {
		return nullptr;
	}

  std::vector<int> all_indices;
  all_indices.resize(schema.get_num_columns());
  std::iota(all_indices.begin(), all_indices.end(), 0);
  bool is_the_same = (column_indices.size() == all_indices.size());
  if (is_the_same) {
    for (auto idx : all_indices) {
      if (column_indices[idx] != idx) {
        is_the_same = false;
        break;
      }
    }
  }

  if (is_the_same) {
    return std::make_unique<ral::frame::BlazingArrowTable>(data_handle.arrow_table);
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> cols;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (auto c : column_indices) {
    cols.push_back(data_handle.arrow_table->column(c));
    fields.push_back(data_handle.arrow_table->field(c));
  }

  auto new_schema = std::make_shared<arrow::Schema>(fields, data_handle.arrow_table->schema()->metadata());
	auto aa = std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(new_schema, cols, data_handle.arrow_table->num_rows()));
  
  return aa;
}

void arrow_parser::parse_schema(ral::execution::execution_backend preferred_compute,ral::io::data_handle /*handle*/,
		ral::io::Schema &  /*schema*/){
	std::vector<std::string> names;
	std::vector<cudf::type_id> types;
}

}
}
