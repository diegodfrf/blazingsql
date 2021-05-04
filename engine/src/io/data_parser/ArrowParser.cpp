#include "ArrowParser.h"
#include <cudf/interop.hpp>
#include "arrow/api.h"

namespace ral {
namespace io {

// arrow_parser::arrow_parser(std::shared_ptr<arrow::Table> table):  table(table) {
// 	// TODO Auto-generated constructor stub

// 	std::cout<<"the total num rows is "<<table->num_rows()<<std::endl;
// 	// WSM TODO table_schema news to be newed up and copy in the properties
// }

arrow_parser::arrow_parser() {}

arrow_parser::~arrow_parser() {}

std::unique_ptr<ral::frame::BlazingTable> arrow_parser::parse_batch(
		ral::io::data_handle data_handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> /*row_groups*/)
{
  std::cout << "ES NULLLLLLLLLLLLLLLLLLLLLLLLLLLLLL\n";
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

  std::cout << "SAMEEEEEEEEEEEEEEEEEEEE????\n" << is_the_same << "\n";

  if (is_the_same) {
    return std::make_unique<ral::frame::BlazingArrowTable>(data_handle.arrow_table);
  }

  std::cout << "NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO\n";
  
  std::vector<std::shared_ptr<arrow::ChunkedArray>> cols;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (auto c : column_indices) {
    cols.push_back(data_handle.arrow_table->column(c));
    fields.push_back(data_handle.arrow_table->field(c));
  }

  auto new_schema = std::make_shared<arrow::Schema>(fields, data_handle.arrow_table->schema()->metadata());
	auto aa = std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(new_schema, cols, data_handle.arrow_table->num_rows()));

  // std::cout << "QQQQQQQQQQQQQQQQQQ????????????\n";
  // std::cout << "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\n\n" << aa->arrow_table()->ToString() << "\n\n\n FFGGGGGGG" << std::flush;

  return aa;
}

void arrow_parser::parse_schema(ral::io::data_handle /*handle*/,
		ral::io::Schema &  /*schema*/){
	std::vector<std::string> names;
	std::vector<cudf::type_id> types;
}

}
}
