#include "BlazingCudfTableView.h"
#include "BlazingCudfTable.h"
#include "parser/types_parser_utils.h"

namespace ral {
namespace frame {

BlazingCudfTableView::BlazingCudfTableView()
  : BlazingTableView(ral::execution::backend_id::CUDF) {
}

BlazingCudfTableView::BlazingCudfTableView(
	cudf::table_view table,
	std::vector<std::string> columnNames)
	: BlazingTableView(execution::backend_id::CUDF),
	columnNames(columnNames), table(table){

}

BlazingCudfTableView::BlazingCudfTableView(BlazingCudfTableView const &other)
  : BlazingTableView(ral::execution::backend_id::CUDF),
  columnNames(other.columnNames),
  table(other.table)
{
}

BlazingCudfTableView::BlazingCudfTableView(BlazingCudfTableView &&other)
  : BlazingTableView(ral::execution::backend_id::CUDF),
  columnNames(std::move(other.columnNames)),
  table(std::move(other.table))
{
}

BlazingCudfTableView & BlazingCudfTableView::operator=(BlazingCudfTableView const &other) {
  this->columnNames = other.columnNames;
  this->table = other.table;
	this->execution_backend = other.execution_backend;
  return *this;
}

BlazingCudfTableView & BlazingCudfTableView::operator=(BlazingCudfTableView &&other) {
  this->columnNames = std::move(other.columnNames);
  this->table = std::move(other.table);
	this->execution_backend = std::move(other.execution_backend);
  return *this;
}

std::vector<std::unique_ptr<BlazingColumn>> BlazingCudfTableView::toBlazingColumns() const{
	return cudfTableViewToBlazingColumns(this->table);
}

size_t BlazingCudfTableView::num_columns() const {
  return table.num_columns();
}

size_t BlazingCudfTableView::num_rows() const {
  return table.num_rows();
}

std::vector<std::string> BlazingCudfTableView::column_names() const{
	return this->columnNames;
}

std::vector<std::shared_ptr<arrow::DataType>> BlazingCudfTableView::column_types() const {
	std::vector<std::shared_ptr<arrow::DataType>> data_types;
	auto view = this->view();
	for (size_t i = 0; i < view.num_columns(); ++i) {
		data_types.push_back(cudf_type_id_to_arrow_data_type(view.column(i).type().id()));
	}
	return data_types;
}

void BlazingCudfTableView::set_column_names(const std::vector<std::string> & column_names) {
  this->columnNames = column_names;
}

unsigned long long BlazingCudfTableView::size_in_bytes() const {
	unsigned long long total_size = 0UL;
	for(cudf::size_type i = 0; i < this->num_columns(); ++i) {
		auto column = this->table.column(i);
		if(column.type().id() == cudf::type_id::STRING) {
			auto num_children = column.num_children();
			if(num_children == 2) {
				auto offsets_column = column.child(0);
				auto chars_column = column.child(1);

				total_size += chars_column.size();
				cudf::data_type offset_dtype(cudf::type_id::INT32);
				total_size += offsets_column.size() * cudf::size_of(offset_dtype);
				if(column.has_nulls()) {
					total_size += cudf::bitmask_allocation_size_bytes(column.size());
				}
			} else {
				// std::cerr << "string column with no children\n";
			}
		} else {
			total_size += column.size() * cudf::size_of(column.type());
			if(column.has_nulls()) {
				total_size += cudf::bitmask_allocation_size_bytes(column.size());
			}
		}
	}
	return total_size;
}

std::unique_ptr<BlazingTable> BlazingCudfTableView::clone() const {
	std::unique_ptr<cudf::table> cudfTable = std::make_unique<cudf::table>(this->table);
	return std::make_unique<BlazingCudfTable>(std::move(cudfTable), this->columnNames);
}

std::unique_ptr<BlazingCudfTable> BlazingCudfTableView::clone() {
	std::unique_ptr<cudf::table> cudfTable = std::make_unique<cudf::table>(this->table);
	return std::make_unique<BlazingCudfTable>(std::move(cudfTable), this->columnNames);
}

cudf::table_view BlazingCudfTableView::view() const{
	return this->table;
}

}// namespace frame
}// namespace ral