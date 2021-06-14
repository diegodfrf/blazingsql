#include "BlazingCudfTable.h"
#include "BlazingColumnOwner.h"
#include "BlazingColumnView.h"
#include "parser/types_parser_utils.h"
#include <cudf/detail/interop.hpp> //cudf::from_arrow
#include <cudf/column/column_factories.hpp> //cudf::make_empty_column
#include "compute/cudf/detail/types.h"

namespace ral {
namespace frame {

BlazingCudfTable::BlazingCudfTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames)
	: BlazingTable(execution::backend_id::CUDF, true)
  , columnNames(columnNames), columns(std::move(columns)) {}

BlazingCudfTable::BlazingCudfTable(std::unique_ptr<cudf::table> table, const std::vector<std::string> & columnNames)
  : BlazingTable(execution::backend_id::CUDF, true)
{
	std::vector<std::unique_ptr<cudf::column>> columns_in = table->release();
	for (size_t i = 0; i < columns_in.size(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnOwner>(std::move(columns_in[i])));
	}
	this->columnNames = columnNames;
}

BlazingCudfTable::BlazingCudfTable(const cudf::table_view & table, const std::vector<std::string> & columnNames)
  : BlazingTable(execution::backend_id::CUDF, true)
{
	for (int i = 0; i < table.num_columns(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
	}
	this->columnNames = columnNames;
}

BlazingCudfTable::BlazingCudfTable(std::unique_ptr<BlazingArrowTable> blazing_arrow_table)
	: BlazingTable(execution::backend_id::CUDF, true){

	std::shared_ptr<arrow::Table> arrow_table = blazing_arrow_table->view();
	std::unique_ptr<cudf::table> cudf_table = cudf::from_arrow(*(arrow_table.get()));

	std::vector<std::unique_ptr<cudf::column>> columns_in = cudf_table->release();
	for (size_t i = 0; i < columns_in.size(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnOwner>(std::move(columns_in[i])));
	}
	this->columnNames = blazing_arrow_table->column_names();	
}

void BlazingCudfTable::ensureOwnership(){
	for (size_t i = 0; i < columns.size(); i++){
		columns[i] = std::make_unique<BlazingColumnOwner>(columns[i]->release());
	}
}

size_t BlazingCudfTable::num_columns() const {
  return columns.size();
}

cudf::table_view BlazingCudfTable::view() const{
	std::vector<cudf::column_view> column_views(columns.size());
	for (size_t i = 0; i < columns.size(); i++){
		column_views[i] = columns[i]->view();
	}
	return cudf::table_view(column_views);
}

size_t BlazingCudfTable::num_rows() const {
  return columns.size() == 0 ? 0 : (columns[0] == nullptr ? 0 : columns[0]->view().size());
}

std::vector<std::string> BlazingCudfTable::column_names() const{
	return this->columnNames;
}

std::vector<std::shared_ptr<arrow::DataType>> BlazingCudfTable::column_types() const {
	std::vector<std::shared_ptr<arrow::DataType>> data_types;
	auto view = this->view();
	for (size_t i = 0; i < view.num_columns(); ++i) {
		data_types.push_back(cudf_type_id_to_arrow_data_type(view.column(i).type().id()));
	}
	return data_types;
}

void BlazingCudfTable::set_column_names(const std::vector<std::string> & column_names) {
  this->columnNames = column_names; 
}

std::shared_ptr<BlazingTableView> BlazingCudfTable::to_table_view() const {
	return std::make_shared<BlazingCudfTableView>(this->view(), this->columnNames);
}

std::shared_ptr<BlazingCudfTableView> BlazingCudfTable::to_table_view() {
	return std::make_shared<BlazingCudfTableView>(this->view(), this->columnNames);
}

std::unique_ptr<cudf::table> BlazingCudfTable::releaseCudfTable() {
	valid = false; // we are taking the data out, so we want to indicate that its no longer valid
	std::vector<std::unique_ptr<cudf::column>> columns_out;
	for (size_t i = 0; i < columns.size(); i++){
		columns_out.emplace_back(std::move(columns[i]->release()));
	}
	return std::make_unique<cudf::table>(std::move(columns_out));
}

std::vector<std::unique_ptr<BlazingColumn>> BlazingCudfTable::releaseBlazingColumns() {
	this->valid = false; // we are taking the data out, so we want to indicate that its no longer valid
	return std::move(columns);
}

unsigned long long BlazingCudfTable::size_in_bytes() const {
	unsigned long long total_size = 0UL;
	for(auto & bz_column : this->columns) {
			auto column =  bz_column->view();
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

std::unique_ptr<BlazingTable> BlazingCudfTable::clone() const {
	return this->to_table_view()->clone();
}

std::unique_ptr<BlazingCudfTable> BlazingCudfTable::clone() {
	return this->to_table_view()->clone();
}

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(
	std::vector<arrow::Type::type> column_types,
	std::vector<std::string> column_names) {
    std::vector< std::unique_ptr<cudf::column> > empty_columns;
    empty_columns.resize(column_types.size());
    for(size_t i = 0; i < column_types.size(); ++i) {
        cudf::data_type dtype = arrow_type_to_cudf_data_type(column_types[i]);
        std::unique_ptr<cudf::column> empty_column = cudf::make_empty_column(dtype);
        empty_columns[i] = std::move(empty_column);
    }

    std::unique_ptr<cudf::table> cudf_table = std::make_unique<cudf::table>(std::move(empty_columns));
    return std::make_unique<BlazingCudfTable>(std::move(cudf_table), column_names);
}

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(
	std::vector<std::shared_ptr<arrow::DataType>> column_types,
	std::vector<std::string> column_names) {
    std::vector< std::unique_ptr<cudf::column> > empty_columns;
    empty_columns.resize(column_types.size());
    for(size_t i = 0; i < column_types.size(); ++i) {
        cudf::data_type dtype = arrow_type_to_cudf_data_type(column_types[i]->id());
        std::unique_ptr<cudf::column> empty_column = cudf::make_empty_column(dtype);
        empty_columns[i] = std::move(empty_column);
    }

    std::unique_ptr<cudf::table> cudf_table = std::make_unique<cudf::table>(std::move(empty_columns));
    return std::make_unique<BlazingCudfTable>(std::move(cudf_table), column_names);
}

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const cudf::table_view & table){
    std::vector<std::unique_ptr<BlazingColumn>> columns_out;
    for (int i = 0; i < table.num_columns(); i++){
        columns_out.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
    }
    return columns_out;
}

}  // namespace frame
}  // namespace ral
