

#include "LogicPrimitives.h"
#include "cudf/column/column_factories.hpp"
#include "blazing_table/BlazingColumnView.h"
#include "blazing_table/BlazingColumnOwner.h"

#include <cudf/detail/interop.hpp>

namespace ral {

namespace frame{

BlazingCudfTable::BlazingCudfTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames)
	: columnNames(columnNames), columns(std::move(columns)) {}


BlazingCudfTable::BlazingCudfTable(	std::unique_ptr<CudfTable> table, const std::vector<std::string> & columnNames){

	std::vector<std::unique_ptr<CudfColumn>> columns_in = table->release();
	for (size_t i = 0; i < columns_in.size(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnOwner>(std::move(columns_in[i])));
	}
	this->columnNames = columnNames;
}

BlazingCudfTable::BlazingCudfTable(const CudfTableView & table, const std::vector<std::string> & columnNames){
	for (int i = 0; i < table.num_columns(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
	}
	this->columnNames = columnNames;
}

BlazingCudfTable::BlazingCudfTable(std::shared_ptr<arrow::Table> arrow_table)
  : columnNames(arrow_table->ColumnNames()), arrow_table_(arrow_table) {
}

void BlazingCudfTable::ensureOwnership(){
	for (size_t i = 0; i < columns.size(); i++){
		columns[i] = std::make_unique<BlazingColumnOwner>(std::move(columns[i]->release()));
	}
}

void BlazingArrowTable::ensureOwnership(){
}

cudf::size_type BlazingArrowTable::num_columns() const {  
	int a = this->arrow_table()->num_columns();
	return a;
}


cudf::size_type BlazingCudfTable::num_columns() const {
  return columns.size();
}

CudfTableView BlazingCudfTable::view() const{
	std::vector<CudfColumnView> column_views(columns.size());
	for (size_t i = 0; i < columns.size(); i++){
		column_views[i] = columns[i]->view();
	}
	return CudfTableView(column_views);
}



cudf::size_type BlazingCudfTable::num_rows() const {
  return columns.size() == 0 ? 0 : (columns[0] == nullptr ? 0 : columns[0]->view().size());
}

cudf::size_type BlazingArrowTable::num_rows() const {
    return this->arrow_table_->num_rows();
}


std::vector<std::string> BlazingArrowTable::names() const{
	return this->arrow_table_->ColumnNames();
}


std::vector<std::string> BlazingCudfTable::names() const{
	return this->columnNames;
}

std::vector<cudf::data_type> BlazingArrowTable::get_schema() const {
	std::vector<cudf::data_type> ret;
	for (auto f : this->arrow_table_->schema()->fields()) {
		ret.push_back(cudf::detail::arrow_to_cudf_type(*f->type()));
	}
	return ret;
}

std::vector<cudf::data_type> BlazingCudfTable::get_schema() const {
	std::vector<cudf::data_type> data_types(this->num_columns());
	auto view = this->view();
	std::transform(view.begin(), view.end(), data_types.begin(), [](auto & col){ return col.type(); });
	return data_types;
}

void BlazingArrowTable::setNames(const std::vector<std::string> & names) {
    assert(names.size() == this->arrow_table()->fields().size());
    int i = 0;
    for (auto f : this->arrow_table()->fields()) {
      this->arrow_table()->schema()->SetField(i, f->WithName(names[i]));
      ++i;
    } 
}

void BlazingCudfTable::setNames(const std::vector<std::string> & names) {
  this->columnNames = names; 
}

BlazingArrowTableView BlazingArrowTable::toBlazingCudfTableView() const{
	return BlazingArrowTableView(this->arrow_table_);
}
BlazingCudfTableView BlazingCudfTable::toBlazingCudfTableView() const{
	return BlazingCudfTableView(this->view(), this->columnNames);
}

std::unique_ptr<CudfTable> BlazingCudfTable::releaseCudfTable() {
	valid = false; // we are taking the data out, so we want to indicate that its no longer valid
	std::vector<std::unique_ptr<CudfColumn>> columns_out;
	for (size_t i = 0; i < columns.size(); i++){
		columns_out.emplace_back(std::move(columns[i]->release()));
	}
	return std::make_unique<CudfTable>(std::move(columns_out));
}

std::vector<std::unique_ptr<BlazingColumn>> BlazingCudfTable::releaseBlazingColumns() {
	valid = false; // we are taking the data out, so we want to indicate that its no longer valid
	return std::move(columns);
}

unsigned long long BlazingArrowTable::sizeInBytes(){
	unsigned long long size = 0;
	for(auto & col : this->arrow_table_->columns()){
		for(auto & chunk : col->chunks()){
			for(auto & data : chunk->data()){
				for(auto & buffer : data->buffers()){
					size += buffer->size(); 
				}
			}
		}
	}
	return size;
}

unsigned long long BlazingCudfTable::sizeInBytes()
{
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

BlazingCudfTableView::BlazingCudfTableView(){

}

BlazingCudfTableView::BlazingCudfTableView(
	CudfTableView table,
	std::vector<std::string> columnNames)
	: columnNames(std::move(columnNames)), table(std::move(table)){

}

BlazingArrowTableView::BlazingArrowTableView(std::shared_ptr<arrow::Table> arrow_table)
  : columnNames(arrow_table->ColumnNames()), arrow_table_(arrow_table) {
}

BlazingCudfTableView::BlazingCudfTableView(BlazingCudfTableView const &other)
  : columnNames(other.columnNames)
  , table(other.table)
  , arrow_table_(other.arrow_table_)
{
}

BlazingCudfTableView & BlazingCudfTableView::operator=(BlazingCudfTableView const &other) {
  this->columnNames = other.columnNames;
  this->table = other.table;
  this->arrow_table_ = other.arrow_table_;
  return *this;
}

CudfTableView BlazingCudfTableView::view() const{
	return this->table;
}

std::vector<std::unique_ptr<BlazingColumn>> BlazingCudfTableView::toBlazingColumns() const{
	return cudfTableViewToBlazingColumns(this->table);
}

std::vector<std::string> BlazingCudfTableView::names() const{
	return this->columnNames;
}

std::vector<std::string> BlazingArrowTableView::names() const{
	return this->columnNames;
}


void BlazingArrowTableView::setNames(const std::vector<std::string> & names) {
  this->arrow_table_->setNames(names);
}

void BlazingCudfTableView::setNames(const std::vector<std::string> & names) {
  this->columnNames = names;
}

std::vector<cudf::data_type> BlazingArrowTableView::get_schema() const {
  return this->arrow_table_->get_schema();
}

std::vector<cudf::data_type> BlazingCudfTableView::get_schema() const {
	std::vector<cudf::data_type> data_types(this->num_columns());
	auto view = this->view();
	std::transform(view.begin(), view.end(), data_types.begin(), [](auto & col){ return col.type(); });
	return data_types;
}

cudf::size_type BlazingArrowTableView::num_columns() const {
    int a = this->arrow_table_->num_columns();
    return a;
}

cudf::size_type BlazingCudfTableView::num_columns() const {
  return table.num_columns();
}

cudf::size_type BlazingArrowTableView::num_rows() const {
    return this->arrow_table_->num_rows();
}

cudf::size_type BlazingCudfTableView::num_rows() const {
  return table.num_rows();
}

unsigned long long BlazingArrowTableView::sizeInBytes(){
	this->arrow_table_->sizeInBytes();
}

unsigned long long BlazingCudfTableView::sizeInBytes()
{
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

std::unique_ptr<BlazingCudfTable> BlazingCudfTableView::clone() const {
	std::unique_ptr<CudfTable> cudfTable = std::make_unique<CudfTable>(this->table);
	return std::make_unique<BlazingCudfTable>(std::move(cudfTable), this->columnNames);
}

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::type_id> column_types,
																  std::vector<std::string> column_names) {
	std::vector< std::unique_ptr<cudf::column> > empty_columns;
	empty_columns.resize(column_types.size());
	for(size_t i = 0; i < column_types.size(); ++i) {
		cudf::type_id col_type = column_types[i];
		cudf::data_type dtype(col_type);
		std::unique_ptr<cudf::column> empty_column = cudf::make_empty_column(dtype);
		empty_columns[i] = std::move(empty_column);
	}

	std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(std::move(empty_columns));
	return std::make_unique<BlazingCudfTable>(std::move(cudf_table), column_names);
}

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::data_type> column_types,
									   std::vector<std::string> column_names) {
	std::vector< std::unique_ptr<cudf::column> > empty_columns;
	empty_columns.resize(column_types.size());
	for(size_t i = 0; i < column_types.size(); ++i) {
		auto dtype = column_types[i];
		std::unique_ptr<cudf::column> empty_column = cudf::make_empty_column(dtype);
		empty_columns[i] = std::move(empty_column);
	}

	std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(std::move(empty_columns));
	return std::make_unique<BlazingCudfTable>(std::move(cudf_table), column_names);
}

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const CudfTableView & table){
	std::vector<std::unique_ptr<BlazingColumn>> columns_out;
	for (int i = 0; i < table.num_columns(); i++){
		columns_out.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
	}
	return columns_out;
}

} // end namespace frame
}  // namespace ral
