

#include "LogicPrimitives.h"
#include "cudf/column/column_factories.hpp"
#include "blazing_table/BlazingColumnView.h"
#include "blazing_table/BlazingColumnOwner.h"

#include <cudf/detail/interop.hpp>

namespace ral {

namespace frame{

BlazingTable::BlazingTable(execution::backend_id execution_backend_id, const bool & valid)
  : BlazingDispatchable(execution_backend_id), valid(valid) {
}

// BEGIN BlazingArrowTableView

BlazingArrowTableView::BlazingArrowTableView(std::shared_ptr<arrow::Table> arrow_table)
  : arrow_table(arrow_table) {
}

BlazingArrowTableView::BlazingArrowTableView(BlazingArrowTableView &&other)
  : arrow_table(std::move(other.arrow_table)) {
}

size_t BlazingArrowTableView::num_columns() const {  
	int a = this->arrow_table->num_columns();
	return a;
}

size_t BlazingArrowTableView::num_rows() const {
    return this->arrow_table->num_rows();
}

std::vector<std::string> BlazingArrowTableView::column_names() const{
	return this->arrow_table->ColumnNames();
}

std::vector<cudf::data_type> BlazingArrowTableView::column_types() const {
	std::vector<cudf::data_type> ret;
	for (auto f : this->arrow_table->schema()->fields()) {
		ret.push_back(cudf::detail::arrow_to_cudf_type(*f->type()));
	}
	return ret;
}

void BlazingArrowTableView::set_column_names(const std::vector<std::string> & column_names) {
    assert(column_names.size() == this->arrow_table->fields().size());
    int i = 0;
    for (auto f : this->arrow_table->fields()) {
      this->arrow_table->schema()->SetField(i, f->WithName(column_names[i]));
      ++i;
    } 
}

unsigned long long BlazingArrowTableView::size_in_bytes() const {
	unsigned long long size = 0;
  return 0; // TODO percy arrow
//	for(auto & col : this->arrow_table->columns()){
//		for(auto & chunk : col->chunks()){
//			for(auto & data : chunk->data()){
//				for(auto & buffer : data->buffers()){
//					size += buffer->size(); 
//				}
//			}
//		}
//	}
//	return size;
}

// END BlazingArrowTableView

// BEGIN BlazingArrowTable

BlazingArrowTable::BlazingArrowTable(std::shared_ptr<arrow::Table> arrow_table)
  : BlazingTable(execution::backend_id::ARROW, true)
	, BlazingArrowTableView(arrow_table) {
}

std::shared_ptr<BlazingTableView> BlazingArrowTable::to_table_view() const {
	return std::make_shared<BlazingArrowTableView>(BlazingArrowTableView::view());
}

std::shared_ptr<BlazingArrowTableView> BlazingArrowTable::to_table_view() {
	return std::make_shared<BlazingArrowTableView>(BlazingArrowTableView::view());
}

// END BlazingArrowTable

// BEGIN BlazingCudfTableView


BlazingCudfTableView::BlazingCudfTableView(){
}

BlazingCudfTableView::BlazingCudfTableView(
	cudf::table_view table,
	std::vector<std::string> columnNames)
	: columnNames(std::move(columnNames)), table(std::move(table)){

}

BlazingCudfTableView::BlazingCudfTableView(BlazingCudfTableView const &other)
  : columnNames(other.columnNames)
  , table(other.table)
{
}

BlazingCudfTableView::BlazingCudfTableView(BlazingCudfTableView &&other)
  : columnNames(std::move(other.columnNames))
  , table(std::move(other.table))
{
}

BlazingCudfTableView & BlazingCudfTableView::operator=(BlazingCudfTableView const &other) {
  this->columnNames = other.columnNames;
  this->table = other.table;
  return *this;
}

BlazingCudfTableView & BlazingCudfTableView::operator=(BlazingCudfTableView &&other) {
  this->columnNames = std::move(other.columnNames);
  this->table = std::move(other.table);
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

std::vector<cudf::data_type> BlazingCudfTableView::column_types() const {
	std::vector<cudf::data_type> data_types(this->num_columns());
	auto view = this->view();
	std::transform(view.begin(), view.end(), data_types.begin(), [](auto & col){ return col.type(); });
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

cudf::table_view BlazingCudfTableView::view() const{
	return this->table;
}

std::unique_ptr<BlazingCudfTable> BlazingCudfTableView::clone() const {
	std::unique_ptr<cudf::table> cudfTable = std::make_unique<cudf::table>(this->table);
	return std::make_unique<BlazingCudfTable>(std::move(cudfTable), this->columnNames);
}


// END BlazingCudfTableView


// BEGIN BlazingCudfTableView

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

void BlazingCudfTable::ensureOwnership(){
	for (size_t i = 0; i < columns.size(); i++){
		columns[i] = std::make_unique<BlazingColumnOwner>(std::move(columns[i]->release()));
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

std::vector<cudf::data_type> BlazingCudfTable::column_types() const {
	std::vector<cudf::data_type> data_types(this->num_columns());
	auto view = this->view();
	std::transform(view.begin(), view.end(), data_types.begin(), [](auto & col){ return col.type(); });
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

// END BlazingCudfTableView

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

	std::unique_ptr<cudf::table> cudf_table = std::make_unique<cudf::table>(std::move(empty_columns));
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

} // end namespace frame
}  // namespace ral
