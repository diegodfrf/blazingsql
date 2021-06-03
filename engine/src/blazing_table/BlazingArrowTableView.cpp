#include "BlazingArrowTableView.h"
#include "BlazingArrowTable.h"
#include <cudf/detail/interop.hpp>

namespace ral {
namespace frame {

BlazingArrowTableView::BlazingArrowTableView(std::shared_ptr<arrow::Table> arrow_table)
  : BlazingTableView(ral::execution::backend_id::ARROW), arrow_table(arrow_table) {
}

BlazingArrowTableView::BlazingArrowTableView(BlazingArrowTableView &&other)
  : BlazingTableView(ral::execution::backend_id::ARROW), arrow_table(std::move(other.arrow_table)) {
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

std::shared_ptr<arrow::Table> clone_arrow_table(std::shared_ptr<arrow::Table> arrow_table) {
  std::vector<std::shared_ptr<arrow::ChunkedArray>> the_cols;
  the_cols.resize(arrow_table->columns().size());
  int icol = 0;
  for (auto col : arrow_table->columns()) {
    arrow::ArrayVector the_chunks;
    the_chunks.resize(col->chunks().size());
    int ichunk = 0;
    for (auto chunk : col->chunks()) {
      the_chunks[ichunk] = arrow::MakeArray(chunk->data()->Copy());
      ++ichunk;
    }
    the_cols[icol] = std::make_shared<arrow::ChunkedArray>(the_chunks, col->type());
    ++icol;
  }
  auto new_schema = arrow_table->schema()->WithMetadata(arrow_table->schema()->metadata());
  auto new_table = arrow::Table::Make(new_schema, the_cols, arrow_table->num_rows());
  return new_table;
}

std::unique_ptr<BlazingTable> BlazingArrowTableView::clone() const {
  return std::make_unique<BlazingArrowTable>(clone_arrow_table(this->arrow_table));  
}

std::unique_ptr<BlazingArrowTable> BlazingArrowTableView::clone() {
  return std::make_unique<BlazingArrowTable>(clone_arrow_table(this->arrow_table));
}

}  // namespace frame
}  // namespace ral