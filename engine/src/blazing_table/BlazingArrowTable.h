#pragma once

#include "BlazingArrowTableView.h"
#include "BlazingTable.h"
#include <arrow/table.h>

namespace ral {
namespace frame {

class BlazingArrowTable : public BlazingTable, public BlazingArrowTableView {
public:
  BlazingArrowTable(std::shared_ptr<arrow::Table> arrow_table);
  BlazingArrowTable(std::unique_ptr<BlazingCudfTable> blazing_cudf_table);
  BlazingArrowTable(BlazingArrowTable &&other) = default;

  size_t num_columns() const override { return BlazingArrowTableView::num_columns(); }
  size_t num_rows() const override { return BlazingArrowTableView::num_rows(); }
  std::vector<std::string> column_names() const override { return BlazingArrowTableView::column_names(); }
  std::vector<std::shared_ptr<arrow::DataType>> column_types() const override { return BlazingArrowTableView::column_types(); }
  void set_column_names(const std::vector<std::string> & column_names) { return BlazingArrowTableView::set_column_names(column_names); }
  unsigned long long size_in_bytes() const override { return BlazingArrowTableView::size_in_bytes(); }
  std::unique_ptr<BlazingTable> clone() const override;
  std::unique_ptr<BlazingArrowTable> clone();
  std::shared_ptr<BlazingTableView> to_table_view() const override;
  std::shared_ptr<BlazingArrowTableView> to_table_view();
};


}  // namespace frame
}  // namespace ral