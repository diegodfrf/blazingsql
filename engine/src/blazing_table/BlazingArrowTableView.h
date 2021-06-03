#pragma once

#include "BlazingTableView.h"
#include <arrow/table.h>

namespace ral {
namespace frame {

class BlazingArrowTable;

class BlazingArrowTableView : public BlazingTableView {
public:
  BlazingArrowTableView(std::shared_ptr<arrow::Table> arrow_table);
  BlazingArrowTableView(BlazingArrowTableView &&other);

  size_t num_columns() const override;
  size_t num_rows() const override;
  std::vector<std::string> column_names() const override;
  std::vector<cudf::data_type> column_types() const override;
  void set_column_names(const std::vector<std::string> & column_names) override;
  unsigned long long size_in_bytes() const override;
  std::unique_ptr<BlazingTable> clone() const override;
  std::unique_ptr<BlazingArrowTable> clone();
  std::shared_ptr<arrow::Table> view() const { return this->arrow_table; };

protected:
  std::shared_ptr<arrow::Table> arrow_table;
};

}  // namespace frame
}  // namespace ral