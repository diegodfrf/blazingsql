#pragma once

#include "BlazingTableView.h"
#include "blazing_table/BlazingColumn.h"

namespace ral {
namespace frame {

class BlazingCudfTable;

class BlazingCudfTableView : public BlazingTableView {
public:
	BlazingCudfTableView();
	BlazingCudfTableView(cudf::table_view table, std::vector<std::string> columnNames);
	BlazingCudfTableView(BlazingCudfTableView const &other);
	BlazingCudfTableView(BlazingCudfTableView &&other);

	BlazingCudfTableView & operator=(BlazingCudfTableView const &other);
	BlazingCudfTableView & operator=(BlazingCudfTableView &&);

	cudf::column_view const & column(cudf::size_type column_index) const { return table.column(column_index); }
	std::vector<std::unique_ptr<BlazingColumn>> toBlazingColumns() const;

  size_t num_columns() const override;
  size_t num_rows() const override;
  std::vector<std::string> column_names() const override;
  std::vector<std::shared_ptr<arrow::DataType>> column_types() const override;
  void set_column_names(const std::vector<std::string> & column_names) override;
  unsigned long long size_in_bytes() const override;
  std::unique_ptr<BlazingTable> clone() const override;
  std::unique_ptr<BlazingCudfTable> clone();
  cudf::table_view view() const;

private:
	std::vector<std::string> columnNames;
	cudf::table_view table;
};

}  // namespace frame
}  // namespace ral