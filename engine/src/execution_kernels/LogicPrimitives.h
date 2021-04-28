
#pragma once

#include <arrow/table.h>
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <memory>
#include <string>
#include <vector>
#include "blazing_table/BlazingColumn.h"
#include "blazing_table/BlazingHostTable.h"

typedef cudf::table CudfTable;
typedef cudf::table_view CudfTableView;

namespace ral {

namespace frame {

class BlazingTable;
class BlazingTableView;
class BlazingHostTable;

class BlazingTable {
public:
	BlazingTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames);
	BlazingTable(std::unique_ptr<CudfTable> table, const std::vector<std::string> & columnNames);
	BlazingTable(const CudfTableView & table, const std::vector<std::string> & columnNames);
  BlazingTable(std::shared_ptr<arrow::Table> arrow_table);
	BlazingTable(BlazingTable &&) = default;
	BlazingTable & operator=(BlazingTable const &) = delete;
	BlazingTable & operator=(BlazingTable &&) = delete;

  std::shared_ptr<arrow::Table> arrow_table() const { return this->arrow_table_; }
  bool is_arrow() const { return (this->arrow_table_ != nullptr); }

	CudfTableView view() const;
	cudf::size_type num_columns() const;
	cudf::size_type num_rows() const;
	std::vector<std::string> names() const;
	std::vector<cudf::data_type> get_schema() const;
	// set columnNames
	void setNames(const std::vector<std::string> & names);

	BlazingTableView toBlazingTableView() const;

	operator bool() const { return this->is_valid(); }

	bool is_valid() const { return valid; }

	std::unique_ptr<CudfTable> releaseCudfTable();
	std::vector<std::unique_ptr<BlazingColumn>> releaseBlazingColumns();

	unsigned long long sizeInBytes();
	void ensureOwnership();	

private:
	std::vector<std::string> columnNames;
	std::vector<std::unique_ptr<BlazingColumn>> columns;
	bool valid=true;
  std::shared_ptr<arrow::Table> arrow_table_;
};

class BlazingTableView {
public:
	BlazingTableView();
	BlazingTableView(CudfTableView table, std::vector<std::string> columnNames);
  BlazingTableView(std::shared_ptr<arrow::Table> arrow_table);
  BlazingTableView(BlazingTableView const &other);
	BlazingTableView(BlazingTableView &&) = default;

	BlazingTableView & operator=(BlazingTableView const &other);
	BlazingTableView & operator=(BlazingTableView &&) = default;

  std::shared_ptr<arrow::Table> arrow_table() const { return this->arrow_table_; }
  bool is_arrow() const { return (this->arrow_table_ != nullptr); }
  
	CudfTableView view() const;

	cudf::column_view const & column(cudf::size_type column_index) const { return table.column(column_index); }
	std::vector<std::unique_ptr<BlazingColumn>> toBlazingColumns() const;

	std::vector<cudf::data_type> get_schema() const;

	std::vector<std::string> names() const;
	void setNames(const std::vector<std::string> & names);

	cudf::size_type num_columns() const;

	cudf::size_type num_rows() const;

	unsigned long long sizeInBytes();

	std::unique_ptr<BlazingTable> clone() const;

private:
	std::vector<std::string> columnNames;
	CudfTableView table;
  std::shared_ptr<arrow::Table> arrow_table_;
};

std::unique_ptr<ral::frame::BlazingTable> createEmptyBlazingTable(std::vector<cudf::data_type> column_types,
									   std::vector<std::string> column_names);

std::unique_ptr<ral::frame::BlazingTable> createEmptyBlazingTable(std::vector<cudf::type_id> column_types,
									   std::vector<std::string> column_names);

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const CudfTableView & table);

}  // namespace frame
}  // namespace ral
