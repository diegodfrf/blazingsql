#pragma once

#include <arrow/table.h>
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"

#include "execution_graph/backend.hpp"
#include "blazing_table/BlazingColumn.h"

namespace ral {
namespace frame {

class BlazingObject {
public:
  BlazingObject(execution::backend_id execution_backend_id) : execution_backend(execution_backend_id) {}
  BlazingObject(BlazingObject &&) = default;
  virtual ~BlazingObject() {}

  execution::execution_backend get_execution_backend() const { return this->execution_backend; }

private:
  execution::execution_backend execution_backend;
};

class BlazingTable : public BlazingObject {
public:
  BlazingTable(execution::backend_id execution_backend_id, const bool & valid);
  BlazingTable(BlazingTable &&) = default;
  virtual ~BlazingTable() {}

  bool is_valid() const { return valid; }
  operator bool() const { return this->is_valid(); }

  virtual size_t num_columns() const = 0;
  virtual size_t num_rows() const = 0;
  virtual std::vector<std::string> column_names() const = 0;
  virtual std::vector<arrow::Type::type> column_types() const = 0;
  virtual void set_column_names(const std::vector<std::string> & column_names) = 0;
  virtual unsigned long long size_in_bytes() const = 0;

private:
  bool valid = true;
};

class BlazingArrowTable : public BlazingTable {
public:
  BlazingArrowTable(std::shared_ptr<arrow::Table> arrow_table);
  BlazingArrowTable(BlazingArrowTable &&other);

  virtual size_t num_columns() const override;
  virtual size_t num_rows() const override;
  virtual std::vector<std::string> column_names() const override;
  virtual std::vector<arrow::Type::type> column_types() const override;
  virtual void set_column_names(const std::vector<std::string> & column_names) override;
  unsigned long long size_in_bytes() const override;
  std::shared_ptr<arrow::Table> view() const { return this->arrow_table; };

private:
  std::shared_ptr<arrow::Table> arrow_table;
};

class BlazingCudfTable;

class BlazingCudfTableView : public BlazingTable {
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
  std::vector<arrow::Type::type> column_types() const override;
  void set_column_names(const std::vector<std::string> & column_names) override;
  unsigned long long size_in_bytes() const override;
  cudf::table_view view() const;

	std::unique_ptr<BlazingCudfTable> clone() const;

private:
	std::vector<std::string> columnNames;
	cudf::table_view table;
};

class BlazingCudfTable : public BlazingTable {
public:
	BlazingCudfTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames);
	BlazingCudfTable(std::unique_ptr<cudf::table> table, const std::vector<std::string> & columnNames);
	BlazingCudfTable(const cudf::table_view & table, const std::vector<std::string> & columnNames);
	BlazingCudfTable(BlazingCudfTable &&other);

	BlazingCudfTable & operator=(BlazingCudfTable const &) = delete;
	BlazingCudfTable & operator=(BlazingCudfTable &&) = delete;

	cudf::table_view view() const;
	BlazingCudfTableView toBlazingCudfTableView() const;	
	std::unique_ptr<cudf::table> releaseCudfTable();
	std::vector<std::unique_ptr<BlazingColumn>> releaseBlazingColumns();

private:
	std::vector<std::string> columnNames;
	std::vector<std::unique_ptr<BlazingColumn>> columns;
};

// TODO percy arrow cudf scalar
class BlazingScalar: public BlazingObject {
public:
  BlazingScalar(execution::backend_id execution_backend_id);
  BlazingScalar(BlazingScalar &&) = default;
  virtual ~BlazingScalar() = default;

  virtual arrow::Type::type type() const = 0;
};

class BlazingArrowScalar: public BlazingScalar {
public:
  BlazingArrowScalar(execution::backend_id execution_backend_id);
  BlazingArrowScalar(BlazingArrowScalar &&) = default;
  virtual ~BlazingArrowScalar() = default;

  virtual arrow::Type::type type() const override;
};

class BlazingCudfScalar: public BlazingScalar {
public:
  BlazingCudfScalar(execution::backend_id execution_backend_id);
  BlazingCudfScalar(BlazingCudfScalar &&) = default;
  virtual ~BlazingCudfScalar() = default;

  virtual arrow::Type::type type() const override;
};

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::data_type> column_types,
									   std::vector<std::string> column_names);

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::type_id> column_types,
									   std::vector<std::string> column_names);

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const cudf::table_view & table);

}  // namespace frame
}  // namespace ral
