#pragma once

#include <arrow/table.h>
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"

#include "execution_graph/backend.hpp"
#include "blazing_table/BlazingColumn.h"

namespace ral {
namespace frame {

class BlazingTable;
class BlazingCudfTable;

class BlazingTableView : public execution::BlazingDispatchable {
public:
  BlazingTableView(execution::backend_id execution_backend_id);

  virtual size_t num_columns() const = 0;
  virtual size_t num_rows() const = 0;
  virtual std::vector<std::string> column_names() const = 0;
  virtual std::vector<cudf::data_type> column_types() const = 0; // TODO percy rommel arrow use srd_ptr arrow::Type
  virtual void set_column_names(const std::vector<std::string> & column_names) = 0;
  virtual unsigned long long size_in_bytes() const = 0;
  virtual std::unique_ptr<BlazingTable> clone() const = 0;
};

class BlazingTable: public BlazingTableView {
public:
  BlazingTable(execution::backend_id execution_backend_id, const bool & valid);
  BlazingTable(BlazingTable &&) = default;
  virtual ~BlazingTable() {};

  bool is_valid() const { return valid; }
  operator bool() const { return this->is_valid(); }

  virtual void ensureOwnership() {}
  virtual std::shared_ptr<ral::frame::BlazingTableView> to_table_view() const = 0;

protected:
  bool valid = true;
};

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
  std::shared_ptr<arrow::Table> view() const { return this->arrow_table; };

protected:
  std::shared_ptr<arrow::Table> arrow_table;
};

class BlazingArrowTable : public BlazingTable, public BlazingArrowTableView {
public:
  BlazingArrowTable(std::shared_ptr<arrow::Table> arrow_table);
  BlazingArrowTable(std::unique_ptr<BlazingCudfTable> blazing_cudf_table);
  BlazingArrowTable(BlazingArrowTable &&other) = default;

  size_t num_columns() const override { return BlazingArrowTableView::num_columns(); }
  size_t num_rows() const override { return BlazingArrowTableView::num_rows(); }
  std::vector<std::string> column_names() const override { return BlazingArrowTableView::column_names(); }
  std::vector<cudf::data_type> column_types() const override { return BlazingArrowTableView::column_types(); }
  void set_column_names(const std::vector<std::string> & column_names) { return BlazingArrowTableView::set_column_names(column_names); }
  unsigned long long size_in_bytes() const override { return BlazingArrowTableView::size_in_bytes(); }
  std::unique_ptr<BlazingTable> clone() const override;
  std::shared_ptr<BlazingTableView> to_table_view() const override;
  std::shared_ptr<BlazingArrowTableView> to_table_view();
};

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
  std::vector<cudf::data_type> column_types() const override;
  void set_column_names(const std::vector<std::string> & column_names) override;
  unsigned long long size_in_bytes() const override;
  std::unique_ptr<BlazingTable> clone() const override;
  cudf::table_view view() const;

private:
	std::vector<std::string> columnNames;
	cudf::table_view table;
};

class BlazingCudfTable : public BlazingTable {
public:
	BlazingCudfTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames);
	BlazingCudfTable(std::unique_ptr<cudf::table> table, const std::vector<std::string> & columnNames);
	BlazingCudfTable(const cudf::table_view & table, const std::vector<std::string> & columnNames);
  BlazingCudfTable(std::unique_ptr<BlazingArrowTable> blazing_arrow_table);
	BlazingCudfTable(BlazingCudfTable &&other);

	BlazingCudfTable & operator=(BlazingCudfTable const &) = delete;
	BlazingCudfTable & operator=(BlazingCudfTable &&) = delete;

  size_t num_columns() const override;
  size_t num_rows() const override;
  std::vector<std::string> column_names() const override;
  std::vector<cudf::data_type> column_types() const override;
  void set_column_names(const std::vector<std::string> & column_names) override;
  unsigned long long size_in_bytes() const override;
  std::unique_ptr<BlazingTable> clone() const override;
	cudf::table_view view() const;
	std::shared_ptr<ral::frame::BlazingTableView> to_table_view() const override;
  std::shared_ptr<BlazingCudfTableView> to_table_view();
	std::unique_ptr<cudf::table> releaseCudfTable();
	std::vector<std::unique_ptr<BlazingColumn>> releaseBlazingColumns();
  void ensureOwnership() override;

private:
	std::vector<std::string> columnNames;
	std::vector<std::unique_ptr<BlazingColumn>> columns;
};

// TODO percy arrow cudf scalar
//class BlazingScalar: public execution::BlazingDispatchable {
//public:
//  BlazingScalar(execution::backend_id execution_backend_id);
//  BlazingScalar(BlazingScalar &&) = default;
//  virtual ~BlazingScalar() = default;

//  virtual cudf::data_type type() const = 0;
//};

//class BlazingArrowScalar: public BlazingScalar {
//public:
//  BlazingArrowScalar(std::shared_ptr<arrow::Scalar> scalar);
//  BlazingArrowScalar(BlazingArrowScalar &&) = default;
//  virtual ~BlazingArrowScalar() = default;

//  cudf::data_type type() const override;
//  std::shared_ptr<arrow::Scalar> value() const;

//private:
//  std::shared_ptr<arrow::Scalar> scalar;
//};

//class BlazingCudfScalar: public BlazingScalar {
//public:
//  BlazingCudfScalar(std::unique_ptr<cudf::scalar> scalar);
//  BlazingCudfScalar(BlazingCudfScalar &&) = default;
//  virtual ~BlazingCudfScalar() = default;

//  cudf::data_type type() const override;
//  //std::unique_ptr<cudf::scalar> value() const;

//private:
//  //std::unique_ptr<cudf::scalar> scalar;
//};

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::data_type> column_types,
									   std::vector<std::string> column_names);

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::type_id> column_types,
									   std::vector<std::string> column_names);

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const cudf::table_view & table);


}  // namespace frame
}  // namespace ral
