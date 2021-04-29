
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

class BlazingCudaTable;
class BlazingCudfTableView;
class BlazingHostTable;

class BlazingTable{
	public:
		BlazingTable(const execution_backend & backend, const bool & valid);
		virtual cudf::size_type num_columns() const = delete;
		virtual cudf::size_type num_rows() const = delete;
		virtual std::vector<std::string> names() const = delete;
		virtual std::vector<cudf::data_type> get_schema() const = delete;
		virtual void setNames(const std::vector<std::string> & names) = delete;
		operator bool() const { return this->is_valid(); }

		bool is_valid() const { return valid; }
		virtual void ensureOwnership() = delete;	
		virtual unsigned long long sizeInBytes() = delete;

	private:
		execution_backend backend; //<-- Just a wrapper for an enum incase we want to add more to it
		
		bool valid=true;

};

class BlazingArrowTable : public BlazingTable{
	public:
  		BlazingArrowTable(std::shared_ptr<arrow::Table> arrow_table);

		cudf::size_type num_columns() const;
		cudf::size_type num_rows() const;
		std::vector<std::string> names() const;
		std::vector<cudf::data_type> get_schema() const;
		void setNames(const std::vector<std::string> & names);

		void ensureOwnership();	
		unsigned long long sizeInBytes();
		std::shared_ptr<arrow::Table> arrow_table() const { return this->arrow_table_; }
	private:
  		std::shared_ptr<arrow::Table> arrow_table_;
};

class BlazingCudfTable : public BlazingTable{
public:
	BlazingCudfTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames);
	BlazingCudfTable(std::unique_ptr<CudfTable> table, const std::vector<std::string> & columnNames);
	BlazingCudfTable(const CudfTableView & table, const std::vector<std::string> & columnNames);

	BlazingCudfTable(BlazingCudfTable &&) = default;
	BlazingCudfTable & operator=(BlazingCudfTable const &) = delete;
	BlazingCudfTable & operator=(BlazingCudfTable &&) = delete;


	CudfTableView view() const;

	BlazingCudfTableView toBlazingCudfTableView() const;

	
	std::unique_ptr<CudfTable> releaseCudfTable();
	std::vector<std::unique_ptr<BlazingColumn>> releaseBlazingColumns();




private:
	std::vector<std::string> columnNames;
	std::vector<std::unique_ptr<BlazingColumn>> columns;


};

class BlazingTableView{
public:
	virtual std::vector<cudf::data_type> get_schema() const = delete;

	virtual std::vector<std::string> names() const = delete;
	virtual void setNames(const std::vector<std::string> & names) = delete;

	virtual cudf::size_type num_columns() const = delete;

	virtual cudf::size_type num_rows() const = delete;

	virtual unsigned long long sizeInBytes() = delete;

private:

}

class BlazingArrowTableview {
	public:
	  	std::shared_ptr<arrow::Table> arrow_table() const { return this->arrow_table_->arrow_table(); }
		std::vector<cudf::data_type> get_schema() const;

		std::vector<std::string> names() const;
		void setNames(const std::vector<std::string> & names);

		cudf::size_type num_columns() const;

		cudf::size_type num_rows() const;

		unsigned long long sizeInBytes();

	private:
		BlazingArrowTable arrow_table_;
}

class BlazingCudfTableView {
public:
	BlazingCudfTableView();
	BlazingCudfTableView(CudfTableView table, std::vector<std::string> columnNames);
  	BlazingCudfTableView(BlazingCudfTableView const &other);
	BlazingCudfTableView(BlazingCudfTableView &&) = default;

	BlazingCudfTableView & operator=(BlazingCudfTableView const &other);
	BlazingCudfTableView & operator=(BlazingCudfTableView &&) = default;


  
	CudfTableView view() const;

	cudf::column_view const & column(cudf::size_type column_index) const { return table.column(column_index); }
	std::vector<std::unique_ptr<BlazingColumn>> toBlazingColumns() const;

	std::vector<cudf::data_type> get_schema() const;

	std::vector<std::string> names() const;
	void setNames(const std::vector<std::string> & names);

	cudf::size_type num_columns() const;

	cudf::size_type num_rows() const;

	unsigned long long sizeInBytes();

	std::unique_ptr<BlazingCudfTable> clone() const;

private:
	std::vector<std::string> columnNames;
	CudfTableView table;
 
};

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::data_type> column_types,
									   std::vector<std::string> column_names);

std::unique_ptr<ral::frame::BlazingCudfTable> createEmptyBlazingCudfTable(std::vector<cudf::type_id> column_types,
									   std::vector<std::string> column_names);

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const CudfTableView & table);

}  // namespace frame
}  // namespace ral
