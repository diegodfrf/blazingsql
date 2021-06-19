/*
 * Schema.h
 *
 *  Created on: Apr 26, 2019
 *      Author: felipe
 */

#ifndef BLAZING_RAL_SCHEMA_H_
#define BLAZING_RAL_SCHEMA_H_

#include <string>
#include <vector>
#include <arrow/type.h>

namespace ral {
namespace io {

class Schema {
public:
	Schema();

	Schema(std::vector<std::string> names,
		std::vector<size_t> calcite_to_file_indices,
		std::vector<std::shared_ptr<arrow::DataType>> types,
		std::vector<std::vector<int>> row_groups_ids = {}
		);

	Schema(std::vector<std::string> names,
		std::vector<size_t> calcite_to_file_indices,
		std::vector<std::shared_ptr<arrow::DataType>> types,
		std::vector<bool> in_file,
		std::vector<std::vector<int>> row_groups_ids = {});

	Schema(std::vector<std::string> names, std::vector<std::shared_ptr<arrow::DataType>> types);

	Schema(const Schema& ) = default;

	Schema& operator = (const Schema& ) = default;

	virtual ~Schema();

	std::vector<std::string> get_names() const;
	void set_names(const std::vector<std::string> & names);
	std::vector<std::string> get_files() const;
	std::vector<bool> get_in_file() const;
	bool all_in_file() const;
	std::vector<std::shared_ptr<arrow::DataType>> get_dtypes() const;
	std::shared_ptr<arrow::DataType> get_dtype(size_t schema_index) const;
	std::string get_name(size_t schema_index) const;
	std::vector<size_t> get_calcite_to_file_indices() const { return this->calcite_to_file_indices; }
	Schema fileSchema(size_t current_file_index) const;

	size_t get_num_columns() const;

	std::vector<int> get_rowgroup_ids(size_t file_index) const;
	std::vector<std::vector<int>> get_rowgroups();
	int get_total_num_rowgroups();

	bool get_has_header_csv() const;
	void set_has_header_csv(bool has_header);

	void add_file(std::string file);

	void add_column(std::string name,
		std::shared_ptr<arrow::DataType> type,
		size_t file_index,
		bool is_in_file = true);

	inline bool operator==(const Schema & rhs) const {
		return (this->names == rhs.names) && (this->types == rhs.types);
	}

	inline bool operator!=(const Schema & rhs) { return !(*this == rhs); }

private:
	std::vector<std::string> names;
	std::vector<size_t> calcite_to_file_indices;  // maps calcite columns to our columns
	std::vector<std::shared_ptr<arrow::DataType>> types;
	std::vector<bool> in_file;
	std::vector<std::string> files;
	std::vector<std::vector<int>> row_groups_ids;
	bool has_header_csv = false;
};

} /* namespace io */
} /* namespace ral */

#endif /* BLAZING_RAL_SCHEMA_H_ */
