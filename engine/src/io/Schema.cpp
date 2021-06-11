/*
 * Schema.cpp
 *
 *  Created on: Apr 26, 2019
 *      Author: felipe
 */

#include "Schema.h"
#include "parser/types_parser_utils.h"

namespace ral {
namespace io {

Schema::Schema(std::vector<std::string> names,
	std::vector<size_t> calcite_to_file_indices,
	std::vector<arrow::Type::type> types,
	std::vector<std::vector<int>> row_groups_ids)
	: names(names), calcite_to_file_indices(calcite_to_file_indices), types(types),
	  row_groups_ids{row_groups_ids} {

	in_file.resize(names.size(), true);
}

Schema::Schema(std::vector<std::string> names,
	std::vector<size_t> calcite_to_file_indices,
	std::vector<arrow::Type::type> types,
	std::vector<bool> in_file,
	std::vector<std::vector<int>> row_groups_ids)
	: names(names), calcite_to_file_indices(calcite_to_file_indices), types(types),
	  in_file(in_file), row_groups_ids{row_groups_ids} {
	if(in_file.size() != names.size()) {
		this->in_file.resize(names.size(), true);
	}
}

Schema::Schema(std::vector<std::string> names, std::vector<arrow::Type::type> types)
	: names(names), calcite_to_file_indices({}), types(std::move(types)) {
	in_file.resize(names.size(), true);
}

Schema::Schema() : names({}), calcite_to_file_indices({}), types({}) {}


Schema::~Schema() {
	// TODO Auto-generated destructor stub
}

std::vector<std::string> Schema::get_names() const { return this->names; }

void Schema::set_names(const std::vector<std::string> & col_names) {
	this->names = col_names;
}

std::vector<std::shared_ptr<arrow::DataType>> Schema::get_data_types() const {
	std::vector<std::shared_ptr<arrow::DataType>> data_types;
	for(auto type_id : this->types){
		data_types.push_back(get_right_arrow_datatype(type_id));
	}
	return data_types;
}

std::vector<std::string> Schema::get_files() const { return this->files; }

std::vector<arrow::Type::type> Schema::get_dtypes() const { return this->types; }

arrow::Type::type Schema::get_dtype(size_t schema_index) const { return this->types[schema_index]; }

std::string Schema::get_name(size_t schema_index) const { return this->names[schema_index]; }

size_t Schema::get_num_columns() const { return this->names.size(); }

std::vector<bool> Schema::get_in_file() const { return this->in_file; }

bool Schema::all_in_file() const {
	return std::all_of(this->in_file.begin(), this->in_file.end(), [](bool elem) { return elem; });
}

bool Schema::get_has_header_csv() const { return this->has_header_csv; }

void Schema::set_has_header_csv(bool has_header) {
	this->has_header_csv = has_header;
}

void Schema::add_column(std::string name, arrow::Type::type type, size_t file_index, bool is_in_file) {
	this->names.push_back(name);
	this->types.push_back(type);
	this->calcite_to_file_indices.push_back(file_index);
	this->in_file.push_back(is_in_file);
}

void Schema::add_file(std::string file){
	this->files.push_back(file);
}

Schema Schema::fileSchema(size_t current_file_index) const {
	Schema schema;
	for(size_t i = 0; i < this->names.size(); i++) {
		size_t file_index = this->calcite_to_file_indices.empty() ? i : this->calcite_to_file_indices[i];
		if(this->in_file[i]) {
			schema.add_column(this->names[i], this->types[i], file_index);
		}
	}
	// Just get the associated row_groups for current_file_index
	if (this->row_groups_ids.size() > current_file_index){
		schema.row_groups_ids.push_back(this->row_groups_ids.at(current_file_index));
	}
	if (this->files.size() > current_file_index){
		schema.files.push_back(this->files.at(current_file_index));
	}
	return schema;
}

std::unique_ptr<ral::frame::BlazingCudfTable> Schema::makeEmptyBlazingCudfTable(const std::vector<int> & column_indices) const {
	std::vector<std::string> select_names(column_indices.size());
	std::vector<arrow::Type::type> select_types(column_indices.size());
	if (column_indices.empty()) {
		select_names = this->names;
		select_types = this->types;
	} else {
		for (size_t i = 0; i < column_indices.size(); i++){
			select_names[i] = this->names[column_indices[i]];
			select_types[i] = this->types[column_indices[i]];
		}
	}

	return ral::frame::createEmptyBlazingCudfTable(select_types, select_names);
}

std::vector<int> Schema::get_rowgroup_ids(size_t file_index) const {
	if (this->row_groups_ids.size() > file_index) {
		return this->row_groups_ids.at(file_index);
	} else {
		return std::vector<int>{};
	}
}

std::vector<std::vector<int>> Schema::get_rowgroups(){
	return this->row_groups_ids;
}

int Schema::get_total_num_rowgroups() {
	int num_rowgroups = 0;
	for (auto row_group_set : this->row_groups_ids){
		num_rowgroups += row_group_set.size();
	}
	return num_rowgroups;
}

} /* namespace io */
} /* namespace ral */
