#include "compute/cudf/detail/types.h"

#include <cudf/unary.hpp>
#include <cudf/column/column_factories.hpp>
#include "utilities/error.hpp"
#include <numeric>
#include "blazing_table/BlazingColumnOwner.h"
#include "parser/types_parser_utils.h"

// TODO: cordova , undefined symbol
void normalize_types_gpu(std::unique_ptr<ral::frame::BlazingTable> & gpu_table,  const std::vector<std::shared_ptr<arrow::DataType>>  & types,
		std::vector<cudf::size_type> column_indices) {
	// TODO: cordova , undefined symbol
	/*
  	auto table = dynamic_cast<ral::frame::BlazingCudfTable*>(gpu_table.get());
  
	if (column_indices.size() == 0){
		RAL_EXPECTS(static_cast<size_t>(table->num_columns()) == types.size(), "In normalize_types: table->num_columns() != types.size()");
		column_indices.resize(table->num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	} else {
		RAL_EXPECTS(column_indices.size() == types.size(), "In normalize_types: column_indices.size() != types.size()");
	}
	std::vector<std::unique_ptr<ral::frame::BlazingColumn>> columns = table->releaseBlazingColumns();
	for (size_t i = 0; i < column_indices.size(); i++){
		if (!(columns[column_indices[i]]->view().type() == types[i])){
			std::unique_ptr<cudf::column> casted = cudf::cast(columns[column_indices[i]]->view(), types[i]);
			columns[column_indices[i]] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(casted));
		}
	}
	gpu_table = std::make_unique<ral::frame::BlazingCudfTable>(std::move(columns), table->column_names());
	*/
}

std::unique_ptr<ral::frame::BlazingCudfTable> create_empty_cudf_table(const std::vector<std::string> &column_names,
	const std::vector<std::shared_ptr<arrow::DataType>> &dtypes, std::vector<size_t> column_indices) {

	if (column_indices.size() == 0){
		column_indices.resize(column_names.size());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	std::vector<std::unique_ptr<cudf::column>> columns(column_indices.size());

	for (auto idx : column_indices) {
		cudf::data_type dtype = arrow_type_to_cudf_data_type(dtypes[idx]->id());
		columns[idx] = cudf::make_empty_column(dtype);
	}
	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(table), column_names);
}

std::unique_ptr<cudf::table> create_empty_cudf_table(const std::vector<std::shared_ptr<arrow::Type::type>> &dtypes) {
	std::vector<std::unique_ptr<cudf::column>> columns(dtypes.size());
	for (size_t idx =0; idx < dtypes.size(); idx++) {
		cudf::data_type dtype = arrow_type_to_cudf_data_type(*dtypes[idx]);
		columns[idx] = cudf::make_empty_column(dtype);
	}
	return std::make_unique<cudf::table>(std::move(columns));
}

std::vector<cudf::order> toCudfOrderTypes(std::vector<voltron::compute::SortOrder> sortOrderTypes) {
  std::vector<cudf::order> cudfOrderTypes;
  cudfOrderTypes.resize(sortOrderTypes.size());

  for(std::size_t idx; idx < cudfOrderTypes.size(); idx++){
    switch(sortOrderTypes[idx]){
      case voltron::compute::SortOrder::ASCENDING:
        cudfOrderTypes[idx]=cudf::order::ASCENDING;
      break;
      case voltron::compute::SortOrder::DESCENDING:
        cudfOrderTypes[idx]=cudf::order::DESCENDING;
      break;
    }
  }
}

std::vector<cudf::null_order> toCudfNullOrderTypes(std::vector<voltron::compute::NullOrder> sortNullOrderTypes) {
  std::vector<cudf::null_order> cudfNullOrderTypes;
  cudfNullOrderTypes.resize(sortNullOrderTypes.size());

  for(std::size_t idx; idx < cudfNullOrderTypes.size(); idx++){
    switch(sortNullOrderTypes[idx]){
      case voltron::compute::NullOrder::AFTER:
        cudfNullOrderTypes[idx]=cudf::null_order::AFTER;
      break;
      case voltron::compute::NullOrder::BEFORE:
        cudfNullOrderTypes[idx]=cudf::null_order::BEFORE;
      break;
    }
  }
}

