#pragma once

#include "blazing_table/BlazingCudfTable.h"
#include "operators/operators_definitions.h"
#include "parser/expression_utils.hpp"

void normalize_types_gpu(std::unique_ptr<ral::frame::BlazingTable> & gpu_table, const std::vector<std::shared_ptr<arrow::DataType>> & types,
	std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );

std::unique_ptr<ral::frame::BlazingCudfTable> create_empty_cudf_table(
  const std::vector<std::string> &column_names,
  const std::vector<std::shared_ptr<arrow::DataType>> &dtypes, std::vector<int> column_indices = {});

std::unique_ptr<cudf::table> create_empty_cudf_table(const std::vector<cudf::type_id> &dtypes);

// This is only for numerics
template<typename T>
std::unique_ptr<cudf::column> vector_to_column(std::vector<T> vect, cudf::data_type type){
	return std::make_unique<cudf::column>(type, vect.size(), rmm::device_buffer{vect.data(), vect.size() * sizeof(T)});
}

// This is only for numerics
template<typename T>
std::vector<T> column_to_vector(cudf::column_view column){
	std::vector<T> host_data(column.size());
  	CUDA_TRY(cudaMemcpy(host_data.data(), column.data<T>(), column.size() * sizeof(T), cudaMemcpyDeviceToHost));
	return host_data;
}

std::vector<cudf::order> toCudfOrderTypes(std::vector<voltron::compute::SortOrder> sortOrderTypes);

std::vector<cudf::null_order> toCudfNullOrderTypes(std::vector<voltron::compute::NullOrder> sortOrderNulls);

cudf::data_type get_common_cudf_type(cudf::data_type type1, cudf::data_type type2, bool strict);

std::shared_ptr<arrow::DataType> cudf_type_id_to_arrow_data_type(cudf::type_id type);

cudf::data_type arrow_type_to_cudf_data_type(arrow::Type::type arrow_type);
