#pragma once

#include <cudf/types.hpp>
#include <arrow/type.h>
#include <vector>
#include <memory>

cudf::data_type get_common_type(cudf::data_type type1, cudf::data_type type2, bool strict);

std::vector<std::shared_ptr<arrow::DataType>> get_common_types(const std::vector<std::shared_ptr<arrow::DataType>> & types1,
   const std::vector<std::shared_ptr<arrow::DataType>> & types2, bool strict);

arrow::Type::type cudf_type_id_to_arrow_type(cudf::type_id dtype);
 
cudf::data_type arrow_type_to_cudf_data_type(arrow::Type::type arrow_type);
 
std::shared_ptr<arrow::DataType> get_right_arrow_datatype(arrow::Type::type arrow_type);

std::shared_ptr<arrow::DataType> get_arrow_datatype_from_int_value(int32_t value);
