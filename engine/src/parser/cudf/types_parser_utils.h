#pragma once

#include <arrow/type.h>
#include <vector>
#include <memory>

#include <cudf/types.hpp>

std::shared_ptr<arrow::DataType>  cudf_type_id_to_arrow_type(cudf::type_id dtype);

cudf::data_type arrow_type_to_cudf_data_type(arrow::Type::type arrow_type);

cudf::data_type arrow_type_to_cudf_data_type(std::shared_ptr<arrow::DataType> arrow_type);
