#pragma once

#include <arrow/scalar.h>
#include <cudf/scalar/scalar.hpp>

namespace ral {
namespace cpu {

template<typename CPPType, typename ArrowScalarType>
std::unique_ptr<cudf::scalar> to_cudf_numeric_scalar(cudf::data_type cudf_dtype, std::shared_ptr<arrow::Scalar> arrow_scalar);

std::unique_ptr<cudf::scalar> to_cudf_scalar(std::shared_ptr<arrow::Scalar> arrow_scalar);

} // namespace cpu
} // namespace ral
