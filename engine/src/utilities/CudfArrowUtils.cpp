#include "CudfArrowUtils.h"

#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/detail/interop.hpp>

namespace ral {
namespace cpu {

template<typename CPPType, typename ArrowScalarType>
std::unique_ptr<cudf::scalar> to_cudf_numeric_scalar(cudf::data_type cudf_dtype, std::shared_ptr<arrow::Scalar> arrow_scalar) {
  std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf_dtype);
  auto numeric_s = static_cast< cudf::scalar_type_t<CPPType>* >(scalar.get());
  std::shared_ptr<ArrowScalarType> s = std::static_pointer_cast<ArrowScalarType>(arrow_scalar);
  numeric_s->set_value((CPPType)s->value);
  return scalar;
}

std::unique_ptr<cudf::scalar> to_cudf_scalar(std::shared_ptr<arrow::Scalar> arrow_scalar)
{
  cudf::data_type cudf_dtype = cudf::detail::arrow_to_cudf_type(*arrow_scalar->type);
  switch (arrow_scalar->type->id()) {
    case arrow::Type::NA: {} break;
    case arrow::Type::BOOL: {} break;
    case arrow::Type::INT8: {
      return to_cudf_numeric_scalar<int8_t, arrow::Int8Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::INT16: {
      return to_cudf_numeric_scalar<int16_t, arrow::Int16Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::INT32: {
      return to_cudf_numeric_scalar<int32_t, arrow::Int32Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::INT64: {
      return to_cudf_numeric_scalar<int64_t, arrow::Int64Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT8: {
      return to_cudf_numeric_scalar<uint8_t, arrow::UInt8Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT16: {
      return to_cudf_numeric_scalar<uint16_t, arrow::UInt16Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT32: {
      return to_cudf_numeric_scalar<uint32_t, arrow::UInt32Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT64: {
      return to_cudf_numeric_scalar<uint64_t, arrow::UInt64Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::FLOAT: {
      return to_cudf_numeric_scalar<float, arrow::FloatScalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::DOUBLE: {
      return to_cudf_numeric_scalar<double, arrow::DoubleScalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::DATE32: {} break;
    case arrow::Type::TIMESTAMP: {
      // TODO percy arrow
//      auto type = static_cast<arrow::TimestampType const*>(&arrow_type);
//      switch (type->unit()) {
//        case arrow::TimeUnit::type::SECOND: return data_type(type_id::TIMESTAMP_SECONDS);
//        case arrow::TimeUnit::type::MILLI: return data_type(type_id::TIMESTAMP_MILLISECONDS);
//        case arrow::TimeUnit::type::MICRO: return data_type(type_id::TIMESTAMP_MICROSECONDS);
//        case arrow::TimeUnit::type::NANO: return data_type(type_id::TIMESTAMP_NANOSECONDS);
//        default: CUDF_FAIL("Unsupported timestamp unit in arrow");
//      }
    } break;
    case arrow::Type::DURATION: {
      // TODO percy arrow
//      auto type = static_cast<arrow::DurationType const*>(&arrow_type);
//      switch (type->unit()) {
//        case arrow::TimeUnit::type::SECOND: return data_type(type_id::DURATION_SECONDS);
//        case arrow::TimeUnit::type::MILLI: return data_type(type_id::DURATION_MILLISECONDS);
//        case arrow::TimeUnit::type::MICRO: return data_type(type_id::DURATION_MICROSECONDS);
//        case arrow::TimeUnit::type::NANO: return data_type(type_id::DURATION_NANOSECONDS);
//        default: CUDF_FAIL("Unsupported duration unit in arrow");
//      }
    } break;
    case arrow::Type::STRING: {
      std::shared_ptr<arrow::StringScalar> s = std::static_pointer_cast<arrow::StringScalar>(arrow_scalar);
      return cudf::make_string_scalar(s->value->ToString());
    } break;
    case arrow::Type::DICTIONARY: {} break;
    case arrow::Type::LIST: {} break;
    case arrow::Type::DECIMAL: {
      // TODO percy arrow
      //auto const type = static_cast<arrow::Decimal128Type const*>(&arrow_type);
      //return data_type{type_id::DECIMAL64, -type->scale()};
    } break;
    case arrow::Type::STRUCT: {} break;
  }

  // TODO percy arrow thrown error
}

} // namespace cpu
} // namespace ral
