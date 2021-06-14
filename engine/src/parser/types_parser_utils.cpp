#include "parser/types_parser_utils.h"

#include "parser/CalciteExpressionParsing.h"
#include <cudf/filling.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/copying.hpp>
#include <cudf/unary.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <numeric>
#include "blazing_table/BlazingColumnOwner.h"
#include <cudf/detail/interop.hpp>
#include "compute/backend_dispatcher.h"
#include "compute/api.h"

std::vector<std::shared_ptr<arrow::DataType>> get_common_types(const std::vector<std::shared_ptr<arrow::DataType>> & types1,
	const std::vector<std::shared_ptr<arrow::DataType>> & types2, bool strict){
	RAL_EXPECTS(types1.size() == types2.size(), "In get_common_types: Mismatched number of columns");
	std::vector<std::shared_ptr<arrow::DataType>> common_types(types1.size());
	for(size_t j = 0; j < common_types.size(); j++) {
		common_types[j] = get_common_type(types1[j], types2[j], strict);
	}
	return common_types;
}

std::shared_ptr<arrow::DataType> get_common_type(std::shared_ptr<arrow::DataType> type1, std::shared_ptr<arrow::DataType> type2, bool strict) {
	if (type1 == type2) {
		return type1;
	} else if((is_type_float_arrow(type1->id()) && is_type_float_arrow(type2->id())) || (is_type_integer_arrow(type1->id()) && is_type_integer_arrow(type2->id()))) {
		return (arrow::internal::GetByteWidth(*type1) >= arrow::internal::GetByteWidth(*type2))	? type1	: type2;
	} else if(is_type_timestamp_arrow(type1->id()) && is_type_timestamp_arrow(type2->id())) {
		// TODO: handle units better
		return type1;
	} else if ( is_type_string_arrow(type1->id()) && is_type_timestamp_arrow(type2->id()) ) {
		return type2;
	}
	if (strict) {
		RAL_FAIL("No common type between " + std::to_string(static_cast<int32_t>(type1->id())) + " and " + std::to_string(static_cast<int32_t>(type2->id())));
	} else {
		if(is_type_float_arrow(type1->id()) && is_type_integer_arrow(type2->id())) {
			return type1;
		} else if (is_type_float_arrow(type2->id()) && is_type_integer_arrow(type1->id())) {
			return type2;
		} else if (is_type_bool_arrow(type1->id()) && (is_type_integer_arrow(type2->id()) || is_type_float_arrow(type2->id()) || is_type_string_arrow(type2->id()) )){
			return type2;
		} else if (is_type_bool_arrow(type2->id()) && (is_type_integer_arrow(type1->id()) || is_type_float_arrow(type1->id()) || is_type_string_arrow(type1->id()) )){
			return type1;
		}
	}
}

std::shared_ptr<arrow::DataType> cudf_type_id_to_arrow_data_type(cudf::type_id type) {
	switch (type)
	{
    case cudf::type_id::BOOL8: return arrow::boolean();
	case cudf::type_id::UINT8: return arrow::uint8();
    case cudf::type_id::INT8: return arrow::int8();
	case cudf::type_id::UINT16: return arrow::uint16();
    case cudf::type_id::INT16: return arrow::int16();
	case cudf::type_id::UINT32: return arrow::uint32();
    case cudf::type_id::INT32: return arrow::int32();
	case cudf::type_id::UINT64: return arrow::uint64();
    case cudf::type_id::INT64: return arrow::int64();
    case cudf::type_id::FLOAT32: return arrow::float32();
    case cudf::type_id::FLOAT64: return arrow::float64();
    case cudf::type_id::STRING: return arrow::utf8();
    case cudf::type_id::TIMESTAMP_DAYS: return arrow::date32();
    case cudf::type_id::TIMESTAMP_SECONDS: return arrow::timestamp(arrow::TimeUnit::SECOND);
    case cudf::type_id::TIMESTAMP_MILLISECONDS: return arrow::timestamp(arrow::TimeUnit::MILLI);
    case cudf::type_id::TIMESTAMP_MICROSECONDS: return arrow::timestamp(arrow::TimeUnit::MICRO);
    case cudf::type_id::TIMESTAMP_NANOSECONDS: return arrow::timestamp(arrow::TimeUnit::NANO);
    //case cudf::type_id::DURATION_DAYS: return arrow::Type::DURATION; 
    case cudf::type_id::DURATION_SECONDS: return arrow::duration(arrow::TimeUnit::SECOND);
    case cudf::type_id::DURATION_MILLISECONDS: return arrow::duration(arrow::TimeUnit::MILLI);
    case cudf::type_id::DURATION_MICROSECONDS: return arrow::duration(arrow::TimeUnit::MICRO);
    case cudf::type_id::DURATION_NANOSECONDS: return arrow::duration(arrow::TimeUnit::NANO);
    // TODO: enables more types
    default: return arrow::null();
	}
}

arrow::Type::type cudf_type_id_to_arrow_type(cudf::type_id type) {
  return cudf_type_id_to_arrow_data_type(type)->id();
}

cudf::data_type arrow_type_to_cudf_data_type(arrow::Type::type arrow_type) {
	switch (arrow_type)
	{
	case arrow::Type::NA: return cudf::data_type(cudf::type_id::EMPTY);
	case arrow::Type::BOOL: return cudf::data_type(cudf::type_id::BOOL8);
	case arrow::Type::INT8: return cudf::data_type(cudf::type_id::INT8);
	case arrow::Type::INT16: return cudf::data_type(cudf::type_id::INT16);
	case arrow::Type::INT32: return cudf::data_type(cudf::type_id::INT32);
	case arrow::Type::INT64: return cudf::data_type(cudf::type_id::INT64);
	case arrow::Type::UINT8: return cudf::data_type(cudf::type_id::UINT8);
	case arrow::Type::UINT16: return cudf::data_type(cudf::type_id::UINT16);
	case arrow::Type::UINT32: return cudf::data_type(cudf::type_id::UINT32);
	case arrow::Type::UINT64: return cudf::data_type(cudf::type_id::UINT64);
	case arrow::Type::FLOAT: return cudf::data_type(cudf::type_id::FLOAT32);
	case arrow::Type::DOUBLE: return cudf::data_type(cudf::type_id::FLOAT64);
	case arrow::Type::DATE32: return cudf::data_type(cudf::type_id::TIMESTAMP_DAYS);
	case arrow::Type::STRING: return cudf::data_type(cudf::type_id::STRING);
	case arrow::Type::DICTIONARY: return cudf::data_type(cudf::type_id::DICTIONARY32);
	case arrow::Type::LIST: return cudf::data_type(cudf::type_id::LIST);
	case arrow::Type::STRUCT: return cudf::data_type(cudf::type_id::STRUCT);
	// TODO: for now we are handling just MILLI
	case arrow::Type::TIMESTAMP: return cudf::data_type(cudf::type_id::TIMESTAMP_MILLISECONDS);
	case arrow::Type::DURATION: return cudf::data_type(cudf::type_id::DURATION_MILLISECONDS);

		// TODO: enables more types
		/*
		case arrow::Type::TIMESTAMP: {
      auto type = static_cast<arrow::TimestampType const>(arrow_type);
      switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: return cudf::data_type(cudf::type_id::TIMESTAMP_SECONDS);
        case arrow::TimeUnit::type::MILLI: return cudf::data_type(cudf::type_id::TIMESTAMP_MILLISECONDS);
        case arrow::TimeUnit::type::MICRO: return cudf::data_type(cudf::type_id::TIMESTAMP_MICROSECONDS);
        case arrow::TimeUnit::type::NANO: return cudf::data_type(cudf::type_id::TIMESTAMP_NANOSECONDS);
        default: throw std::runtime_error("Unsupported timestamp unit in arrow");
      }
    }
    case arrow::Type::DURATION: {
      auto type = static_cast<arrow::DurationType const>(arrow_type);
      switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: return cudf::data_type(cudf::type_id::DURATION_SECONDS);
        case arrow::TimeUnit::type::MILLI: return cudf::data_type(cudf::type_id::DURATION_MILLISECONDS);
        case arrow::TimeUnit::type::MICRO: return cudf::data_type(cudf::type_id::DURATION_MICROSECONDS);
        case arrow::TimeUnit::type::NANO: return cudf::data_type(cudf::type_id::DURATION_NANOSECONDS);
        default: throw std::runtime_error("Unsupported duration unit in arrow");
      }
    }
    case arrow::Type::DECIMAL: {
      auto const type = static_cast<arrow::Decimal128Type const>(arrow_type);
      return data_type{cudf::type_id::DECIMAL64, -type->scale()};
    }
	*/
	default: throw std::runtime_error("Unsupported cudf::type_id conversion to cudf");
    }
}

std::shared_ptr<arrow::DataType> get_right_arrow_datatype(arrow::Type::type arrow_type) {

    switch (arrow_type)
    {
	case arrow::Type::BOOL: return arrow::boolean();
	case arrow::Type::UINT8: return arrow::uint8();
	case arrow::Type::INT8: return arrow::int8();
	case arrow::Type::UINT16: return arrow::uint16();
	case arrow::Type::INT16: return arrow::int16();
	case arrow::Type::UINT32: return arrow::uint32();
    case arrow::Type::INT32: return arrow::int32();
	case arrow::Type::UINT64: return arrow::uint64();
	case arrow::Type::INT64: return arrow::int64();
    case arrow::Type::FLOAT: return arrow::float32();
    case arrow::Type::DOUBLE: return arrow::float64();
    case arrow::Type::STRING: return arrow::utf8();
	// TODO: for now we are handling just MILLI
	case arrow::Type::TIMESTAMP: return arrow::timestamp(arrow::TimeUnit::type::MILLI);
	// TODO: enables more types
    default: return arrow::null();
    }
}

// TODO: This function is just useful for few cases (BlazingHostTable , CPUCacheData)
// Remove when it's needed
std::shared_ptr<arrow::DataType> get_arrow_datatype_from_int_value(int32_t value) {
	switch (value)
    {
	case 1: return arrow::boolean();
	case 2: return arrow::uint8();
	case 3: return arrow::int8();
	case 4: return arrow::uint16();
	case 5: return arrow::int16();
	case 6: return arrow::uint32();
    case 7: return arrow::int32();
	case 8: return arrow::uint64();
	case 9: return arrow::int64();
    case 11: return arrow::float32();
    case 12: return arrow::float64();
    case 13: return arrow::utf8();
	// TODO: enables more types
    default: return arrow::null();
    }
}
