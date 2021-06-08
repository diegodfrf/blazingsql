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
		// TODO: cordova - arrow types
		//common_types[j] = get_common_type(types1[j], types2[j], strict);
	}
	return common_types;
}

cudf::data_type get_common_type(cudf::data_type type1, cudf::data_type type2, bool strict) {
	if(type1 == type2) {
		return type1;
	} else if((is_type_float(type1.id()) && is_type_float(type2.id())) || (is_type_integer(type1.id()) && is_type_integer(type2.id()))) {
		return (cudf::size_of(type1) >= cudf::size_of(type2))	? type1	: type2;
	} else if(is_type_timestamp(type1.id()) && is_type_timestamp(type2.id())) {
		// if they are both datetime, return the highest resolution either has
		static constexpr std::array<cudf::data_type, 5> datetime_types = {
			cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_MICROSECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}
		};

		for (auto datetime_type : datetime_types){
			if(type1 == datetime_type || type2 == datetime_type)
				return datetime_type;
		}
	} else if(is_type_duration(type1.id()) && is_type_duration(type2.id())) {
		// if they are both durations, return the highest resolution either has
		static constexpr std::array<cudf::data_type, 5> duration_types = {
			cudf::data_type{cudf::type_id::DURATION_NANOSECONDS},
			cudf::data_type{cudf::type_id::DURATION_MICROSECONDS},
			cudf::data_type{cudf::type_id::DURATION_MILLISECONDS},
			cudf::data_type{cudf::type_id::DURATION_SECONDS},
			cudf::data_type{cudf::type_id::DURATION_DAYS}
		};

		for (auto duration_type : duration_types){
			if(type1 == duration_type || type2 == duration_type)
				return duration_type;
		}
	}
	else if ( is_type_string(type1.id()) && is_type_timestamp(type2.id()) ) {
		return type2;
	}
	if (strict) {
		RAL_FAIL("No common type between " + std::to_string(static_cast<int32_t>(type1.id())) + " and " + std::to_string(static_cast<int32_t>(type2.id())));
	} else {
		if(is_type_float(type1.id()) && is_type_integer(type2.id())) {
			return type1;
		} else if (is_type_float(type2.id()) && is_type_integer(type1.id())) {
			return type2;
		} else if (is_type_bool(type1.id()) && (is_type_integer(type2.id()) || is_type_float(type2.id()) || is_type_string(type2.id()) )){
			return type2;
		} else if (is_type_bool(type2.id()) && (is_type_integer(type1.id()) || is_type_float(type1.id()) || is_type_string(type1.id()) )){
			return type1;
		}
	}
}

arrow::Type::type cudf_type_id_to_arrow_type(cudf::type_id type) {
	switch (type)
	{
    case cudf::type_id::BOOL8: return arrow::Type::BOOL;
    case cudf::type_id::INT8: return arrow::Type::INT8;
    case cudf::type_id::INT16: return arrow::Type::INT16;
    case cudf::type_id::INT32: return arrow::Type::INT32;
    case cudf::type_id::INT64: return arrow::Type::INT64;
    case cudf::type_id::UINT8: return arrow::Type::UINT8;
    case cudf::type_id::UINT16: return arrow::Type::UINT16;
    case cudf::type_id::UINT32: return arrow::Type::UINT32;
    case cudf::type_id::UINT64: return arrow::Type::UINT64;
    case cudf::type_id::FLOAT32: return arrow::Type::FLOAT;
    case cudf::type_id::FLOAT64: return arrow::Type::DOUBLE;
    case cudf::type_id::STRING: return arrow::Type::STRING;
    case cudf::type_id::TIMESTAMP_DAYS: return arrow::Type::DATE32;
    case cudf::type_id::TIMESTAMP_SECONDS: //return arrow::TimeUnit::type::SECOND;
    case cudf::type_id::TIMESTAMP_MILLISECONDS: //arrow::TimeUnit::type::MILLI;
    case cudf::type_id::TIMESTAMP_MICROSECONDS: //return arrow::TimeUnit::type::MICRO;
    case cudf::type_id::TIMESTAMP_NANOSECONDS: //return arrow::TimeUnit::type::NANO;
		return arrow::Type::TIMESTAMP;
    case cudf::type_id::DURATION_DAYS: return arrow::Type::DURATION; 
    case cudf::type_id::DURATION_SECONDS: //return arrow::TimeUnit::type::SECOND;
    case cudf::type_id::DURATION_MILLISECONDS: //return arrow::TimeUnit::type::MILLI;
    case cudf::type_id::DURATION_MICROSECONDS: //return arrow::TimeUnit::type::MICRO;
    case cudf::type_id::DURATION_NANOSECONDS: //return arrow::TimeUnit::type::NANO;
			return arrow::Type::DURATION;
    case cudf::type_id::LIST: return arrow::Type::LIST;
    case cudf::type_id::STRUCT: return arrow::Type::STRUCT;
    case cudf::type_id::DICTIONARY32: return arrow::Type::DICTIONARY;
    // TODO: DECIMAL32, DECIMAL64
    default: return arrow::Type::NA;
	}
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
	case arrow::Type::type::BOOL: return arrow::boolean();
	case arrow::Type::type::UINT8: return arrow::uint8();
	case arrow::Type::type::INT8: return arrow::int8();
	case arrow::Type::type::UINT16: return arrow::uint16();
	case arrow::Type::type::INT16: return arrow::int16();
	case arrow::Type::type::UINT32: return arrow::uint32();
    case arrow::Type::type::INT32: return arrow::int32();
	case arrow::Type::type::UINT64: return arrow::uint64();
	case arrow::Type::type::INT64: return arrow::int64();
    case arrow::Type::type::FLOAT: return arrow::float32();
    case arrow::Type::type::DOUBLE: return arrow::float64();
    case arrow::Type::type::STRING: return arrow::utf8();
	// TODO: enables more types
    default: return arrow::null();
    }
}

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