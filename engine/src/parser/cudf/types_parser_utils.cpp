#include "types_parser_utils.h"

#include "parser/CalciteExpressionParsing.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <numeric>
#include "utilities/error.hpp"


std::shared_ptr<arrow::DataType> cudf_type_id_to_arrow_type(cudf::type_id type) {
  switch (type)
  {
    case cudf::type_id::BOOL8: return arrow::boolean();
    case cudf::type_id::INT8: return arrow::int8();
    case cudf::type_id::INT16: return arrow::int16();
    case cudf::type_id::INT32: return arrow::int32();
    case cudf::type_id::INT64: return arrow::int64();
    case cudf::type_id::UINT8: return arrow::uint8();
    case cudf::type_id::UINT16: return arrow::uint16();
    case cudf::type_id::UINT32: return arrow::uint32();
    case cudf::type_id::UINT64: return arrow::uint64();
    case cudf::type_id::FLOAT32: return arrow::float32();
    case cudf::type_id::FLOAT64: return arrow::float64();
    case cudf::type_id::STRING: return arrow::utf8();
    case cudf::type_id::TIMESTAMP_DAYS: return arrow::date32();
    case cudf::type_id::TIMESTAMP_SECONDS: //return arrow::TimeUnit::type::SECOND;
      return arrow::timestamp(arrow::TimeUnit::type::SECOND);
    case cudf::type_id::TIMESTAMP_MILLISECONDS: //arrow::TimeUnit::type::MILLI;
      return arrow::timestamp(arrow::TimeUnit::type::MILLI);
    case cudf::type_id::TIMESTAMP_MICROSECONDS: //return arrow::TimeUnit::type::MICRO;
      return arrow::timestamp(arrow::TimeUnit::type::MICRO);
    case cudf::type_id::TIMESTAMP_NANOSECONDS: //return arrow::TimeUnit::type::NANO;
      return arrow::timestamp(arrow::TimeUnit::type::NANO);

// TODO
//    case cudf::type_id::DURATION_DAYS: return arrow::duration(arrow::TimeUnit::type::SECOND); // TODO: check this??

    case cudf::type_id::DURATION_SECONDS: //return arrow::TimeUnit::type::SECOND;
      return arrow::duration(arrow::TimeUnit::type::SECOND);
    case cudf::type_id::DURATION_MILLISECONDS: //return arrow::TimeUnit::type::MILLI;
      return arrow::duration(arrow::TimeUnit::type::MILLI);
    case cudf::type_id::DURATION_MICROSECONDS: //return arrow::TimeUnit::type::MICRO;
      return arrow::duration(arrow::TimeUnit::type::MICRO);
    case cudf::type_id::DURATION_NANOSECONDS: //return arrow::TimeUnit::type::NANO;
      return arrow::duration(arrow::TimeUnit::type::NANO);

    //TODO: Check this/?
    case cudf::type_id::LIST: return arrow::list(arrow::int32());
    case cudf::type_id::STRUCT: return arrow::struct_({});
    case cudf::type_id::DICTIONARY32: return arrow::dictionary(arrow::int32(), arrow::int32());
      // TODO: DECIMAL32, DECIMAL64
    default: return arrow::null();
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

cudf::data_type arrow_type_to_cudf_data_type(std::shared_ptr<arrow::DataType> arrow_type) {
  return arrow_type_to_cudf_data_type(arrow_type->id());
}

