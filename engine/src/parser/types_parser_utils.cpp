#include "parser/types_parser_utils.h"

#include "parser/CalciteExpressionParsing.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <numeric>
#include "utilities/error.hpp"

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
  // TODO percy arrow error
  return nullptr;
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
