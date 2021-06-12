#include "compute/arrow/detail/scalars.h"
#include "parser/expression_utils.hpp"


// TODO: fill this function 
std::shared_ptr<arrow::Scalar> get_scalar_from_string_arrow(const std::string & scalar_string, std::shared_ptr<arrow::DataType> type, bool strings_have_quotes) {
  // TODO percy arrow
    if (is_null(scalar_string)) {
        //return cudf::make_default_constructed_scalar(type);
    }
    if(type->id() == arrow::Type::BOOL) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = bool;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(scalar_string == "true"));
//		return ret;
    }
    if(type->id() == arrow::Type::UINT8) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = uint8_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoul(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::INT8) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = int8_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoi(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::UINT16) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = uint16_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoul(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::INT16) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = int16_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoi(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::UINT32) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = uint32_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoul(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::INT32) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = int32_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoi(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::UINT64) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = uint64_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoull(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::INT64) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = int64_t;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoll(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::FLOAT) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = float;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stof(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::DOUBLE) {
//		auto ret = cudf::make_numeric_scalar(type);
//		using T = double;
//		using ScalarType = cudf::scalar_type_t<T>;
//		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stod(scalar_string)));
//		return ret;
    }
    if(type->id() == arrow::Type::STRING) {
//		if (strings_have_quotes) {
//			return cudf::make_string_scalar(scalar_string.substr(1, scalar_string.length() - 2));
//		} else {
//			return cudf::make_string_scalar(scalar_string);
//		}
    }
    // TODO: Handle all other types

    assert(false);
}
