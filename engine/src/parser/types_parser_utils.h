#pragma once

#include <arrow/type.h>
#include <vector>
#include <memory>

std::shared_ptr<arrow::DataType> get_common_type(std::shared_ptr<arrow::DataType> type1, std::shared_ptr<arrow::DataType> type2, bool strict);

std::vector<std::shared_ptr<arrow::DataType>> get_common_types(const std::vector<std::shared_ptr<arrow::DataType>> & types1,
   const std::vector<std::shared_ptr<arrow::DataType>> & types2, bool strict);

std::shared_ptr<arrow::DataType> get_right_arrow_datatype(arrow::Type::type arrow_type);

std::shared_ptr<arrow::DataType> get_arrow_datatype_from_int_value(int32_t value);

std::shared_ptr<arrow::DataType> string_to_arrow_datatype(const std::string &str_type);
