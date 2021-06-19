#pragma once

#include <arrow/scalar.h>


std::shared_ptr<arrow::Scalar> get_scalar_from_string_arrow(const std::string & scalar_string, std::shared_ptr<arrow::DataType> type, bool strings_have_quotes);
