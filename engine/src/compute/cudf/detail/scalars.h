#pragma once

#include <cudf/scalar/scalar.hpp>


std::unique_ptr<cudf::scalar> get_max_integer_scalar(cudf::data_type type);

std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string, cudf::data_type type, bool strings_have_quotes = true);



namespace strings {

std::unique_ptr<cudf::scalar> str_to_timestamp_scalar( std::string const& str, cudf::data_type timestamp_type, std::string const& format );

} // namespace strings
