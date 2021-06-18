#pragma once

#include <string>
#include <vector>
#include "cudf/types.hpp"

bool is_type_float(cudf::type_id type);
bool is_type_integer(cudf::type_id type);
bool is_type_bool(cudf::type_id type);
bool is_type_timestamp(cudf::type_id type);
bool is_type_duration(cudf::type_id type) ;
bool is_type_string(cudf::type_id type);
