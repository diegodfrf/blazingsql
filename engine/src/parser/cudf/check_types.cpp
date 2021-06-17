#include "check_types.h"

bool is_type_float(cudf::type_id type) { return (cudf::type_id::FLOAT32 == type || cudf::type_id::FLOAT64 == type); }
bool is_type_integer(cudf::type_id type) {
	return (cudf::type_id::INT8 == type || cudf::type_id::INT16 == type || cudf::type_id::INT32 == type ||
			cudf::type_id::INT64 == type || cudf::type_id::UINT8 == type || cudf::type_id::UINT16 == type ||
			cudf::type_id::UINT32 == type || cudf::type_id::UINT64 == type);
}
bool is_type_bool(cudf::type_id type) { return cudf::type_id::BOOL8 == type; }

bool is_type_timestamp(cudf::type_id type) {
	return (cudf::type_id::TIMESTAMP_DAYS == type || cudf::type_id::TIMESTAMP_SECONDS == type ||
			cudf::type_id::TIMESTAMP_MILLISECONDS == type || cudf::type_id::TIMESTAMP_MICROSECONDS == type ||
			cudf::type_id::TIMESTAMP_NANOSECONDS == type);
}
bool is_type_duration(cudf::type_id type) {
	return (cudf::type_id::DURATION_DAYS == type || cudf::type_id::DURATION_SECONDS == type ||
			cudf::type_id::DURATION_MILLISECONDS == type || cudf::type_id::DURATION_MICROSECONDS == type ||
			cudf::type_id::DURATION_NANOSECONDS == type);
}
bool is_type_string(cudf::type_id type) { return cudf::type_id::STRING == type; }