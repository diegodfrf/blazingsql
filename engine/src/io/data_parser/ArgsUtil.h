#ifndef _BZ_RAL_ARGS_UTIL_H_
#define _BZ_RAL_ARGS_UTIL_H_

#include "io/io.h"
#include <cudf/io/json.hpp>
#include <cudf/io/orc.hpp>
#include <cudf/io/datasource.hpp>
#include "../data_provider/sql/AbstractSQLDataProvider.h"

namespace ral {
namespace io {

DataType inferDataType(std::string file_format_hint);

DataType inferFileType(std::vector<std::string> files, DataType data_type_hint, bool ignore_missing_paths = false);

bool in(std::string key, std::map<std::string, std::string> args);

bool to_bool(std::string value);

char ord(std::string value);

int to_int(std::string value);

std::vector<std::string> to_vector_string(std::string value);

cudf::io::json_reader_options getJsonReaderOptions(const std::map<std::string, std::string> & args, cudf::io::arrow_io_source & arrow_source);

cudf::io::orc_reader_options getOrcReaderOptions(const std::map<std::string, std::string> & args, cudf::io::arrow_io_source & arrow_source);

std::map<std::string, std::string> to_map(std::vector<std::string> arg_keys, std::vector<std::string> arg_values);

std::string getDataTypeName(DataType dataType);

sql_info getSqlInfo(std::map<std::string, std::string> &args_map);

// TODO percy arrow it seems we have here generic cpp utils
bool map_contains(std::string key, std::map<std::string, std::string> args);
std::vector<int> to_vector_int(std::string value);

} /* namespace io */
} /* namespace ral */

#endif /* _BZ_RAL_ARGS_UTIL_H_ */
