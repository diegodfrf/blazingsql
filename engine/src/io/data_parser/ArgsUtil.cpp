#include "ArgsUtil.h"

#include <blazingdb/io/FileSystem/Uri.h>
#include <blazingdb/io/Util/StringUtil.h>

#include "../data_provider/UriDataProvider.h"

namespace ral {
namespace io {

DataType inferDataType(std::string file_format_hint) {
	if(file_format_hint == "parquet")
		return DataType::PARQUET;
	if(file_format_hint == "json")
		return DataType::JSON;
	if(file_format_hint == "orc")
		return DataType::ORC;
	if(file_format_hint == "csv")
		return DataType::CSV;
	if(file_format_hint == "psv")
		return DataType::CSV;
	if(file_format_hint == "tbl")
		return DataType::CSV;
	if(file_format_hint == "txt")
		return DataType::CSV;
	if(file_format_hint == "mysql")
		return DataType::MYSQL;
	if(file_format_hint == "postgresql")
		return DataType::POSTGRESQL;
	if(file_format_hint == "sqlite")
		return DataType::SQLITE;
	if(file_format_hint == "snowflake")
		return DataType::SNOWFLAKE;
	// NOTE if you need more options the user can pass file_format in the create table

	return DataType::UNDEFINED;
}

DataType inferFileType(std::vector<std::string> files,
                       DataType data_type_hint,
                       bool ignore_missing_paths) {
  if (data_type_hint == DataType::PARQUET || data_type_hint == DataType::CSV ||
      data_type_hint == DataType::JSON || data_type_hint == DataType::ORC ||
      data_type_hint == DataType::MYSQL ||
      data_type_hint == DataType::POSTGRESQL ||
      data_type_hint == DataType::SQLITE ||
      data_type_hint == DataType::SNOWFLAKE) {
    return data_type_hint;
  }

  std::vector<Uri> uris;
	std::transform(
		files.begin(), files.end(), std::back_inserter(uris), [](std::string uri) -> Uri { return Uri(uri); });
	ral::io::uri_data_provider udp(uris, ignore_missing_paths);
	bool open_file = false;
	const ral::io::data_handle dh = udp.get_next(open_file);
	std::string ext = dh.uri.getPath().getFileExtension();
	std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

	return inferDataType(ext);
}

bool map_contains(std::string key, std::map<std::string, std::string> args) { return !(args.find(key) == args.end()); }

bool to_bool(std::string value) {
	if(value == "True")
		return true;
	if(value == "False")
		return false;
	return false;
}

char ord(std::string value) { return (char) value[0]; }

int to_int(std::string value) { return std::atoi(value.c_str()); }

std::vector<std::string> to_vector_string(std::string value) {
	std::string vec = StringUtil::replace(value, "'", "");
	// removing `[` and `]` characters
	vec = vec.substr(1, vec.size() - 2);
	vec = StringUtil::replace(vec, " ", "");
	std::vector<std::string> ret = StringUtil::split(vec, ",");
	return ret;
}

std::vector<int> to_vector_int(std::string value) {
	std::vector<std::string> input = to_vector_string(value);
	std::vector<int> ret;
	std::transform(input.begin(), input.end(), std::back_inserter(ret), [](std::string v) -> int { return to_int(v); });
	return ret;
}

std::map<std::string, std::string> to_map(std::vector<std::string> arg_keys, std::vector<std::string> arg_values) {
	std::map<std::string, std::string> ret;
	for(size_t i = 0; i < arg_keys.size(); ++i) {
		ret[arg_keys[i]] = arg_values[i];
	}
	return ret;
}

std::string getDataTypeName(DataType dataType) {
	switch(dataType) {
	case DataType::PARQUET: return "parquet"; break;
	case DataType::ORC: return "orc"; break;
	case DataType::CSV: return "csv"; break;
	case DataType::JSON: return "json"; break;
	case DataType::CUDF: return "cudf"; break;
	case DataType::DASK_CUDF: return "dask_cudf"; break;
	case DataType::ARROW: return "arrow"; break;
	case DataType::MYSQL: return "mysql"; break;
	case DataType::POSTGRESQL: return "postgresql"; break;
	case DataType::SQLITE: return "sqlite"; break;
	case DataType::SNOWFLAKE: return "snowflake"; break;
	default: break;
	}

	return "undefined";
}

sql_info getSqlInfo(std::map<std::string, std::string> &args_map) {
  // TODO percy william maybe we can move this constant as a bc.BlazingContext config opt
  const size_t DETAULT_TABLE_BATCH_SIZE = 100000;
  // TODO(percy, cristhian): add exception for key error and const
  // TODO(percy, cristhian): for sqlite, add contionals to avoid unncessary fields
  sql_info sql;
  if (args_map.find("hostname") != args_map.end()) {
    sql.host = args_map.at("hostname");
  }
  if (args_map.find("port") != args_map.end()) {
    sql.port = static_cast<std::size_t>(std::atoll(args_map["port"].data()));
  }
  if (args_map.find("username") != args_map.end()) {
    sql.user = args_map.at("username");
  }
  if (args_map.find("password") != args_map.end()) {
    sql.password = args_map.at("password");
  }
  if (args_map.find("database") != args_map.end()) {
    sql.schema = args_map.at("database");
  }
  if (args_map.find("table") != args_map.end()) {
    sql.table = args_map.at("table");
  }
  if (args_map.find("table_filter") != args_map.end()) {
    sql.table_filter = args_map.at("table_filter");
  }
  if (args_map.find("table_batch_size") != args_map.end()) {
    if (args_map.at("table_batch_size").empty()) {
      sql.table_batch_size = DETAULT_TABLE_BATCH_SIZE;
    } else {
      sql.table_batch_size = static_cast<std::size_t>(std::atoll(args_map.at("table_batch_size").data()));
    }
  } else {
    sql.table_batch_size = DETAULT_TABLE_BATCH_SIZE;
  }
  if (args_map.find("dsn") != args_map.end()) {
    sql.dsn = args_map.at("dsn");
  }
  if (args_map.find("server") != args_map.end()) {
    sql.server = args_map.at("server");
  }
  if (args_map.find("schema") != args_map.end()) {
    // TODO (cristhian): move sub_schema to replace schema member
    // and move schema to replace database member simultaneously
    sql.sub_schema = args_map.at("schema");
  }
  return sql;
}

} /* namespace io */
} /* namespace ral */
