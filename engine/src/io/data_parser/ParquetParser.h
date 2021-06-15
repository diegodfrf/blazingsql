/*
 * ParquetParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef PARQUETPARSER_H_
#define PARQUETPARSER_H_

#include "../data_provider/DataProvider.h"
#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

namespace ral {
namespace io {

class parquet_parser : public data_parser {
public:
	parquet_parser();
	virtual ~parquet_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
      ral::execution::execution_backend preferred_compute,
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(ral::execution::execution_backend preferred_compute, ral::io::data_handle handle, Schema & schema);

	std::unique_ptr<ral::frame::BlazingTable> get_metadata( ral::execution::execution_backend preferred_compute,
		std::vector<ral::io::data_handle> handles, int offset,
		std::map<std::string, std::string> args_map);

	DataType type() const override { return DataType::PARQUET; }
};

} /* namespace io */
} /* namespace ral */

#endif /* PARQUETPARSER_H_ */
