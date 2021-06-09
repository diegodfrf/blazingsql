/*
 * DataParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef DATAPARSER_H_
#define DATAPARSER_H_

#include "../Schema.h"
#include "../DataType.h"
#include "../data_provider/DataProvider.h"

#include "blazing_table/BlazingTable.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

namespace ral {
namespace io {

class data_parser {
public:
	virtual std::unique_ptr<ral::frame::BlazingTable> parse_batch(
      ral::execution::execution_backend preferred_compute,
		ral::io::data_handle /*handle*/,
		const Schema & /*schema*/,
		std::vector<int> /*column_indices*/,
		std::vector<cudf::size_type> /*row_groups*/) {
		return nullptr; // TODO cordova ask ALexander why is not a pure virtual function as before
	}

	virtual size_t get_num_partitions(ral::execution::execution_backend preferred_compute) {
		return 0;
	}

	virtual void parse_schema(
		ral::execution::execution_backend preferred_compute,ral::io::data_handle /*handle*/, ral::io::Schema & schema) = 0;

	virtual std::unique_ptr<ral::frame::BlazingTable> get_metadata(ral::execution::execution_backend preferred_compute,
		std::vector<ral::io::data_handle> /*handles*/,
		int /*offset*/,
		std::map<std::string, std::string> /*args_map*/) {
		return nullptr;
	}

	virtual DataType type() const { return 	DataType::UNDEFINED; }

};

} /* namespace io */
} /* namespace ral */

class DataParser {
public:
	DataParser();
	virtual ~DataParser();
};
#endif /* DATAPARSER_H_ */
