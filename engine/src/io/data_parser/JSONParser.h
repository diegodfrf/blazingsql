#pragma once

#include <arrow/io/interfaces.h>
#include <memory>
#include <vector>

#include "DataParser.h"

namespace ral {
namespace io {

class json_parser : public data_parser {
public:
	json_parser(std::map<std::string, std::string> args_map);

	virtual ~json_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(ral::execution::execution_backend preferred_compute, ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<int> row_groups) override;

	void parse_schema(ral::execution::execution_backend preferred_compute, ral::io::data_handle handle, Schema & schema);

	DataType type() const override { return DataType::JSON; }

private:
	std::map<std::string, std::string> args_map;
};

} /* namespace io */
} /* namespace ral */
