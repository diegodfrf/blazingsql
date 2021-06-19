

#ifndef GDFPARSER_H_
#define GDFPARSER_H_

#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

#include "blazing_table/BlazingTable.h"

namespace ral {
namespace io {

class gdf_parser : public data_parser {
public:
	gdf_parser();

	virtual ~gdf_parser();

	size_t get_num_partitions();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(ral::execution::execution_backend preferred_compute,
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<int> row_groups);

	void parse_schema(ral::execution::execution_backend preferred_compute,ral::io::data_handle /*handle*/, ral::io::Schema & schema);

	DataType type() const override { return DataType::CUDF; }

private:

};

} /* namespace io */
} /* namespace ral */

#endif /* GDFPARSER_H_ */
