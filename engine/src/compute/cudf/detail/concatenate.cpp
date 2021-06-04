#include "compute/cudf/detail/concatenate.h"

bool checkIfConcatenatingStringsWillOverflow_gpu(const std::vector<std::shared_ptr<ral::frame::BlazingCudfTableView>> & tables) {
	if( tables.size() == 0 ) {
		return false;
	}

	// Lets only look at tables that not empty
	std::vector<size_t> non_empty_index;
	for(size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
		if (tables[table_idx]->num_columns() > 0){
			non_empty_index.push_back(table_idx);
		}
	}
	if( non_empty_index.size() == 0 ) {
		return false;
	}

	for(size_t col_idx = 0; col_idx < tables[non_empty_index[0]]->column_types().size(); col_idx++) {
		if(tables[non_empty_index[0]]->column_types()[col_idx].id() == cudf::type_id::STRING) {
			std::size_t total_bytes_size = 0;
			std::size_t total_offset_count = 0;

			for(size_t i = 0; i < non_empty_index.size(); i++) {
				size_t table_idx = non_empty_index[i];

				// Column i-th from the next tables are expected to have the same string data type
				assert( tables[table_idx]->column_types()[col_idx].id() == cudf::type_id::STRING );

				auto & column = tables[table_idx]->column(col_idx);
				auto num_children = column.num_children();
				if(num_children == 2) {

					auto offsets_column = column.child(0);
					auto chars_column = column.child(1);

					// Similarly to cudf, we focus only on the byte number of chars and the offsets count
					total_bytes_size += chars_column.size();
					total_offset_count += offsets_column.size() + 1;

					if( total_bytes_size > static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max()) ||
						total_offset_count > static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max())) {
						return true;
					}
				}
			}
		}
	}

	return false;
}
