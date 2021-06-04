#include "compute/cudf/detail/join.h"

std::unique_ptr<cudf::table> reordering_columns_due_to_right_join(std::unique_ptr<cudf::table> table_ptr, size_t n_right_columns) {
	std::vector<std::unique_ptr<cudf::column>> columns_ptr = table_ptr->release();
	std::vector<std::unique_ptr<cudf::column>> columns_right_pos;

	// First let's put all the left columns
	for (size_t index = n_right_columns; index < columns_ptr.size(); ++index) {
		columns_right_pos.push_back(std::move(columns_ptr[index]));
	}

	// Now let's append right columns
	for (size_t index = 0; index < n_right_columns; ++index) {
		columns_right_pos.push_back(std::move(columns_ptr[index]));
	}

	return std::make_unique<cudf::table>(std::move(columns_right_pos));
} 
