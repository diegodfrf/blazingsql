#include "operators/Concatenate.h"

#include "compute/backend_dispatcher.h"
#include "compute/backend.h"
#include "compute/api.h"
#include "utilities/error.hpp"

bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) {
  return ral::execution::backend_dispatcher(tables[0]->get_execution_backend(), checkIfConcatenatingStringsWillOverflow_functor(),
      tables);
}

bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & tables) {
	std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables_to_concat(tables.size());
	for (std::size_t i = 0; i < tables.size(); i++){
		tables_to_concat[i] = tables[i]->to_table_view();
	}

	return checkIfConcatenatingStringsWillOverflow(tables_to_concat);
}

// TODO percy arrow here we need to think about how we want to manage hybrid tables
// for now we will concat either cudf tables or arrow tables
// for now we are just validating that they are all the same type
std::unique_ptr<ral::frame::BlazingTable> concatTables(const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) {
	assert(tables.size() >= 0);

	std::vector<std::string> names;
	std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views_to_concat;
	ral::execution::execution_backend common_backend = tables.size() > 0 ? tables[0]->get_execution_backend() : ral::execution::execution_backend(ral::execution::backend_id::NONE);
	for(size_t i = 0; i < tables.size(); i++) {
		if (tables[i]->column_names().size() > 0){ // lets make sure we get the names from a table that is not empty
			names = tables[i]->column_names();
      		table_views_to_concat.push_back(tables[i]);

			RAL_EXPECTS(tables[i]->get_execution_backend() == common_backend, "Concatenating tables have different backends");
		}
	}
	// TODO want to integrate data type normalization.
	// Data type normalization means that only some columns from a table would get normalized,
	// so we would need to manage the lifecycle of only a new columns that get allocated

	size_t empty_count = 0;
  
  for(size_t i = 0; i < table_views_to_concat.size(); i++) {
    if (table_views_to_concat[i]->num_rows() == 0){
      ++empty_count;
    }
  }
  
  auto out = ral::execution::backend_dispatcher(common_backend, concat_functor(),
      table_views_to_concat, empty_count, names);
	  return out;
}
