#pragma once

#include <execution_graph/Context.h>
#include "execution_kernels/LogicPrimitives.h"
#include <arrow/compute/api.h>

namespace ral{

namespace cpu {

inline bool check_if_has_nulls(std::shared_ptr<arrow::Table> input, std::vector<cudf::size_type> const& keys){
  for (auto col : keys) {
    if (input->num_columns() != 0 && input->num_rows() != 0 && input->column(col)->null_count() == 0) {
        return true;
    }
  }  
  return false;
}


} // namespace cpu

namespace operators{


std::unique_ptr<ral::frame::BlazingTable> process_filter(
  std::shared_ptr<ral::frame::BlazingTableView> table_view,
  const std::string & query_part,
  blazingdb::manager::Context * context);


/**
 * This function is only used by bc.partition
 */
std::unique_ptr<ral::frame::BlazingTable> process_distribution_table(
  	const ral::frame::BlazingTableView & table,
    std::vector<int> & columnIndices,
    blazingdb::manager::Context * context);

} // namespace operators
} // namespace ral
