#pragma once

#include <string>
#include <vector>
#include "blazing_table/BlazingTable.h"
#include "blazing_table/BlazingCudfTableView.h"
#include "cudf/column/column_factories.hpp"
#include <random>
#include <arrow/scalar.h>
#include <cudf/scalar/scalar.hpp>
#include <cudf/copying.hpp>
#include <arrow/compute/api.h>
#include <cudf/copying.hpp>
#include <cudf/unary.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/filling.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/partitioning.hpp>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_base.h>
#include <arrow/table.h>



std::shared_ptr<arrow::Schema> build_arrow_schema(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>> &columns,
    const std::vector<std::string> &column_names,
    std::shared_ptr<const arrow::KeyValueMetadata> metadata = NULLPTR);

namespace ral {
namespace utilities {

using namespace ral::frame;

bool checkIfConcatenatingStringsWillOverflow_gpu(const std::vector<std::shared_ptr<BlazingCudfTableView>> & tables);

bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::shared_ptr<BlazingTableView>> & tables);
bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::unique_ptr<BlazingTable>> & tables);

std::unique_ptr<BlazingTable> concatTables(const std::vector<std::shared_ptr<BlazingTableView>> & tables);

std::unique_ptr<BlazingTable> getLimitedRows(std::shared_ptr<BlazingTableView> table_view, cudf::size_type num_rows, bool front=true);

std::unique_ptr<ral::frame::BlazingCudfTable> create_empty_cudf_table(const std::vector<std::string> &column_names,
	const std::vector<cudf::data_type> &dtypes, std::vector<size_t> column_indices = std::vector<size_t>());

std::unique_ptr<cudf::table> create_empty_cudf_table(const std::vector<cudf::type_id> &dtypes);

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(std::shared_ptr<ral::frame::BlazingTableView> table);

cudf::data_type get_common_type(cudf::data_type type1, cudf::data_type type2, bool strict);

std::vector<cudf::data_type> get_common_types(const std::vector<cudf::data_type> & types1, const std::vector<cudf::data_type> & types2, bool strict);

void normalize_types_gpu(std::unique_ptr<ral::frame::BlazingTable> & gpu_table,  const std::vector<cudf::data_type> & types,
	 		std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );

void normalize_types(std::unique_ptr<ral::frame::BlazingTable> & table,  const std::vector<cudf::data_type> & types,
	 		std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );

// This is only for numerics
template<typename T>
std::unique_ptr<cudf::column> vector_to_column(std::vector<T> vect, cudf::data_type type){
	return std::make_unique<cudf::column>(type, vect.size(), rmm::device_buffer{vect.data(), vect.size() * sizeof(T)});
}

// This is only for numerics
template<typename T>
std::vector<T> column_to_vector(cudf::column_view column){
	std::vector<T> host_data(column.size());
  	CUDA_TRY(cudaMemcpy(host_data.data(), column.data<T>(), column.size() * sizeof(T), cudaMemcpyDeviceToHost));
	return host_data;
}

std::pair<std::shared_ptr<arrow::Table>, std::vector<cudf::size_type>>
split_arrow(std::shared_ptr<arrow::Table> table_View,
            std::vector<cudf::size_type> const& columns_to_hash,
            int num_partitions);



/*
std::vector<std::shared_ptr<ral::frame::BlazingTableView>> slice(
    std::shared_ptr<ral::frame::BlazingTableView> input,
    std::vector<cudf::size_type> const& indices)
{
  //CUDF_FUNC_RANGE();
  //CUDF_EXPECTS(indices.size() % 2 == 0, "indices size must be even");
  if (indices.empty()) { return {}; }

  // 2d arrangement of column_views that represent the outgoing table_views sliced_table[i][j]
  // where i is the i'th column of the j'th table_view
  auto op = [&indices](std::shared_ptr<arrow::ChunkedArray> c) {
    //return cudf::slice(c, indices);
    c->Slice()
  };
  auto f  = thrust::make_transform_iterator(input.begin(), op);

  auto sliced_table = std::vector<std::vector<cudf::column_view>>(f, f + input.num_columns());
  sliced_table.reserve(indices.size() + 1);

  std::vector<cudf::table_view> result{};
  // distribute columns into outgoing table_views
  size_t num_output_tables = indices.size() / 2;
  for (size_t i = 0; i < num_output_tables; i++) {
    std::vector<cudf::column_view> table_columns;
    for (size_type j = 0; j < input.num_columns(); j++) {
      table_columns.emplace_back(sliced_table[j][i]);
    }
    result.emplace_back(table_view{table_columns});
  }

  return result;
};

std::vector<std::shared_ptr<ral::frame::BlazingTableView>> split(
    std::shared_ptr<ral::frame::BlazingTableView> input,
    cudf::size_type column_size,
    std::vector<cudf::size_type> const& splits)
{
  if (splits.empty() or column_size == 0) { 
    std::vector<std::shared_ptr<ral::frame::BlazingTableView>> ret;
    ret.push_back(input);
    return ret;
  }
  //CUDF_EXPECTS(splits.back() <= column_size, "splits can't exceed size of input columns");

  // If the size is not zero, the split will always start at `0`
  std::vector<cudf::size_type> indices{0};
  std::for_each(splits.begin(), splits.end(), [&indices](auto split) {
    indices.push_back(split);  // This for end
    indices.push_back(split);  // This for the start
  });

  indices.push_back(column_size);  // This to include rest of the elements

  return cudf::slice(input, indices);
}
*/

}  // namespace utilities
}  // namespace ral
