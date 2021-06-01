#pragma once

#include <string>
#include <vector>
#include "execution_kernels/LogicPrimitives.h"
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

namespace ral {
namespace cpu {
namespace utilities {
  std::shared_ptr<arrow::Schema> build_arrow_schema(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>> &columns,
    const std::vector<std::string> &column_names,
    std::shared_ptr<const arrow::KeyValueMetadata> metadata = NULLPTR);

	std::unique_ptr<ral::frame::BlazingTable> getLimitedRows(std::shared_ptr<arrow::Table> table, cudf::size_type num_rows, bool front=true);
  template<typename CPPType, typename ArrowScalarType>
  std::unique_ptr<cudf::scalar> to_cudf_numeric_scalar(cudf::data_type cudf_dtype, std::shared_ptr<arrow::Scalar> arrow_scalar);
  std::unique_ptr<cudf::scalar> to_cudf_scalar(std::shared_ptr<arrow::Scalar> arrow_scalar);
  
  
  void normalize_types(std::unique_ptr<ral::frame::BlazingArrowTable> & table,  const std::vector<cudf::data_type> & types,
                       std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );

}  // namespace utilities
}  // namespace cpu

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
















}  // namespace utilities



























///////////////// create_empty_table_like_functor / empty_like functor
struct create_empty_table_like_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view) const
  {
    throw std::runtime_error("ERROR: create_empty_table_like_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_like_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  // TODO percy
  throw std::runtime_error("ERROR: create_empty_table_like_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr; //return ral::cpu::empty_like(arrow_table_view->view());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_like_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  std::unique_ptr<cudf::table> empty = cudf::empty_like(cudf_table_view->view());
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(empty), cudf_table_view->column_names());
}


struct create_empty_table_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      const std::vector<std::string> &column_names,
	    const std::vector<cudf::data_type> &dtypes) const
  {
    throw std::runtime_error("ERROR: create_empty_table_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_functor::operator()<ral::frame::BlazingArrowTable>(
    const std::vector<std::string> &column_names,
	  const std::vector<cudf::data_type> &dtypes) const
{
  // TODO percy
  throw std::runtime_error("ERROR: create_empty_table_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr; //return ral::cpu::empty_like(arrow_table_view->view());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_functor::operator()<ral::frame::BlazingCudfTable>(
    const std::vector<std::string> &column_names,
	  const std::vector<cudf::data_type> &dtypes) const
{
  return ral::utilities::create_empty_cudf_table(column_names, dtypes);
}


//////////// from_table_view_to_table functor

struct from_table_view_to_table_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view) const
  {
    throw std::runtime_error("ERROR: from_table_view_to_table_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> from_table_view_to_table_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  // TODO percy
  throw std::runtime_error("ERROR: from_table_view_to_table_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr; //return ral::cpu::empty_like(arrow_table_view->view());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> from_table_view_to_table_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf_table_view->view(), cudf_table_view->column_names());
}












///////////////////////////// sample functor







struct sample_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      cudf::size_type const num_samples,
      std::vector<std::string> sortColNames,
      std::vector<int> sortColIndices) const
  {
    throw std::runtime_error("ERROR: sample_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sample_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    cudf::size_type const num_samples,
    std::vector<std::string> sortColNames,
    std::vector<int> sortColIndices) const
{
  std::random_device rd;
  auto arrow_table = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view)->view();
  std::vector<int> population(arrow_table->num_rows());
  std::iota(population.begin(), population.end(), 0);
  std::vector<int> samples_indexes_raw;
  std::sample(population.begin(),
              population.end(),
              std::back_inserter(samples_indexes_raw),
              num_samples,
              rd);
  auto int_builder = std::make_unique<arrow::Int32Builder>();
  int_builder->AppendValues(samples_indexes_raw);
  std::shared_ptr<arrow::Array> samples_indexes;
  int_builder->Finish(&samples_indexes);
  auto input = arrow_table->SelectColumns(sortColIndices).ValueOrDie();
  auto samples = arrow::compute::Take(*input, *samples_indexes).ValueOrDie();
  return std::make_unique<ral::frame::BlazingArrowTable>(samples);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sample_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    cudf::size_type const num_samples,
    std::vector<std::string> sortColNames,
    std::vector<int> sortColIndices) const
{
  std::random_device rd;
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  auto samples = cudf::sample(cudf_table_view->view().select(sortColIndices), num_samples, cudf::sample_with_replacement::FALSE, rd());
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(samples), sortColNames);
}















///////////////////////////// checkIfConcatenatingStringsWillOverflow functor

struct checkIfConcatenatingStringsWillOverflow_functor {
  template <typename T>
  bool operator()(const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) const
  {
    throw std::runtime_error("ERROR: checkIfConcatenatingStringsWillOverflow_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
inline bool checkIfConcatenatingStringsWillOverflow_functor::operator()<ral::frame::BlazingArrowTable>(
    const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) const
{
  return false;
  // TODO percy arrow implement size
  // this check is only relevant to Cudf
  throw std::runtime_error("ERROR: checkIfConcatenatingStringsWillOverflow_functor BlazingSQL doesn't support this Arrow operator yet.");
  return false;
}

template <>
inline bool checkIfConcatenatingStringsWillOverflow_functor::operator()<ral::frame::BlazingCudfTable>(
    const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) const
{
  std::vector<std::shared_ptr<ral::frame::BlazingCudfTableView>> in;
  for (auto tv : tables) {
    in.push_back(std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tv));
  }
  return ral::utilities::checkIfConcatenatingStringsWillOverflow_gpu(in);
}



















////////////////// concat functor




struct concat_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views,
      size_t empty_count,
      std::vector<std::string> names) const
  {
    throw std::runtime_error("ERROR: concat_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> concat_functor::operator()<ral::frame::BlazingArrowTable>(
    std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views,
    size_t empty_count,
    std::vector<std::string> names) const
{
  std::vector<std::shared_ptr<arrow::Table>> arrow_tables_to_concat;
  for (auto tv : table_views) {
    arrow_tables_to_concat.push_back(std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(tv)->view());
  }
  
  if (empty_count == arrow_tables_to_concat.size()) {
    return std::make_unique<ral::frame::BlazingArrowTable>(arrow_tables_to_concat[0]);
  }

  std::shared_ptr<arrow::Table> concatenated_tables = arrow::ConcatenateTables(arrow_tables_to_concat).ValueOrDie();
  return std::make_unique<ral::frame::BlazingArrowTable>(concatenated_tables);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> concat_functor::operator()<ral::frame::BlazingCudfTable>(
    std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views,
    size_t empty_count,
    std::vector<std::string> names) const
{
  std::vector<cudf::table_view> table_views_to_concat;
  for (auto tv : table_views) {
    table_views_to_concat.push_back(std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tv)->view());
  }
  if (empty_count == table_views_to_concat.size()) {
    return std::make_unique<ral::frame::BlazingCudfTable>(table_views_to_concat[0], names);
  }
  std::unique_ptr<cudf::table> concatenated_tables = cudf::concatenate(table_views_to_concat);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(concatenated_tables), names);
}

















/////////////////////////// split functor

struct split_functor {
  template <typename T>
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_View,
      std::vector<cudf::size_type> const& splits) const
  {
    throw std::runtime_error("ERROR: split_functor This default dispatcher operator should not be called.");
  }
};





std::pair<std::shared_ptr<arrow::Table>, std::vector<cudf::size_type>>
split_arrow(std::shared_ptr<arrow::Table> table_View,
            std::vector<cudf::size_type> const& columns_to_hash,
            int num_partitions);

template <>
inline std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
split_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<cudf::size_type> const& splits) const
{
  /*
  * input:   [{10, 12, 14, 16, 18, 20, 22, 24, 26, 28},
  *           {50, 52, 54, 56, 58, 60, 62, 64, 66, 68}]
  * splits:  {2, 5, 9}
  * output:  [{{10, 12}, {14, 16, 18}, {20, 22, 24, 26}, {28}},
  *           {{50, 52}, {54, 56, 58}, {60, 62, 64, 66}, {68}}]
  */

//  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> result{};
//  if (table_View.num_columns() == 0) { return result; }
  
  throw std::runtime_error("ERROR: split_functor BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
split_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<cudf::size_type> const& splits) const
{
  auto cudf_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_View);
  std::vector<std::string> names;
  auto tbs = cudf::split(cudf_view->view(), splits);
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> ret;
  for (auto tb : tbs) {
    ret.push_back(std::make_shared<ral::frame::BlazingCudfTableView>(tb, cudf_view->column_names()));
  }
  return ret;
}

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













/////////////////////// normalize_types functor



struct normalize_types_functor {
  template <typename T>
  void operator()(
      std::unique_ptr<ral::frame::BlazingTable> & table,
      const std::vector<cudf::data_type> & types,
      std::vector<cudf::size_type> column_indices) const
  {
    throw std::runtime_error("ERROR: normalize_types_functor This default dispatcher operator should not be called.");
  }
};

template <>
inline void
normalize_types_functor::operator()<ral::frame::BlazingArrowTable>(
    std::unique_ptr<ral::frame::BlazingTable> & table,
    const std::vector<cudf::data_type> & types,
    std::vector<cudf::size_type> column_indices) const
{
  throw std::runtime_error("ERROR: normalize_types_functor BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline void
normalize_types_functor::operator()<ral::frame::BlazingCudfTable>(
    std::unique_ptr<ral::frame::BlazingTable> & table,
    const std::vector<cudf::data_type> & types,
    std::vector<cudf::size_type> column_indices) const
{
  ral::utilities::normalize_types_gpu(table, types, column_indices);
}













///////////////// hash_partition functor



struct hash_partition_functor {
  template <typename T>
  inline std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::vector<cudf::size_type>> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_View,
      std::vector<cudf::size_type> const& columns_to_hash,
      int num_partitions) const
  {
    throw std::runtime_error("ERROR: hash_partition_functor This default dispatcher operator should not be called.");
  }
};

template <>
inline std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::vector<cudf::size_type>>
hash_partition_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<cudf::size_type> const& columns_to_hash,
    int num_partitions) const
{
  throw std::runtime_error("ERROR: hash_partition_functor BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::vector<cudf::size_type>>
hash_partition_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<cudf::size_type> const& columns_to_hash,
    int num_partitions) const
{
  auto batch_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_View);
  auto tb = cudf::hash_partition(batch_view->view(), columns_to_hash, num_partitions);
  return std::make_pair(std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb.first), table_View->column_names()), tb.second);
}

}  // namespace ral
