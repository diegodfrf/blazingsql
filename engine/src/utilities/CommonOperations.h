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

namespace ral {
namespace cpu {
namespace utilities {
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
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_like_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  // TODO percy
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
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
std::unique_ptr<ral::frame::BlazingTable> create_empty_table_functor::operator()<ral::frame::BlazingArrowTable>(
    const std::vector<std::string> &column_names,
	  const std::vector<cudf::data_type> &dtypes) const
{
  // TODO percy
  return nullptr; //return ral::cpu::empty_like(arrow_table_view->view());
}

template <>
std::unique_ptr<ral::frame::BlazingTable> create_empty_table_functor::operator()<ral::frame::BlazingCudfTable>(
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
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline std::unique_ptr<ral::frame::BlazingTable> from_table_view_to_table_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  // TODO percy
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
    // TODO percy arrow thrown error
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
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  return nullptr; //return ral::cpu::empty_like(arrow_table_view->view());
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
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
inline bool checkIfConcatenatingStringsWillOverflow_functor::operator()<ral::frame::BlazingArrowTable>(
    const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) const
{
  // this check is only relevant to Cudf
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
    // TODO percy arrow thrown error
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
  
  std::cout << "------->>>>>>>>>>AROW CONCAT: \n" << "\t" << concatenated_tables->ToString() << "\n\n";
  std::cout << "FINDELGATO!!!!\n\n"; 
  
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
    // TODO percy arrow thrown error
    //return nullptr;
  }
};

template <>
inline std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
split_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<cudf::size_type> const& splits) const
{
  // TODO percy arrow
  //return std::make_pair(nullptr, {});
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












/////////////////////// normalize_types functor



struct normalize_types_functor {
  template <typename T>
  void operator()(
      std::unique_ptr<ral::frame::BlazingTable> & table,
      const std::vector<cudf::data_type> & types,
      std::vector<cudf::size_type> column_indices) const
  {
    // TODO percy arrow thrown error
    //return nullptr;
  }
};

template <>
inline void
normalize_types_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::unique_ptr<ral::frame::BlazingTable> & table,
    const std::vector<cudf::data_type> & types,
    std::vector<cudf::size_type> column_indices) const
{
  // TODO percy arrow
  //return std::make_pair(nullptr, {});
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

}  // namespace ral
