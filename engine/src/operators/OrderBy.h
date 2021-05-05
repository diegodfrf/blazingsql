#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <execution_graph/Context.h>
#include <string>
#include <vector>
#include <tuple>
#include "execution_kernels/LogicPrimitives.h"
#include "utilities/CommonOperations.h"
#include <cudf/copying.hpp>
#include <cudf/sorting.hpp>
#include <cudf/search.hpp>

namespace ral {

namespace cpu {
namespace operators {
  std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t> limit_table(std::shared_ptr<arrow::Table> table, int64_t num_rows_limit);
}  // namespace operators
}  // namespace cpu

namespace operators {

namespace {
  using blazingdb::manager::Context;
}


////////////////////////////// select / take functor


struct select_functor {
  template <typename T>
  std::shared_ptr<ral::frame::BlazingTableView> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      const std::vector<int> & sortColIndices) const
  {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
std::shared_ptr<ral::frame::BlazingTableView> select_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    const std::vector<int> & sortColIndices) const
{
  // TODO percy arrow
  return nullptr;
}

template <>
std::shared_ptr<ral::frame::BlazingTableView> select_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    const std::vector<int> & sortColIndices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);  
  return std::make_shared<ral::frame::BlazingCudfTableView>(cudf_table_view->view().select(sortColIndices), 
                                                            cudf_table_view->column_names());
}













/////////////////////////// upper_bound functor


struct upper_bound_split_functor {
  template <typename T>
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> sortedTable_view,
      std::shared_ptr<ral::frame::BlazingTableView> t,
      std::shared_ptr<ral::frame::BlazingTableView> values,
      std::vector<cudf::order> const& column_order,
      std::vector<cudf::null_order> const& null_precedence) const
  {
    // TODO percy arrow thrown error
    //return nullptr;
  }
};

template <>
std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
upper_bound_split_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> sortedTable_view,
    std::shared_ptr<ral::frame::BlazingTableView> t,
    std::shared_ptr<ral::frame::BlazingTableView> values,
    std::vector<cudf::order> const& column_order,
    std::vector<cudf::null_order> const& null_precedence) const
{
  // TODO percy arrow
  //return nullptr;
}

template <>
std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
upper_bound_split_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> sortedTable_view,
    std::shared_ptr<ral::frame::BlazingTableView> t,
    std::shared_ptr<ral::frame::BlazingTableView> values,
    std::vector<cudf::order> const& column_order,
    std::vector<cudf::null_order> const& null_precedence) const
{
  auto sortedTable = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(sortedTable_view);
  auto columns_to_search = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(t);  
  auto partitionPlan = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(values);  

  auto pivot_indexes = cudf::upper_bound(columns_to_search->view(), partitionPlan->view(), column_order, null_precedence);
	std::vector<cudf::size_type> split_indexes = ral::utilities::column_to_vector<cudf::size_type>(pivot_indexes->view());
  auto tbs = cudf::split(sortedTable->view(), split_indexes);
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> ret;
  for (auto tb : tbs) {
    ret.push_back(std::make_shared<ral::frame::BlazingCudfTableView>(tb, sortedTable_view->column_names()));
  }
  return ret;
}













////////////////////////////// sorted_order_grather functor



struct sorted_order_gather_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
      const std::vector<cudf::order> & sortOrderTypes,
      std::vector<cudf::null_order> null_orders) const
  {
    // TODO percy arrow thrown error
    return nullptr;
  }
};

template <>
std::unique_ptr<ral::frame::BlazingTable> sorted_order_gather_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
    const std::vector<cudf::order> & sortOrderTypes,
    std::vector<cudf::null_order> null_orders) const
{
  // TODO percy arrow
  return nullptr;
}

template <>
std::unique_ptr<ral::frame::BlazingTable> sorted_order_gather_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
    const std::vector<cudf::order> & sortOrderTypes,
    std::vector<cudf::null_order> null_orders) const
{
  auto table = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  auto sortColumns = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(sortColumns_view);
  std::unique_ptr<cudf::column> output = cudf::sorted_order( sortColumns->view(), sortOrderTypes, null_orders );
	std::unique_ptr<cudf::table> gathered = cudf::gather( table->view(), output->view() );
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(gathered), table->column_names());
}




std::unique_ptr<ral::frame::BlazingTable> logicalSort(std::shared_ptr<ral::frame::BlazingTableView> table_view,
      const std::vector<int> & sortColIndices,	const std::vector<cudf::order> & sortOrderTypes);

std::tuple<std::vector<int>, std::vector<cudf::order>, cudf::size_type> get_sort_vars(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<cudf::order> > get_vars_to_partition(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<cudf::order> > get_vars_to_orders(const std::string & query_part);

std::tuple< std::vector<int>, std::vector<cudf::order> > get_vars_to_partition_and_order(const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> sort(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part);

std::size_t compute_total_samples(std::size_t num_rows);

std::unique_ptr<ral::frame::BlazingTable> sample(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string & query_part);

std::unique_ptr<ral::frame::BlazingTable> generate_partition_plan(const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & samples,
    std::size_t table_num_rows, std::size_t avg_bytes_per_row, const std::string & query_part, Context * context);

std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partition_table(std::shared_ptr<ral::frame::BlazingTableView> partitionPlan,
	std::shared_ptr<ral::frame::BlazingTableView> sortedTable, const std::vector<cudf::order> & sortOrderTypes,	const std::vector<int> & sortColIndices);

bool has_limit_only(const std::string & query_part);

int64_t get_limit_rows_when_relational_alg_is_simple(const std::string & query_part);

std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t> limit_table(std::shared_ptr<ral::frame::BlazingTableView> table_view, int64_t num_rows_limit);

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitions_to_merge, const std::string & query_part);

}  // namespace operators
}  // namespace ral
