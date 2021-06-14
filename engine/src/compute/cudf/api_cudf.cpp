#pragma once

#include "compute/api.h"


#include <random>
#include <cudf/aggregation.hpp>
#include <cudf/filling.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/detail/gather.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/merge.hpp>
#include <cudf/reduction.hpp>
#include <cudf/replace.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/sorting.hpp>
#include <cudf/search.hpp>
#include <cudf/replace.hpp>
#include <cudf/join.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/reduction.hpp>
#include <cudf/detail/interop.hpp>

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "parser/types_parser_utils.h"

#include "compute/cudf/detail/aggregations.h"

#include "compute/cudf/detail/interops.h"
#include "compute/cudf/detail/join.h"
#include "compute/cudf/detail/filter.h"
#include "compute/cudf/detail/nulls.h"
#include "compute/cudf/detail/search.h"
#include "compute/cudf/detail/concatenate.h"
#include "compute/cudf/detail/types.h"
#include "compute/cudf/detail/io.h"
#include "compute/cudf/detail/scalars.h"

#include "utilities/error.hpp"
#include "blazing_table/BlazingCudfTable.h"

//namespace voltron {
//namespace compute {

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_merger_functor::operator()<ral::frame::BlazingCudfTable>(
		std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables,
		const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
		const std::vector<int> & sortColIndices, const std::vector<voltron::compute::NullOrder> & sortOrderNulls) const
{
	return sorted_merger(tables, sortOrderTypes, sortColIndices, sortOrderNulls);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> gather_functor::operator()<ral::frame::BlazingCudfTable>(
		std::shared_ptr<ral::frame::BlazingTableView> table,
		std::unique_ptr<cudf::column> column,
		cudf::out_of_bounds_policy out_of_bounds_policy,
		cudf::detail::negative_index_policy negative_index_policy) const
{
	// TODO percy rommel arrow
	ral::frame::BlazingCudfTableView *table_ptr = dynamic_cast<ral::frame::BlazingCudfTableView*>(table.get());
	std::unique_ptr<cudf::table> pivots = cudf::detail::gather(table_ptr->view(), column->view(), out_of_bounds_policy, negative_index_policy);

	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(pivots), table->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<int> group_column_indices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return compute_groupby_without_aggregations(cudf_table_view, group_column_indices);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> aggregations_without_groupby_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<voltron::compute::AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return compute_aggregations_without_groupby(cudf_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<voltron::compute::AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases,
    std::vector<int> group_column_indices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return compute_aggregations_with_groupby(cudf_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases, group_column_indices);
}

template<typename T>
inline std::vector<T> merge_vectors(std::vector<T> first, std::vector<T> second){
	std::vector<T> merged(first);
	merged.insert(merged.end(), second.begin(), second.end());
	return merged;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> cross_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::cross_join(table_left->view(), table_right->view()), names);
}

template <>
inline bool check_if_has_nulls_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, std::vector<cudf::size_type> const& keys) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return check_if_has_nulls(cudf_table_view->view(), keys);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> inner_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices,
    cudf::null_equality equalityType) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  auto tb = cudf::inner_join(
              table_left->view(),
              table_right->view(),
              left_column_indices,
              right_column_indices,
              equalityType);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb), names);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> drop_nulls_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<cudf::size_type> const& keys) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::drop_nulls(cudf_table_view->view(), keys), table_view->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> left_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  auto tb = cudf::left_join(
              table_left->view(),
              table_right->view(),
              left_column_indices,
              right_column_indices);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb), names);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> full_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    bool has_nulls_left,
    bool has_nulls_right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  auto tb = cudf::full_join(
              table_left->view(),
              table_right->view(),
              left_column_indices,
              right_column_indices,
              (has_nulls_left && has_nulls_right) ? cudf::null_equality::UNEQUAL : cudf::null_equality::EQUAL);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb), names);
}

template<>
inline std::unique_ptr<ral::frame::BlazingTable> reordering_columns_due_to_right_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::unique_ptr<ral::frame::BlazingTable> table_ptr, size_t right_columns) const
{
  ral::frame::BlazingCudfTable* result_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(table_ptr.get());
  return std::make_unique<ral::frame::BlazingCudfTable>(reordering_columns_due_to_right_join(result_table_ptr->releaseCudfTable(), right_columns), 
                                                        table_ptr->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_wo_filter_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table, const std::vector<std::string> & expressions,
  const std::vector<std::string> column_names) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table);
    return std::make_unique<ral::frame::BlazingCudfTable>(evaluate_expressions(cudf_table_view->view(), expressions), column_names);
}

//template <>
//std::unique_ptr<ral::frame::BlazingTable> process_project_functor::operator()<ral::frame::BlazingCudfTable>(
//  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
//  const std::vector<std::string> & expressions,
//  const std::vector<std::string> & out_column_names) const
//{
//  auto blazing_table_in_cudf = dynamic_cast<ral::frame::BlazingCudfTable*>(blazing_table_in.get());
//  std::unique_ptr<ral::frame::BlazingTable> evaluated_table = ral::execution::backend_dispatcher(
//    blazing_table_in->get_execution_backend(),
//    evaluate_expressions_wo_filter_functor(),
//    blazing_table_in_cudf->view(), expressions, out_column_names);
  
//  //auto evaluated_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(evaluated_table.get());
//  //return std::make_unique<ral::frame::BlazingCudfTable>(evaluated_table_ptr->view(), out_column_names);
//  return evaluated_table;
//}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto *table_view_ptr = dynamic_cast<ral::frame::BlazingCudfTableView*>(table_view.get());
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::empty_like(table_view_ptr->view()), table_view_ptr->column_names());
}


template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
    auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(cudf_table_view->view(), expressions);
    RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");
    return applyBooleanFilter(cudf_table_view, evaluated_table[0]->view());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_order_gather_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
    const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
    std::vector<voltron::compute::NullOrder> null_orders) const
{
  auto table = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  auto sortColumns = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(sortColumns_view);
  std::vector<cudf::order> cudfOrderTypes = toCudfOrderTypes(sortOrderTypes);
  std::vector<cudf::null_order> cudfNullOrderTypes = toCudfNullOrderTypes(null_orders);
  std::unique_ptr<cudf::column> output = cudf::sorted_order( sortColumns->view(), cudfOrderTypes, cudfNullOrderTypes );
	std::unique_ptr<cudf::table> gathered = cudf::gather( table->view(), output->view() );
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(gathered), table->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_like_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  std::unique_ptr<cudf::table> empty = cudf::empty_like(cudf_table_view->view());
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(empty), cudf_table_view->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_functor::operator()<ral::frame::BlazingCudfTable>(
    const std::vector<std::string> &column_names,
	  const std::vector<std::shared_ptr<arrow::DataType>> &dtypes) const
{
  return create_empty_cudf_table(column_names, dtypes);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> from_table_view_to_table_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf_table_view->view(), cudf_table_view->column_names());
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

template <>
inline bool checkIfConcatenatingStringsWillOverflow_functor::operator()<ral::frame::BlazingCudfTable>(
    const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) const
{
  std::vector<std::shared_ptr<ral::frame::BlazingCudfTableView>> in;
  for (auto tv : tables) {
    in.push_back(std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tv));
  }
  return checkIfConcatenatingStringsWillOverflow_gpu(in);
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

template <>
inline void
normalize_types_functor::operator()<ral::frame::BlazingCudfTable>(
    std::unique_ptr<ral::frame::BlazingTable> & table,
    const std::vector<std::shared_ptr<arrow::DataType>>  & types,
    std::vector<cudf::size_type> column_indices) const
{
  normalize_types_gpu(table, types, column_indices);
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


template <>
inline std::shared_ptr<ral::frame::BlazingTableView> select_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    const std::vector<int> & sortColIndices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return std::make_shared<ral::frame::BlazingCudfTableView>(cudf_table_view->view().select(sortColIndices),
                                                            cudf_table_view->column_names());
}

template <>
inline std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
upper_bound_split_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> sortedTable_view,
    std::shared_ptr<ral::frame::BlazingTableView> t,
    std::shared_ptr<ral::frame::BlazingTableView> values,
    std::vector<voltron::compute::SortOrder> const& column_order,
    std::vector<voltron::compute::NullOrder> const& null_precedence) const
{
  auto sortedTable = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(sortedTable_view);
  auto columns_to_search = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(t);  
  auto partitionPlan = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(values);  

  std::vector<cudf::order> cudf_column_order = toCudfOrderTypes(column_order);
  std::vector<cudf::null_order> cudfNullOrderTypes = toCudfNullOrderTypes(null_precedence);
  auto pivot_indexes = cudf::upper_bound(columns_to_search->view(), partitionPlan->view(), cudf_column_order, cudfNullOrderTypes);
	std::vector<cudf::size_type> split_indexes = column_to_vector<cudf::size_type>(pivot_indexes->view());
  auto tbs = cudf::split(sortedTable->view(), split_indexes);
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> ret;
  for (auto tb : tbs) {
    ret.push_back(std::make_shared<ral::frame::BlazingCudfTableView>(tb, sortedTable_view->column_names()));
  }
  return ret;
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::PARQUET>::operator()<ral::frame::BlazingCudfTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<cudf::size_type> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::cudf_backend::io::read_parquet_file(file, column_indices, col_names, row_groups);
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::CSV>::operator()<ral::frame::BlazingCudfTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<cudf::size_type> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::cudf_backend::io::read_csv_file(file, column_indices, col_names, row_groups, args_map);
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::ORC>::operator()<ral::frame::BlazingCudfTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<cudf::size_type> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::cudf_backend::io::read_orc_file(file, column_indices, col_names, row_groups, args_map);
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::JSON>::operator()<ral::frame::BlazingCudfTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<cudf::size_type> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::cudf_backend::io::read_json_file(file, column_indices, col_names, row_groups, args_map);
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::PARQUET>::operator()<ral::frame::BlazingCudfTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::cudf_backend::io::parse_parquet_schema(schema_out, file);
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::CSV>::operator()<ral::frame::BlazingCudfTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::cudf_backend::io::parse_csv_schema(schema_out, file, args_map);
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::ORC>::operator()<ral::frame::BlazingCudfTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::cudf_backend::io::parse_orc_schema(schema_out, file, args_map);
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::JSON>::operator()<ral::frame::BlazingCudfTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::cudf_backend::io::parse_json_schema(schema_out, file, args_map);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable>
decache_io_functor::operator()<ral::frame::BlazingCudfTable>(
    std::unique_ptr<ral::frame::BlazingTable> table,
    std::vector<int> projections,
    ral::io::Schema schema,
    std::vector<int> column_indices_in_file,
    std::map<std::string, std::string> column_values) const
{
      std::vector<std::unique_ptr<cudf::column>> all_columns(projections.size());
      std::vector<std::unique_ptr<cudf::column>> file_columns;
      std::vector<std::string> names;
      cudf::size_type num_rows;

      if (column_indices_in_file.size() > 0){
        names = table->column_names();
          ral::frame::BlazingCudfTable* table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(table.get());
          std::unique_ptr<cudf::table> current_table = table_ptr->releaseCudfTable();
          num_rows = current_table->num_rows();
          file_columns = current_table->release();
      }

      int in_file_column_counter = 0;
      for(std::size_t i = 0; i < projections.size(); i++) {
        int col_ind = projections[i];
        if(!schema.get_in_file()[col_ind]) {
          std::string name = schema.get_name(col_ind);
          arrow::Type::type type = schema.get_dtype(col_ind);
          names.push_back(name);
          std::string literal_str = column_values[name];
            std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string(literal_str, arrow_type_to_cudf_data_type(type), false);
            all_columns[i] = cudf::make_column_from_scalar(*scalar, num_rows);
        } else {
            all_columns[i] = std::move(file_columns[in_file_column_counter]);
          in_file_column_counter++;
        }
      }

      auto unique_table = std::make_unique<cudf::table>(std::move(all_columns));
      return std::make_unique<ral::frame::BlazingCudfTable>(std::move(unique_table), names);
}

//} // compute
//} // voltron
