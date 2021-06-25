#pragma once

#include "compute/api.h"

#include <random>
#include <arrow/builder.h>
#include <arrow/compute/api.h>

#include "compute/backend_dispatcher.h"
#include "compute/arrow/detail/aggregations.h"
#include "compute/arrow/detail/io.h"
#include "compute/arrow/detail/scalars.h"
#include "compute/arrow/detail/types.h"
#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "cache_machine/common/ArrowCacheData.h"

// TODO percy arrow 4 move these functions into hosttbale ctor
#include "communication/messages/GPUComponentMessage.h"

#ifdef CUDF_SUPPORT
#include <thrust/binary_search.h>
#endif

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

inline std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  std::shared_ptr<arrow::Table> table,
  std::shared_ptr<arrow::ChunkedArray> boolValues){
  //auto filteredTable = cudf::apply_boolean_mask(
  //  table.view(),boolValues);
  //return std::make_unique<ral::frame::BlazingTable>(std::move(
  //  filteredTable),table.column_names());
  // TODO percy arrow
  return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

inline std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluate_expressions(
    std::shared_ptr<arrow::Table> table,
    const std::vector<std::string> & expressions) {
  // TODO percy arrow
  return table->columns();
}

inline std::unique_ptr<ral::frame::BlazingTable> getLimitedRows(std::shared_ptr<arrow::Table> table, int num_rows, bool front=true){
	if (num_rows == 0) {
		return std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns(), 0));
	} else if (num_rows < table->num_rows()) {
		std::shared_ptr<arrow::Table> arrow_table;
		if (front){
			arrow_table = table->Slice(0, num_rows);
		} else { // back
			arrow_table = table->Slice(table->num_rows() - num_rows);
		}
		return std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(arrow_table->schema(), arrow_table->columns()));
	} else {
		return std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns()));
	}
}

std::tuple<std::unique_ptr<ral::frame::BlazingTable>, bool, int64_t>
inline limit_table(std::shared_ptr<arrow::Table> table, int64_t num_rows_limit) {
	int table_rows = table->num_rows();
	if (num_rows_limit <= 0) {
		return std::make_tuple(std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns(), 0)), false, 0);
	} else if (num_rows_limit >= table_rows) {
		return std::make_tuple(std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns())), true, num_rows_limit - table_rows);
	} else {
		return std::make_tuple(getLimitedRows(table, num_rows_limit), false, 0);
	}
}

// TODO percy arrow
inline std::pair<std::shared_ptr<arrow::Table>, std::vector<int>>
split_arrow(std::shared_ptr<arrow::Table> table_View,
            std::vector<int> const& columns_to_hash,
            int num_partitions)
{
  auto splits = columns_to_hash;
  /*
  * input:   [{10, 12, 14, 16, 18, 20, 22, 24, 26, 28},
  *           {50, 52, 54, 56, 58, 60, 62, 64, 66, 68}]
  * splits:  {2, 5, 9}
  * output:  [{{10, 12}, {14, 16, 18}, {20, 22, 24, 26}, {28}},
  *           {{50, 52}, {54, 56, 58}, {60, 62, 64, 66}, {68}}]
  */
  std::vector<std::shared_ptr<arrow::Table>> tables;
  for (auto s : splits) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> cols;
    for (auto c : table_View->columns()) {
      cols.push_back(c->Slice(s));
    }
    tables.push_back(arrow::Table::Make(table_View->schema(), cols));
  }

  //table_View->Slice()
  //std::shared_ptr<Table> Slice(int64_t offset) const { return Slice(offset, num_rows_); }  
}

















//namespace voltron {
//namespace compute {


template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_wo_filter_functor::operator()<ral::frame::BlazingArrowTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table, const std::vector<std::string> & expressions,
  const std::vector<std::string> column_names) const
{
  //ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table_view.get());
  //std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = ral::cpu::evaluate_expressions(table_view_ptr->view(), expressions);

  //throw std::runtime_error("ERROR: evaluate_expressions_wo_filter_functor BlazingSQL doesn't support this Arrow operator yet.");
  //return nullptr;
  // TODO percy arrow
  ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table.get());
  std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = evaluate_expressions(table_view_ptr->view(), expressions);

  // TODO percy arrow
  //RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

  //return ral::cpu::applyBooleanFilter(table_view_ptr->view(), evaluated_table[0]);
  // TODO percy arrow
  
  return std::make_unique<ral::frame::BlazingArrowTable>(table_view_ptr->view());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_merger_functor::operator()<ral::frame::BlazingArrowTable>(
		std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables,
		const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
		const std::vector<int> & sortColIndices, const std::vector<voltron::compute::NullOrder> & sortOrderNulls) const
{
  throw std::runtime_error("ERROR: sorted_merger_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> get_pivot_points_table_functor::operator()<ral::frame::BlazingArrowTable>(
    int number_partitions,
    std::shared_ptr<ral::frame::BlazingTableView> sortedSamples) const
{

//  TODO percy arrow rommel enable this when we have arrow 4: ex gather_functor
//  std::vector<std::unique_ptr<cudf::column>> cs;
//  cs.push_back(std::make_unique<cudf::column>(column->view()));
//  auto ct = std::make_unique<cudf::table>(std::move(cs));
//  std::vector<cudf::column_metadata> mt;
//  mt.push_back(cudf::column_metadata("any"));
//  auto indexes = cudf::detail::to_arrow(ct->view(), mt);
//  auto idx = indexes->column(0);
//  auto at = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table);
//  auto arrow_table = at->view();
//  std::shared_ptr<arrow::Table> ret = arrow::compute::Take(*arrow_table, *idx).ValueOrDie();
//  return std::make_unique<ral::frame::BlazingArrowTable>(ret);

  throw std::runtime_error("ERROR: get_pivot_points_table_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<int> group_column_indices) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  return compute_groupby_without_aggregations(arrow_table_view->view(), group_column_indices);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> aggregations_without_groupby_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<voltron::compute::AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  return compute_aggregations_without_groupby(arrow_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<voltron::compute::AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases,
    std::vector<int> group_column_indices) const
{
  throw std::runtime_error("ERROR: aggregations_with_groupby_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> cross_join_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right) const
{
  throw std::runtime_error("ERROR: cross_join_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline bool check_if_has_nulls_functor::operator()<ral::frame::BlazingArrowTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, std::vector<int> const& keys) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  //return ral::cpu::check_if_has_nulls(arrow_table_view->view(), keys);
  throw std::runtime_error("ERROR: check_if_has_nulls_functor BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> inner_join_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<int> const& left_column_indices,
    std::vector<int> const& right_column_indices,
    voltron::compute::NullEquality equalityType) const
{
  // TODO percy arrow
  throw std::runtime_error("ERROR: inner_join_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> drop_nulls_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<int> const& keys) const
{
  // TODO percy arrow
  throw std::runtime_error("ERROR: drop_nulls_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> left_join_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<int> const& left_column_indices,
    std::vector<int> const& right_column_indices) const
{
  // TODO percy arrow
  throw std::runtime_error("ERROR: left_join_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> full_join_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    bool has_nulls_left,
    bool has_nulls_right,
    std::vector<int> const& left_column_indices,
    std::vector<int> const& right_column_indices) const
{
  // TODO percy arrow
  throw std::runtime_error("ERROR: full_join_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template<>
inline std::unique_ptr<ral::frame::BlazingTable> reordering_columns_due_to_right_join_functor::operator()<ral::frame::BlazingArrowTable>(
    std::unique_ptr<ral::frame::BlazingTable> table_ptr, size_t right_columns) const
{
  // TODO percy arrow
  throw std::runtime_error("ERROR: reordering_columns_due_to_right_join_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_functor::operator()<ral::frame::BlazingArrowTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
  ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table_view.get());
  std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = evaluate_expressions(table_view_ptr->view(), expressions);

  // TODO percy arrow
  //RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

  //return ral::cpu::applyBooleanFilter(table_view_ptr->view(), evaluated_table[0]);
  // TODO percy arrow
  
  return std::make_unique<ral::frame::BlazingArrowTable>(table_view_ptr->view());
}

//template <>
//std::unique_ptr<ral::frame::BlazingTable> process_project_functor::operator()<ral::frame::BlazingArrowTable>(
//    std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
//    const std::vector<std::string> & expressions,
//    const std::vector<std::string> & out_column_names) const
//{
//  return ral::execution::backend_dispatcher(
//    blazing_table_in->get_execution_backend(),
//    evaluate_expressions_functor(),
//    blazing_table_in->to_table_view(), expressions);
//}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingArrowTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  throw std::runtime_error("ERROR: build_only_schema BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_order_gather_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
    const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
    std::vector<voltron::compute::NullOrder> null_orders) const
{
  auto table = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  auto sortColumns = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(sortColumns_view);
  //std::unique_ptr<cudf::column> output = cudf::sorted_order( sortColumns->view(), sortOrderTypes, null_orders );
  auto col = table->view()->column(0); // TODO percy arrow
  
  // TODO percy arrow complete this when we have arrow 4
//  std::shared_ptr<arrow::Array> vals = arrow::Concatenate(col->chunks()).ValueOrDie();
//  std::shared_ptr<arrow::Array> output = arrow::compute::SortToIndices(*vals).ValueOrDie();
//  std::shared_ptr<arrow::Table> gathered = arrow::compute::Take(*table->view(), *output).ValueOrDie();
//  return std::make_unique<ral::frame::BlazingArrowTable>(gathered);

}

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
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_functor::operator()<ral::frame::BlazingArrowTable>(
    const std::vector<std::string> &column_names,
	const std::vector<std::shared_ptr<arrow::DataType>> &dtypes,
    std::vector<int> column_indices) const
{
  // TODO percy
  throw std::runtime_error("ERROR: create_empty_table_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr; //return ral::cpu::empty_like(arrow_table_view->view());
}

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
inline std::unique_ptr<ral::frame::BlazingTable> sample_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    int const num_samples,
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
inline std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
split_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<int> const& splits) const
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
inline void
normalize_types_functor::operator()<ral::frame::BlazingArrowTable>(
    std::unique_ptr<ral::frame::BlazingTable> & table,
    const std::vector<std::shared_ptr<arrow::DataType>>  & types,
    std::vector<int> column_indices) const
{
  throw std::runtime_error("ERROR: normalize_types_functor BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::vector<int>>
hash_partition_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<int> const& columns_to_hash,
    int num_partitions) const
{
  throw std::runtime_error("ERROR: hash_partition_functor BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline std::shared_ptr<ral::frame::BlazingTableView> select_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    const std::vector<int> & sortColIndices) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  std::shared_ptr<arrow::Table> selected = arrow_table_view->view()->SelectColumns(sortColIndices).ValueOrDie();
  return std::make_shared<ral::frame::BlazingArrowTableView>(selected);
}

// TODO percy arrow rommel enable this when we have arrow 4
#ifdef CUDF_SUPPORT
#include <thrust/binary_search.h>
template <>
inline std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
upper_bound_split_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> sortedTable_view,
    std::shared_ptr<ral::frame::BlazingTableView> t,
    std::shared_ptr<ral::frame::BlazingTableView> values,
    std::vector<voltron::compute::SortOrder> const& column_order,
    std::vector<voltron::compute::NullOrder> const& null_precedence) const
{
  auto sortedTable = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(sortedTable_view)->view();
  auto columns_to_search = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(t)->view();  
  auto partitionPlan = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(values)->view();  
  //thrust::host_vector<unsigned int> output(6);

  std::shared_ptr<arrow::Array> sortedTable_array = *arrow::Concatenate(
    sortedTable->column(0)->chunks(), arrow::default_memory_pool());
  std::shared_ptr<arrow::Int32Array> sortedTable_col = std::dynamic_pointer_cast<arrow::Int32Array>(sortedTable_array);


  std::shared_ptr<arrow::Array> columns_to_search_array = *arrow::Concatenate(
    sortedTable->column(0)->chunks(), arrow::default_memory_pool());
  std::shared_ptr<arrow::Int32Array> columns_to_search_col = std::dynamic_pointer_cast<arrow::Int32Array>(columns_to_search_array);

  std::shared_ptr<arrow::Array> partitionPlan_array = *arrow::Concatenate(
    sortedTable->column(0)->chunks(), arrow::default_memory_pool());
  std::shared_ptr<arrow::Int32Array> partitionPlan_col = std::dynamic_pointer_cast<arrow::Int32Array>(partitionPlan_array);

  thrust::host_vector<int32_t> sortedTable_vals(sortedTable_col->raw_values(), sortedTable_col->raw_values() + sortedTable_col->length());
  thrust::host_vector<int32_t> partitionPlan_vals(partitionPlan_col->raw_values(), partitionPlan_col->raw_values() + partitionPlan_col->length());

  std::vector<int32_t> out;
  out.resize(sortedTable_vals.size()+1);
  thrust::upper_bound(sortedTable_vals.begin(), sortedTable_vals.end(),
                      sortedTable_vals.begin(), sortedTable_vals.end(),
                      out.begin());

  auto int_builder = std::make_unique<arrow::Int32Builder>();
  int_builder->AppendValues(out);
  std::shared_ptr<arrow::Array> pivot_vals;
  int_builder->Finish(&pivot_vals);

  std::vector<std::shared_ptr<arrow::Array>> cols;
  cols.push_back(pivot_vals);
  auto pivot_indexes = arrow::Table::Make(sortedTable->schema(), cols);
  
  auto split_indexes = pivot_indexes;

//  auto pivot_indexes = cudf::upper_bound(columns_to_search, partitionPlan->view(), column_order, null_precedence);
//	std::vector<int> split_indexes = ral::utilities::column_to_vector<int>(pivot_indexes->view());
//  auto tbs = cudf::split(sortedTable->view(), split_indexes);
//  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> ret;
//  for (auto tb : tbs) {
//    ret.push_back(std::make_shared<ral::frame::BlazingCudfTableView>(tb, sortedTable_view->column_names()));
//  }
//  return ret;
  
  // TODO percy arrow
  //return nullptr;
  throw std::runtime_error("ERROR: upper_bound_split_functor BlazingSQL doesn't support this Arrow operator yet.");
}
#endif

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::PARQUET>::operator()<ral::frame::BlazingArrowTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<int> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::arrow_backend::io::read_parquet_file(file, column_indices, col_names, row_groups);
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::CSV>::operator()<ral::frame::BlazingArrowTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<int> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::arrow_backend::io::read_csv_file(file, column_indices, col_names, row_groups, args_map);
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::ORC>::operator()<ral::frame::BlazingArrowTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<int> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::arrow_backend::io::read_orc_file(file, column_indices, col_names, row_groups);
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::JSON>::operator()<ral::frame::BlazingArrowTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<int> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    return voltron::compute::arrow_backend::io::read_json_file(file, column_indices, col_names, row_groups);
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::PARQUET>::operator()<ral::frame::BlazingArrowTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::arrow_backend::io::parse_parquet_schema(schema_out, file);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable>
decache_io_functor::operator()<ral::frame::BlazingArrowTable>(
    std::unique_ptr<ral::frame::BlazingTable> table,
    std::vector<int> projections,
    ral::io::Schema schema,
    std::vector<int> column_indices_in_file,
    std::map<std::string, std::string> column_values) const
{
      std::vector<std::shared_ptr<arrow::ChunkedArray>> all_columns_arrow(projections.size());
      std::vector<std::shared_ptr<arrow::ChunkedArray>> file_columns_arrow;
      std::shared_ptr<const arrow::KeyValueMetadata> arrow_metadata;

      std::vector<std::string> names;
      int num_rows;
      if (column_indices_in_file.size() > 0){
          names = table->column_names();
          ral::frame::BlazingArrowTable* table_ptr = dynamic_cast<ral::frame::BlazingArrowTable*>(table.get());
          std::shared_ptr<arrow::Table> current_table = table_ptr->view();
          num_rows = current_table->num_rows();
          file_columns_arrow = current_table->columns();
          arrow_metadata = current_table->schema()->metadata();
      }

      int in_file_column_counter = 0;
      for(int i = 0; i < projections.size(); i++) {
        int col_ind = projections[i];
        if(!schema.get_in_file()[col_ind]) {
          std::string name = schema.get_name(col_ind);
          names.push_back(name);
          std::string literal_str = column_values[name];
            auto scalar = get_scalar_from_string_arrow(literal_str, schema.get_dtype(col_ind),  false);
            std::shared_ptr<arrow::Array> temp = arrow::MakeArrayFromScalar(*scalar, num_rows).ValueOrDie();
            all_columns_arrow[i] = std::make_shared<arrow::ChunkedArray>(temp);
        } else {
            all_columns_arrow[i] = file_columns_arrow[in_file_column_counter];
          in_file_column_counter++;
        }
      }

    auto new_schema = build_arrow_schema(all_columns_arrow, names, arrow_metadata);
    return std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(new_schema, all_columns_arrow, num_rows));
}

template<>
inline std::unique_ptr<ral::frame::BlazingHostTable> make_blazinghosttable_functor::operator()<ral::frame::BlazingArrowTable>(
	std::unique_ptr<ral::frame::BlazingTable> table, bool use_pinned){

  ral::frame::BlazingArrowTable *arrow_table_ptr = dynamic_cast<ral::frame::BlazingArrowTable*>(table.get());
  return ral::communication::messages::serialize_arrow_message_to_host_table(arrow_table_ptr->to_table_view(), use_pinned);
}


template<>
inline
std::unique_ptr<ral::cache::CacheData> make_cachedata_functor::operator()<ral::frame::BlazingArrowTable>(std::unique_ptr<ral::frame::BlazingTable> table){
  std::unique_ptr<ral::frame::BlazingArrowTable> arrow_table(dynamic_cast<ral::frame::BlazingArrowTable*>(table.release()));
  return std::make_unique<ral::cache::ArrowCacheData>(std::move(arrow_table));
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::CSV>::operator()<ral::frame::BlazingArrowTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::arrow_backend::io::parse_csv_schema(schema_out, file, args_map);
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::ORC>::operator()<ral::frame::BlazingArrowTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::arrow_backend::io::parse_orc_schema(schema_out, file, args_map);
}

template <> template <>
inline void
io_parse_file_schema_functor<ral::io::DataType::JSON>::operator()<ral::frame::BlazingArrowTable>(
        ral::io::Schema & schema_out,
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        const std::map<std::string, std::string> &args_map) const
{
    voltron::compute::arrow_backend::io::parse_json_schema(schema_out, file, args_map);
}

template <>
inline void write_orc_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::string file_path) const
{
	auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);

	std::shared_ptr<::arrow::io::FileOutputStream> outfile;
	PARQUET_ASSIGN_OR_THROW(outfile, ::arrow::io::FileOutputStream::Open(file_path));
	parquet::arrow::WriteTable(*arrow_table_view->view(), ::arrow::default_memory_pool(), outfile, 100);
}


template <>
inline std::unique_ptr<ral::frame::BlazingTable>  read_orc_functor::operator()<ral::frame::BlazingArrowTable>(
    std::string file_path, const std::vector<std::string> &col_names) const
{
  std::shared_ptr<::arrow::io::ReadableFile> infile;
  const char *orc_path_file = file_path.c_str();
  PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(orc_path_file, ::arrow::default_memory_pool()));

  std::unique_ptr<::parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(::parquet::arrow::OpenFile(infile, ::arrow::default_memory_pool(), &reader));
  std::shared_ptr<::arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

  remove(orc_path_file);
  return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

//} // compute
//} // voltron