#pragma once

#include "compute/api.h"

#include <cudf/detail/interop.hpp>


#include "compute/backend_dispatcher.h"

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"

#include <cudf/scalar/scalar_factories.hpp>

#include <thrust/binary_search.h>

#include "compute/arrow/detail/aggregations.h"
#include "compute/arrow/detail/io.h"
#include "compute/arrow/detail/scalars.h"


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

inline std::unique_ptr<ral::frame::BlazingTable> getLimitedRows(std::shared_ptr<arrow::Table> table, cudf::size_type num_rows, bool front=true){
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
	cudf::size_type table_rows = table->num_rows();
	if (num_rows_limit <= 0) {
		return std::make_tuple(std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns(), 0)), false, 0);
	} else if (num_rows_limit >= table_rows) {
		return std::make_tuple(std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(table->schema(), table->columns())), true, num_rows_limit - table_rows);
	} else {
		return std::make_tuple(getLimitedRows(table, num_rows_limit), false, 0);
	}
}

// TODO percy arrow
inline std::pair<std::shared_ptr<arrow::Table>, std::vector<cudf::size_type>>
split_arrow(std::shared_ptr<arrow::Table> table_View,
            std::vector<cudf::size_type> const& columns_to_hash,
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
inline std::unique_ptr<ral::frame::BlazingTable> gather_functor::operator()<ral::frame::BlazingArrowTable>(
		std::shared_ptr<ral::frame::BlazingTableView> table,
		std::unique_ptr<cudf::column> column,
		cudf::out_of_bounds_policy out_of_bounds_policy,
		cudf::detail::negative_index_policy negative_index_policy) const
{
  // TODO percy arrow
  //throw std::runtime_error("ERROR: gather_functor BlazingSQL doesn't support this Arrow operator yet.");

  std::vector<std::unique_ptr<cudf::column>> cs;
  cs.push_back(std::make_unique<cudf::column>(column->view()));
  auto ct = std::make_unique<cudf::table>(std::move(cs));
  std::vector<cudf::column_metadata> mt;
  mt.push_back(cudf::column_metadata("any"));
  auto indexes = cudf::detail::to_arrow(ct->view(), mt);
  auto idx = indexes->column(0);
  auto at = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table);
  auto arrow_table = at->view();
  std::shared_ptr<arrow::Table> ret = arrow::compute::Take(*arrow_table, *idx).ValueOrDie();
  return std::make_unique<ral::frame::BlazingArrowTable>(ret);
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
  std::shared_ptr<ral::frame::BlazingTableView> table_view, std::vector<cudf::size_type> const& keys) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  //return ral::cpu::check_if_has_nulls(arrow_table_view->view(), keys);
  throw std::runtime_error("ERROR: check_if_has_nulls_functor BlazingSQL doesn't support this Arrow operator yet.");
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> inner_join_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices,
    cudf::null_equality equalityType) const
{
  // TODO percy arrow
  throw std::runtime_error("ERROR: inner_join_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> drop_nulls_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<cudf::size_type> const& keys) const
{
  // TODO percy arrow
  throw std::runtime_error("ERROR: drop_nulls_functor BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> left_join_functor::operator()<ral::frame::BlazingArrowTable>(    
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices) const
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
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices) const
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
  std::shared_ptr<arrow::Array> vals = arrow::Concatenate(col->chunks()).ValueOrDie();
  std::shared_ptr<arrow::Array> output = arrow::compute::SortToIndices(*vals).ValueOrDie();
  std::shared_ptr<arrow::Table> gathered = arrow::compute::Take(*table->view(), *output).ValueOrDie();
  return std::make_unique<ral::frame::BlazingArrowTable>(gathered);

  
  /*
  auto table = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  auto arrow_table = table->view();
  for (int c = 0; c < arrow_table->columns().size(); ++c) {
    auto col = arrow_table->column(c);
    std::shared_ptr<arrow::Array> flecha = arrow::Concatenate(col->chunks()).ValueOrDie();
    std::shared_ptr<arrow::Array> sorted_indx = arrow::compute::SortToIndices(*flecha).ValueOrDie();
    arrow::compute::Take(sorted_indx);
  }
*/
//    std::unique_ptr<> arrayBuilder;
//    arrow::ArrayBuilder()
//    arrayBuilder->
//    std::shared_ptr<arrow::Array> temp = std::make_shared<arrow::Array>(col);
    //arrow::compute::SortToIndices();
  //arrow::compute::SortToIndices(
//    arrow::compute::Take();
//  }
  // TODO percy arrow
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
inline void
normalize_types_functor::operator()<ral::frame::BlazingArrowTable>(
    std::unique_ptr<ral::frame::BlazingTable> & table,
    const std::vector<std::shared_ptr<arrow::DataType>>  & types,
    std::vector<cudf::size_type> column_indices) const
{
  throw std::runtime_error("ERROR: normalize_types_functor BlazingSQL doesn't support this Arrow operator yet.");
}

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
inline std::shared_ptr<ral::frame::BlazingTableView> select_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    const std::vector<int> & sortColIndices) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  std::shared_ptr<arrow::Table> selected = arrow_table_view->view()->SelectColumns(sortColIndices).ValueOrDie();
  return std::make_shared<ral::frame::BlazingArrowTableView>(selected);
}

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
//	std::vector<cudf::size_type> split_indexes = ral::utilities::column_to_vector<cudf::size_type>(pivot_indexes->view());
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

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::PARQUET>::operator()<ral::frame::BlazingArrowTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<cudf::size_type> row_groups,
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
        std::vector<cudf::size_type> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    // TODO percy arrow error
    // TODO csv reader for arrow
    return nullptr;
}

template <> template <>
inline std::unique_ptr<ral::frame::BlazingTable>
io_read_file_data_functor<ral::io::DataType::ORC>::operator()<ral::frame::BlazingArrowTable>(
        std::shared_ptr<arrow::io::RandomAccessFile> file,
        std::vector<int> column_indices,
        std::vector<std::string> col_names,
        std::vector<cudf::size_type> row_groups,
        const std::map<std::string, std::string> &args_map) const
{
    // TODO percy arrow error
    // TODO orc reader for arrow
    return nullptr;
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
inline std::shared_ptr<arrow::Scalar>
get_scalar_from_string_functor::operator()<ral::frame::BlazingArrowTable>(
        const std::string & scalar_string,
        std::shared_ptr<arrow::DataType> type,
        bool strings_have_quotes) const
{
    return get_scalar_from_string_arrow(scalar_string, type, strings_have_quotes);
}


//} // compute
//} // voltron
