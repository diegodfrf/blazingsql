#pragma once

#include "compute/api.h"

#include <arrow/compute/api.h>
#include <cudf/detail/interop.hpp>


#include "execution_graph/backend_dispatcher.h"

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"

#include <cudf/scalar/scalar_factories.hpp>

#include "operators/GroupBy.h"

#include <thrust/binary_search.h>

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

template<typename CPPType, typename ArrowScalarType>
inline std::unique_ptr<cudf::scalar> to_cudf_numeric_scalar(cudf::data_type cudf_dtype, std::shared_ptr<arrow::Scalar> arrow_scalar) {
  std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf_dtype);
  auto numeric_s = static_cast< cudf::scalar_type_t<CPPType>* >(scalar.get());
  std::shared_ptr<ArrowScalarType> s = std::static_pointer_cast<ArrowScalarType>(arrow_scalar);
  numeric_s->set_value((CPPType)s->value);
  return scalar;
}

inline std::unique_ptr<cudf::scalar> to_cudf_scalar(std::shared_ptr<arrow::Scalar> arrow_scalar)
{
  cudf::data_type cudf_dtype = cudf::detail::arrow_to_cudf_type(*arrow_scalar->type);
  switch (arrow_scalar->type->id()) {
    case arrow::Type::NA: {} break;
    case arrow::Type::BOOL: {} break;
    case arrow::Type::INT8: {
      return to_cudf_numeric_scalar<int8_t, arrow::Int8Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::INT16: {
      return to_cudf_numeric_scalar<int16_t, arrow::Int16Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::INT32: {
      return to_cudf_numeric_scalar<int32_t, arrow::Int32Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::INT64: {
      return to_cudf_numeric_scalar<int64_t, arrow::Int64Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT8: {
      return to_cudf_numeric_scalar<uint8_t, arrow::UInt8Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT16: {
      return to_cudf_numeric_scalar<uint16_t, arrow::UInt16Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT32: {
      return to_cudf_numeric_scalar<uint32_t, arrow::UInt32Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::UINT64: {
      return to_cudf_numeric_scalar<uint64_t, arrow::UInt64Scalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::FLOAT: {
      return to_cudf_numeric_scalar<float, arrow::FloatScalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::DOUBLE: {
      return to_cudf_numeric_scalar<double, arrow::DoubleScalar>(cudf_dtype, arrow_scalar);
    } break;
    case arrow::Type::DATE32: {} break;
    case arrow::Type::TIMESTAMP: {
      // TODO percy arrow
//      auto type = static_cast<arrow::TimestampType const*>(&arrow_type);
//      switch (type->unit()) {
//        case arrow::TimeUnit::type::SECOND: return data_type(type_id::TIMESTAMP_SECONDS);
//        case arrow::TimeUnit::type::MILLI: return data_type(type_id::TIMESTAMP_MILLISECONDS);
//        case arrow::TimeUnit::type::MICRO: return data_type(type_id::TIMESTAMP_MICROSECONDS);
//        case arrow::TimeUnit::type::NANO: return data_type(type_id::TIMESTAMP_NANOSECONDS);
//        default: CUDF_FAIL("Unsupported timestamp unit in arrow");
//      }
    } break;
    case arrow::Type::DURATION: {
      // TODO percy arrow
//      auto type = static_cast<arrow::DurationType const*>(&arrow_type);
//      switch (type->unit()) {
//        case arrow::TimeUnit::type::SECOND: return data_type(type_id::DURATION_SECONDS);
//        case arrow::TimeUnit::type::MILLI: return data_type(type_id::DURATION_MILLISECONDS);
//        case arrow::TimeUnit::type::MICRO: return data_type(type_id::DURATION_MICROSECONDS);
//        case arrow::TimeUnit::type::NANO: return data_type(type_id::DURATION_NANOSECONDS);
//        default: CUDF_FAIL("Unsupported duration unit in arrow");
//      }
    } break;
    case arrow::Type::STRING: {
      std::shared_ptr<arrow::StringScalar> s = std::static_pointer_cast<arrow::StringScalar>(arrow_scalar);
      return cudf::make_string_scalar(s->value->ToString());
    } break;
    case arrow::Type::DICTIONARY: {} break;
    case arrow::Type::LIST: {} break;
    case arrow::Type::DECIMAL: {
      // TODO percy arrow
      //auto const type = static_cast<arrow::Decimal128Type const*>(&arrow_type);
      //return data_type{type_id::DECIMAL64, -type->scale()};
    } break;
    case arrow::Type::STRUCT: {} break;
  }

  // TODO percy arrow thrown error
}


inline void normalize_types(std::unique_ptr<ral::frame::BlazingArrowTable> & table,  const std::vector<cudf::data_type> & types,
                     std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>()) {
  // TODO percy
}




inline std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
 	std::shared_ptr<arrow::Table> table, const std::vector<int> & group_column_indices) {

   std::vector<std::shared_ptr<arrow::Array>> result;
   result.resize(group_column_indices.size());
 
   auto f = [&table] (int col_idx) -> std::shared_ptr<arrow::Array> {
     auto uniques = arrow::compute::Unique(table->column(col_idx));
     if (!uniques.ok()) {
       // TODO throw/handle error here
       return nullptr;
     }
     return std::move(uniques.ValueOrDie());
   };

   std::transform(group_column_indices.begin(), group_column_indices.end(), result.begin(), f);

   return std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(
     table->schema(),
     std::move(result),
     table->num_rows()));
 }

inline std::shared_ptr<arrow::Scalar> arrow_reduce(std::shared_ptr<arrow::ChunkedArray> col,
                                std::unique_ptr<cudf::aggregation> const &agg,
                                cudf::data_type output_dtype)
{
 switch (agg->kind) {
   case cudf::aggregation::SUM: {
     auto result = arrow::compute::Sum(col);
     // TODO percy arrow error
     return result.ValueOrDie().scalar();
   } break;
   case cudf::aggregation::PRODUCT: {
    
   } break;
   case cudf::aggregation::MIN: {
    
   } break;
   case cudf::aggregation::MAX: {
    
   } break;
   case cudf::aggregation::COUNT_VALID: {
    
   } break;
   case cudf::aggregation::COUNT_ALL: {
    
   } break;
   case cudf::aggregation::ANY: {
    
   } break;
   case cudf::aggregation::ALL: {
    
   } break;
   case cudf::aggregation::SUM_OF_SQUARES: {
    
   } break;
   case cudf::aggregation::MEAN: {
    
   } break;
   case cudf::aggregation::VARIANCE: {
    
   } break;
   case cudf::aggregation::STD: {
    
   } break;
   case cudf::aggregation::MEDIAN: {
    
   } break;
   case cudf::aggregation::QUANTILE: {
    
   } break;
   case cudf::aggregation::ARGMAX: {
    
   } break;
   case cudf::aggregation::ARGMIN: {
    
   } break;
   case cudf::aggregation::NUNIQUE: {
    
   } break;
   case cudf::aggregation::NTH_ELEMENT: {
    
   } break;
   case cudf::aggregation::ROW_NUMBER: {
    
   } break;
   case cudf::aggregation::COLLECT_LIST: {
    
   } break;
   case cudf::aggregation::COLLECT_SET: {
    
   } break;
   case cudf::aggregation::LEAD: {
    
   } break;
   case cudf::aggregation::LAG: {
    
   } break;
   case cudf::aggregation::PTX: {
    
   } break;
   case cudf::aggregation::CUDA: {
    
   } break;
  };
  return nullptr;
}

inline std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
 		std::shared_ptr<ral::frame::BlazingArrowTableView> table_view, const std::vector<std::string> & aggregation_input_expressions,
 		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases)
{
  using namespace ral::operators;
  
    std::shared_ptr<arrow::Table> table = table_view->view();

 	std::vector<std::shared_ptr<arrow::Scalar>> reductions;
 	std::vector<std::string> agg_output_column_names;
 	for (size_t i = 0; i < aggregation_types.size(); i++){
 		if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
            std::shared_ptr<arrow::Int64Scalar> scalar = std::make_shared<arrow::Int64Scalar>(table->num_rows());
 			reductions.emplace_back(std::move(scalar));
 		} else {
 			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> aggregation_input_scope_holder;
            std::shared_ptr<arrow::ChunkedArray> aggregation_input;
 			if(is_var_column(aggregation_input_expressions[i]) || is_number(aggregation_input_expressions[i])) {
 				aggregation_input = table->column(get_index(aggregation_input_expressions[i]));

 			} else {
         //TODO percy rommel arrow
 				//aggregation_input_scope_holder = ral::processor::evaluate_expressions(table.view(), {aggregation_input_expressions[i]});
 				//aggregation_input = aggregation_input_scope_holder[0]->view();
 			}

 			if( aggregation_types[i] == AggregateKind::COUNT_VALID) {
                std::shared_ptr<arrow::Int64Scalar> scalar = std::make_shared<arrow::Int64Scalar>(aggregation_input->length() - aggregation_input->null_count());
 				reductions.emplace_back(std::move(scalar));
 			} else {
                std::unique_ptr<cudf::aggregation> agg = makeCudfAggregation<cudf::aggregation>(aggregation_types[i]);
                cudf::type_id theinput_type = cudf::detail::arrow_to_cudf_type(*aggregation_input->type()).id();
 				cudf::type_id output_type = get_aggregation_output_type(theinput_type, aggregation_types[i], false);

 				std::shared_ptr<arrow::Scalar> reduction_out = arrow_reduce(aggregation_input, agg, cudf::data_type(output_type));

 				if (aggregation_types[i] == AggregateKind::SUM0 && !reduction_out->is_valid){ // if this aggregation was a SUM0, and it was not valid, we want it to be a valid 0 instead
                    auto dt = cudf::detail::arrow_to_cudf_type(*reduction_out->type);
 					std::shared_ptr<arrow::Int64Scalar> zero_scalar = std::make_shared<arrow::Int64Scalar>(0);
 					reductions.emplace_back(std::move(zero_scalar));
 				} else {
 					reductions.emplace_back(std::move(reduction_out));
 				}
 			}
 		}
 		// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
 		if(aggregation_column_assigned_aliases[i] == "") {
 			if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
 				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(*)");
 			} else {
 				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table_view->column_names().at(get_index(aggregation_input_expressions[i])) + ")");
 			}
 		} else {
 			agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
 		}
 	}
 	// convert scalars into columns
 	std::vector<std::shared_ptr<arrow::ChunkedArray>> output_columns;
 	for (size_t i = 0; i < reductions.size(); i++){
 		std::shared_ptr<arrow::Array> temp = arrow::MakeArrayFromScalar((*reductions[i].get()), 1).ValueOrDie();
 		output_columns.emplace_back(std::make_shared<arrow::ChunkedArray>(temp));
 	}
  
  auto new_schema = build_arrow_schema(
        output_columns,
        agg_output_column_names,
        table->schema()->metadata());
  
  auto tt = arrow::Table::Make(new_schema, output_columns);
  return std::make_unique<ral::frame::BlazingArrowTable>(tt);
}





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
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices, const std::vector<cudf::null_order> & sortOrderNulls) const
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
    std::vector<AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  return compute_aggregations_without_groupby(arrow_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<AggregateKind> aggregation_types,
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
inline std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingArrowTableView>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  return nullptr;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_order_gather_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
    const std::vector<cudf::order> & sortOrderTypes,
    std::vector<cudf::null_order> null_orders) const
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
	  const std::vector<cudf::data_type> &dtypes) const
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
    const std::vector<cudf::data_type> & types,
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
    std::vector<cudf::order> const& column_order,
    std::vector<cudf::null_order> const& null_precedence) const
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
//} // compute
//} // voltron
