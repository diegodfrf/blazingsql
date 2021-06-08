#include "compute/arrow/detail/aggregations.h"
#include <cudf/detail/interop.hpp>
#include "blazing_table/BlazingColumn.h"
#include "parser/groupby_parser_utils.h"
#include "compute/arrow/detail/types.h"

template<typename CPPType, typename ArrowScalarType>
std::unique_ptr<cudf::scalar> to_cudf_numeric_scalar(cudf::data_type cudf_dtype, std::shared_ptr<arrow::Scalar> arrow_scalar) {
  std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf_dtype);
  auto numeric_s = static_cast< cudf::scalar_type_t<CPPType>* >(scalar.get());
  std::shared_ptr<ArrowScalarType> s = std::static_pointer_cast<ArrowScalarType>(arrow_scalar);
  numeric_s->set_value((CPPType)s->value);
  return scalar;
}

std::unique_ptr<cudf::scalar> to_cudf_scalar(std::shared_ptr<arrow::Scalar> arrow_scalar)
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


void normalize_types(std::unique_ptr<ral::frame::BlazingArrowTable> & table, const std::vector<std::shared_ptr<arrow::DataType>> & types,
                     std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>()) {
  // TODO percy
}




std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
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

std::shared_ptr<arrow::Scalar> arrow_reduce(std::shared_ptr<arrow::ChunkedArray> col,
                                voltron::compute::AggregateKind agg,
                                cudf::data_type output_dtype)
{
 switch (agg) {
   case voltron::compute::AggregateKind::SUM: {
     auto result = arrow::compute::Sum(col);
     // TODO percy arrow error
     return result.ValueOrDie().scalar();
   } break;
   case voltron::compute::AggregateKind::SUM0: {
    break;
   }
   case voltron::compute::AggregateKind::MEAN: {
    break;
   }
   case voltron::compute::AggregateKind::MIN: {
    break;
   }
   case voltron::compute::AggregateKind::MAX: {
    break;
   }
   case voltron::compute::AggregateKind::COUNT_VALID: {
    break;
   }
   case voltron::compute::AggregateKind::COUNT_ALL: {
    break;
   }
   case voltron::compute::AggregateKind::COUNT_DISTINCT: {
    break;
   }
   case voltron::compute::AggregateKind::ROW_NUMBER: {
    break;
   }
   case voltron::compute::AggregateKind::LAG: {
    break;
   }
   case voltron::compute::AggregateKind::LEAD: {
    break;
   }
   case voltron::compute::AggregateKind::NTH_ELEMENT: {
    break;
   }
  };
  //TODO: Rommel Throw exception
  return nullptr;
}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
 		std::shared_ptr<ral::frame::BlazingArrowTableView> table_view, const std::vector<std::string> & aggregation_input_expressions,
 		const std::vector<voltron::compute::AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases)
{
    std::shared_ptr<arrow::Table> table = table_view->view();

 	std::vector<std::shared_ptr<arrow::Scalar>> reductions;
 	std::vector<std::string> agg_output_column_names;
 	for (size_t i = 0; i < aggregation_types.size(); i++){
 		if(aggregation_input_expressions[i] == "" && aggregation_types[i] == voltron::compute::AggregateKind::COUNT_ALL) { // this is a COUNT(*)
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

 			if( aggregation_types[i] == voltron::compute::AggregateKind::COUNT_VALID) {
                std::shared_ptr<arrow::Int64Scalar> scalar = std::make_shared<arrow::Int64Scalar>(aggregation_input->length() - aggregation_input->null_count());
 				reductions.emplace_back(std::move(scalar));
 			} else {
        //std::unique_ptr<cudf::aggregation> agg = makeCudfAggregation<cudf::aggregation>(aggregation_types[i]);
        cudf::type_id theinput_type = cudf::detail::arrow_to_cudf_type(*aggregation_input->type()).id();
        cudf::type_id output_type = get_aggregation_output_type(theinput_type, aggregation_types[i], false);

        std::shared_ptr<arrow::Scalar> reduction_out = arrow_reduce(aggregation_input, aggregation_types[i], cudf::data_type(output_type));

 				if (aggregation_types[i] == voltron::compute::AggregateKind::SUM0 && !reduction_out->is_valid){ // if this aggregation was a SUM0, and it was not valid, we want it to be a valid 0 instead
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
 			if(aggregation_input_expressions[i] == "" && aggregation_types[i] == voltron::compute::AggregateKind::COUNT_ALL) { // this is a COUNT(*)
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
 
