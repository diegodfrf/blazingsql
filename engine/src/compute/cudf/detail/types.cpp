#include "compute/cudf/detail/types.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/unary.hpp>
#include <numeric>
#include "blazing_table/BlazingColumnOwner.h"
#include "parser/CalciteExpressionParsing.h"
#include "parser/types_parser_utils.h"
#include "utilities/error.hpp"
#include "parser/CalciteExpressionParsing.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <numeric>
#include "utilities/error.hpp"
#include <numeric>
#include "parser/expression_utils.hpp"

namespace voltron {
namespace compute {
namespace cudf_backend {
namespace types {

void normalize_types_gpu(std::unique_ptr<ral::frame::BlazingTable> & gpu_table,  const std::vector<std::shared_ptr<arrow::DataType>>  & types,
		std::vector<cudf::size_type> column_indices) {
  	auto table = dynamic_cast<ral::frame::BlazingCudfTable*>(gpu_table.get());
  
	if (column_indices.size() == 0){
		RAL_EXPECTS(static_cast<size_t>(table->num_columns()) == types.size(), "In normalize_types: table->num_columns() != types.size()");
		column_indices.resize(table->num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	} else {
		RAL_EXPECTS(column_indices.size() == types.size(), "In normalize_types: column_indices.size() != types.size()");
	}
	std::vector<std::unique_ptr<ral::frame::BlazingColumn>> columns = table->releaseBlazingColumns();
	for (size_t i = 0; i < column_indices.size(); i++){
		cudf::data_type type_to_cast =  arrow_type_to_cudf_data_type_cudf(types[i]->id());
		if (!(columns[column_indices[i]]->view().type() == type_to_cast)){
			std::unique_ptr<cudf::column> casted = cudf::cast(columns[column_indices[i]]->view(), type_to_cast);
			columns[column_indices[i]] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(casted));
		}
	}
	gpu_table = std::make_unique<ral::frame::BlazingCudfTable>(std::move(columns), table->column_names());
}

std::unique_ptr<ral::frame::BlazingCudfTable> create_empty_cudf_table(const std::vector<std::string> &column_names,
	const std::vector<std::shared_ptr<arrow::DataType>> &dtypes, std::vector<int> column_indices) {

	if (column_indices.size() == 0){
		column_indices.resize(column_names.size());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	std::vector<std::unique_ptr<cudf::column>> columns(column_indices.size());

	for (auto idx : column_indices) {
		cudf::data_type dtype = arrow_type_to_cudf_data_type_cudf(dtypes[idx]->id());
		columns[idx] = cudf::make_empty_column(dtype);
	}
	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(table), column_names);
}

std::unique_ptr<cudf::table> create_empty_cudf_table(const std::vector<std::shared_ptr<arrow::Type::type>> &dtypes) {
	std::vector<std::unique_ptr<cudf::column>> columns(dtypes.size());
	for (size_t idx =0; idx < dtypes.size(); idx++) {
		cudf::data_type dtype = arrow_type_to_cudf_data_type_cudf(*dtypes[idx]);
		columns[idx] = cudf::make_empty_column(dtype);
	}
	return std::make_unique<cudf::table>(std::move(columns));
}

std::vector<cudf::order> toCudfOrderTypes(std::vector<voltron::compute::SortOrder> sortOrderTypes) {
  std::vector<cudf::order> cudfOrderTypes;
  cudfOrderTypes.resize(sortOrderTypes.size());

  for(std::size_t idx = 0; idx < cudfOrderTypes.size(); idx++){
    switch(sortOrderTypes[idx]){
      case voltron::compute::SortOrder::ASCENDING:
        cudfOrderTypes[idx]=cudf::order::ASCENDING;
      break;
      case voltron::compute::SortOrder::DESCENDING:
        cudfOrderTypes[idx]=cudf::order::DESCENDING;
      break;
    }
  }
  return cudfOrderTypes;
}

std::vector<cudf::null_order> toCudfNullOrderTypes(std::vector<voltron::compute::NullOrder> sortNullOrderTypes) {
  std::vector<cudf::null_order> cudfNullOrderTypes;
  cudfNullOrderTypes.resize(sortNullOrderTypes.size());

  for(std::size_t idx = 0; idx < cudfNullOrderTypes.size(); idx++){
    switch(sortNullOrderTypes[idx]){
      case voltron::compute::NullOrder::AFTER:
        cudfNullOrderTypes[idx]=cudf::null_order::AFTER;
      break;
      case voltron::compute::NullOrder::BEFORE:
        cudfNullOrderTypes[idx]=cudf::null_order::BEFORE;
      break;
    }
  }
  return cudfNullOrderTypes;
}

cudf::data_type get_common_cudf_type(cudf::data_type type1, cudf::data_type type2, bool strict) {
	if(type1 == type2) {
		return type1;
	} else if((is_type_float_cudf(type1.id()) && is_type_float_cudf(type2.id())) || (is_type_integer_cudf(type1.id()) && is_type_integer_cudf(type2.id()))) {
		return (cudf::size_of(type1) >= cudf::size_of(type2))	? type1	: type2;
	} else if(is_type_timestamp_cudf(type1.id()) && is_type_timestamp_cudf(type2.id())) {
		// if they are both datetime, return the highest resolution either has
		static constexpr std::array<cudf::data_type, 5> datetime_types = {
			cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_MICROSECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}
		};

		for (auto datetime_type : datetime_types){
			if(type1 == datetime_type || type2 == datetime_type)
				return datetime_type;
		}
	} else if(is_type_duration_cudf(type1.id()) && is_type_duration_cudf(type2.id())) {
		// if they are both durations, return the highest resolution either has
		static constexpr std::array<cudf::data_type, 5> duration_types = {
			cudf::data_type{cudf::type_id::DURATION_NANOSECONDS},
			cudf::data_type{cudf::type_id::DURATION_MICROSECONDS},
			cudf::data_type{cudf::type_id::DURATION_MILLISECONDS},
			cudf::data_type{cudf::type_id::DURATION_SECONDS},
			cudf::data_type{cudf::type_id::DURATION_DAYS}
		};

		for (auto duration_type : duration_types){
			if(type1 == duration_type || type2 == duration_type)
				return duration_type;
		}
	}
	else if ( is_type_string_cudf(type1.id()) && is_type_timestamp_cudf(type2.id()) ) {
		return type2;
	}
	if (strict) {
		RAL_FAIL("No common type between " + std::to_string(static_cast<int32_t>(type1.id())) + " and " + std::to_string(static_cast<int32_t>(type2.id())));
	} else {
		if(is_type_float_cudf(type1.id()) && is_type_integer_cudf(type2.id())) {
			return type1;
		} else if (is_type_float_cudf(type2.id()) && is_type_integer_cudf(type1.id())) {
			return type2;
		} else if (is_type_bool_cudf(type1.id()) && (is_type_integer_cudf(type2.id()) || is_type_float_cudf(type2.id()) || is_type_string_cudf(type2.id()) )){
			return type2;
		} else if (is_type_bool_cudf(type2.id()) && (is_type_integer_cudf(type1.id()) || is_type_float_cudf(type1.id()) || is_type_string_cudf(type1.id()) )){
			return type1;
		}
	}
}

bool is_type_float_cudf(cudf::type_id type) { return (cudf::type_id::FLOAT32 == type || cudf::type_id::FLOAT64 == type); }

bool is_type_integer_cudf(cudf::type_id type) {
	return (cudf::type_id::INT8 == type || cudf::type_id::INT16 == type || cudf::type_id::INT32 == type ||
			cudf::type_id::INT64 == type || cudf::type_id::UINT8 == type || cudf::type_id::UINT16 == type ||
			cudf::type_id::UINT32 == type || cudf::type_id::UINT64 == type);
}

bool is_type_bool_cudf(cudf::type_id type) { return cudf::type_id::BOOL8 == type; }

bool is_type_timestamp_cudf(cudf::type_id type) {
	return (cudf::type_id::TIMESTAMP_DAYS == type || cudf::type_id::TIMESTAMP_SECONDS == type ||
			cudf::type_id::TIMESTAMP_MILLISECONDS == type || cudf::type_id::TIMESTAMP_MICROSECONDS == type ||
			cudf::type_id::TIMESTAMP_NANOSECONDS == type);
}
bool is_type_duration_cudf(cudf::type_id type) {
	return (cudf::type_id::DURATION_DAYS == type || cudf::type_id::DURATION_SECONDS == type ||
			cudf::type_id::DURATION_MILLISECONDS == type || cudf::type_id::DURATION_MICROSECONDS == type ||
			cudf::type_id::DURATION_NANOSECONDS == type);
}
bool is_type_string_cudf(cudf::type_id type) { return cudf::type_id::STRING == type; }

cudf::type_id get_aggregation_output_type_cudf(cudf::type_id input_type, voltron::compute::AggregateKind aggregation, bool have_groupby) {
  if(aggregation == voltron::compute::AggregateKind::COUNT_VALID || aggregation == voltron::compute::AggregateKind::COUNT_ALL) {
    return cudf::type_id::INT64;
  } else if(aggregation == voltron::compute::AggregateKind::SUM || aggregation == voltron::compute::AggregateKind::SUM0) {
    if(have_groupby)
      return input_type;  // current group by function can only handle this
    else {
      // we can assume it is numeric based on the oepration
      // to be safe we should enlarge to the greatest integer or float representation
      return is_type_float_cudf(input_type) ? cudf::type_id::FLOAT64 : cudf::type_id::INT64;
    }
  } else if(aggregation == voltron::compute::AggregateKind::MIN) {
    return input_type;
  } else if(aggregation == voltron::compute::AggregateKind::MAX) {
    return input_type;
  } else if(aggregation == voltron::compute::AggregateKind::MEAN) {
    return cudf::type_id::FLOAT64;
  } else if(aggregation == voltron::compute::AggregateKind::COUNT_DISTINCT) {
    /* Currently this conditional is unreachable.
       Calcite transforms count distincts through the
       AggregateExpandDistinctAggregates rule, so in fact,
       each count distinct is replaced by some group by clauses. */
    return cudf::type_id::INT64;
  } else {
    throw std::runtime_error(
        "In get_aggregation_output_type function: aggregation type not supported.");
  }
}

cudf::strings::strip_type map_trim_flag_to_strip_type(const std::string & trim_flag)
{
    if (trim_flag == "BOTH")
        return cudf::strings::strip_type::BOTH;
    else if (trim_flag == "LEADING")
        return cudf::strings::strip_type::LEFT;
    else if (trim_flag == "TRAILING")
        return cudf::strings::strip_type::RIGHT;
    else
        // Should not reach here
        assert(false);
}

expr_output_type_visitor::expr_output_type_visitor(const cudf::table_view & table) : table_{table} { }

void expr_output_type_visitor::visit(const ral::parser::operad_node& node)  {
  std::shared_ptr<arrow::DataType> output_type;
  if (is_literal(node.value)) {
    output_type = static_cast<const ral::parser::literal_node&>(node).type();
  } else {
      cudf::size_type idx = static_cast<const ral::parser::variable_node&>(node).index();
      output_type = cudf_type_id_to_arrow_type_cudf(table_.column(idx).type().id());

          // Also store the variable idx for later use
          variable_indices_.push_back(idx);
  }

  node_to_type_map_.insert({&node, output_type});
  expr_output_type_ = output_type;
}

void expr_output_type_visitor::visit(const ral::parser::operator_node& node)  {
  std::shared_ptr<arrow::DataType> output_type;
  operator_type op = map_to_operator_type(node.value);
  if(is_binary_operator(op)) {
    output_type = get_output_type(op, node_to_type_map_.at(node.children[0].get()), node_to_type_map_.at(node.children[1].get()));
  } else if (is_unary_operator(op)) {
    output_type = get_output_type(op, node_to_type_map_.at(node.children[0].get()));
  }else{
    output_type = get_output_type(op);
  }

  node_to_type_map_.insert({&node, output_type});
  expr_output_type_ = output_type;
}

std::shared_ptr<arrow::DataType> expr_output_type_visitor::get_expr_output_type() { return expr_output_type_; }

const std::vector<cudf::size_type> & expr_output_type_visitor::get_variable_indices() { return variable_indices_; }

std::shared_ptr<arrow::DataType> cudf_type_id_to_arrow_type_cudf(cudf::type_id type) {
  switch (type)
  {
    case cudf::type_id::BOOL8: return arrow::boolean();
    case cudf::type_id::INT8: return arrow::int8();
    case cudf::type_id::INT16: return arrow::int16();
    case cudf::type_id::INT32: return arrow::int32();
    case cudf::type_id::INT64: return arrow::int64();
    case cudf::type_id::UINT8: return arrow::uint8();
    case cudf::type_id::UINT16: return arrow::uint16();
    case cudf::type_id::UINT32: return arrow::uint32();
    case cudf::type_id::UINT64: return arrow::uint64();
    case cudf::type_id::FLOAT32: return arrow::float32();
    case cudf::type_id::FLOAT64: return arrow::float64();
    case cudf::type_id::STRING: return arrow::utf8();
    case cudf::type_id::TIMESTAMP_DAYS: return arrow::date32();
    case cudf::type_id::TIMESTAMP_SECONDS: //return arrow::TimeUnit::type::SECOND;
      return arrow::timestamp(arrow::TimeUnit::type::SECOND);
    case cudf::type_id::TIMESTAMP_MILLISECONDS: //arrow::TimeUnit::type::MILLI;
      return arrow::timestamp(arrow::TimeUnit::type::MILLI);
    case cudf::type_id::TIMESTAMP_MICROSECONDS: //return arrow::TimeUnit::type::MICRO;
      return arrow::timestamp(arrow::TimeUnit::type::MICRO);
    case cudf::type_id::TIMESTAMP_NANOSECONDS: //return arrow::TimeUnit::type::NANO;
      return arrow::timestamp(arrow::TimeUnit::type::NANO);

// TODO
//    case cudf::type_id::DURATION_DAYS: return arrow::duration(arrow::TimeUnit::type::SECOND); // TODO: check this??

    case cudf::type_id::DURATION_SECONDS: //return arrow::TimeUnit::type::SECOND;
      return arrow::duration(arrow::TimeUnit::type::SECOND);
    case cudf::type_id::DURATION_MILLISECONDS: //return arrow::TimeUnit::type::MILLI;
      return arrow::duration(arrow::TimeUnit::type::MILLI);
    case cudf::type_id::DURATION_MICROSECONDS: //return arrow::TimeUnit::type::MICRO;
      return arrow::duration(arrow::TimeUnit::type::MICRO);
    case cudf::type_id::DURATION_NANOSECONDS: //return arrow::TimeUnit::type::NANO;
      return arrow::duration(arrow::TimeUnit::type::NANO);

    //TODO: Check this/?
    case cudf::type_id::LIST: return arrow::list(arrow::int32());
    case cudf::type_id::STRUCT: return arrow::struct_({});
    case cudf::type_id::DICTIONARY32: return arrow::dictionary(arrow::int32(), arrow::int32());
      // TODO: DECIMAL32, DECIMAL64
    default: return arrow::null();
  }
}

cudf::data_type arrow_type_to_cudf_data_type_cudf(arrow::Type::type arrow_type) {
	switch (arrow_type)
	{
	case arrow::Type::NA: return cudf::data_type(cudf::type_id::EMPTY);
	case arrow::Type::BOOL: return cudf::data_type(cudf::type_id::BOOL8);
	case arrow::Type::INT8: return cudf::data_type(cudf::type_id::INT8);
	case arrow::Type::INT16: return cudf::data_type(cudf::type_id::INT16);
	case arrow::Type::INT32: return cudf::data_type(cudf::type_id::INT32);
	case arrow::Type::INT64: return cudf::data_type(cudf::type_id::INT64);
	case arrow::Type::UINT8: return cudf::data_type(cudf::type_id::UINT8);
	case arrow::Type::UINT16: return cudf::data_type(cudf::type_id::UINT16);
	case arrow::Type::UINT32: return cudf::data_type(cudf::type_id::UINT32);
	case arrow::Type::UINT64: return cudf::data_type(cudf::type_id::UINT64);
	case arrow::Type::FLOAT: return cudf::data_type(cudf::type_id::FLOAT32);
	case arrow::Type::DOUBLE: return cudf::data_type(cudf::type_id::FLOAT64);
	case arrow::Type::DATE32: return cudf::data_type(cudf::type_id::TIMESTAMP_DAYS);
	case arrow::Type::STRING: return cudf::data_type(cudf::type_id::STRING);
	case arrow::Type::DICTIONARY: return cudf::data_type(cudf::type_id::DICTIONARY32);
	case arrow::Type::LIST: return cudf::data_type(cudf::type_id::LIST);
	case arrow::Type::STRUCT: return cudf::data_type(cudf::type_id::STRUCT);
	// TODO: for now we are handling just MILLI
	case arrow::Type::TIMESTAMP: return cudf::data_type(cudf::type_id::TIMESTAMP_MILLISECONDS);
	case arrow::Type::DURATION: return cudf::data_type(cudf::type_id::DURATION_MILLISECONDS);

		// TODO: enables more types
		/*
		case arrow::Type::TIMESTAMP: {
      auto type = static_cast<arrow::TimestampType const>(arrow_type);
      switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: return cudf::data_type(cudf::type_id::TIMESTAMP_SECONDS);
        case arrow::TimeUnit::type::MILLI: return cudf::data_type(cudf::type_id::TIMESTAMP_MILLISECONDS);
        case arrow::TimeUnit::type::MICRO: return cudf::data_type(cudf::type_id::TIMESTAMP_MICROSECONDS);
        case arrow::TimeUnit::type::NANO: return cudf::data_type(cudf::type_id::TIMESTAMP_NANOSECONDS);
        default: throw std::runtime_error("Unsupported timestamp unit in arrow");
      }
    }
    case arrow::Type::DURATION: {
      auto type = static_cast<arrow::DurationType const>(arrow_type);
      switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: return cudf::data_type(cudf::type_id::DURATION_SECONDS);
        case arrow::TimeUnit::type::MILLI: return cudf::data_type(cudf::type_id::DURATION_MILLISECONDS);
        case arrow::TimeUnit::type::MICRO: return cudf::data_type(cudf::type_id::DURATION_MICROSECONDS);
        case arrow::TimeUnit::type::NANO: return cudf::data_type(cudf::type_id::DURATION_NANOSECONDS);
        default: throw std::runtime_error("Unsupported duration unit in arrow");
      }
    }
    case arrow::Type::DECIMAL: {
      auto const type = static_cast<arrow::Decimal128Type const>(arrow_type);
      return data_type{cudf::type_id::DECIMAL64, -type->scale()};
    }
	*/
	default: throw std::runtime_error("Unsupported cudf::type_id conversion to cudf");
    }
}

cudf::data_type arrow_type_to_cudf_data_type_cudf(std::shared_ptr<arrow::DataType> arrow_type) {
  return arrow_type_to_cudf_data_type_cudf(arrow_type->id());
}

std::basic_string<char> get_typed_vector_content(cudf::type_id dtype, std::vector<int64_t> &vector) {
  std::basic_string<char> output;
  switch (dtype) {
	case cudf::type_id::INT8:{
		std::vector<char> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(char));
		break;
	}
	case cudf::type_id::UINT8:{
		std::vector<uint8_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(uint8_t));
		break;
	}
	case cudf::type_id::INT16: {
		std::vector<int16_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int16_t));
		break;
	}
	case cudf::type_id::UINT16:{
		std::vector<uint16_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(uint16_t));
		break;
	}
	case cudf::type_id::INT32:{
		std::vector<int32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int32_t));
		break;
	}
	case cudf::type_id::UINT32:{
		std::vector<uint32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(uint32_t));
		break;
	}
	case cudf::type_id::INT64: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::UINT64: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(uint64_t));
		break;
	}
	case cudf::type_id::FLOAT32: {
		std::vector<float> typed_v(vector.size());
		for(size_t I=0;I<vector.size();I++){
			typed_v[I] = *(reinterpret_cast<float*>(&(vector[I])));
		}
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(float));
		break;
	}
	case cudf::type_id::FLOAT64: {
		double* casted_metadata = reinterpret_cast<double*>(&(vector[0]));
		output = std::basic_string<char>((char *)casted_metadata, vector.size() * sizeof(double));
		break;
	}
	case cudf::type_id::BOOL8: {
		std::vector<int8_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int8_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_DAYS: {
		std::vector<int32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int32_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_SECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_MILLISECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_MICROSECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_NANOSECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	default: {
		// default return type since we're throwing an exception.
		std::cerr << "Invalid gdf_dtype in create_host_column" << std::endl;
		throw std::runtime_error("Invalid gdf_dtype in create_host_column");
	}
  }
  return output;
}

std::unique_ptr<cudf::column> make_cudf_column_from_vector(cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size) {
	size_t width_per_value = cudf::size_of(dtype);
	if (vector.size() != 0) {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(vector.data(), buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, std::move(gpu_buffer));
	} else {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, buffer_size);
	}
}

} // namespace io
} // namespace cudf_backend
} // namespace compute
} // namespace voltron
