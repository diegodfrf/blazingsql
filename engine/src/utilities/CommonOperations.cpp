#include "utilities/CommonOperations.h"

#include "error.hpp"

#include "parser/CalciteExpressionParsing.h"
#include <cudf/filling.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/copying.hpp>
#include <cudf/unary.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <numeric>
#include "blazing_table/BlazingColumnOwner.h"
#include <cudf/detail/interop.hpp>
#include <arrow/type.h>
#include "execution_graph/backend_dispatcher.h"

namespace ral {

namespace cpu {
namespace utilities {

std::shared_ptr<arrow::Schema> build_arrow_schema(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>> &columns,
    const std::vector<std::string> &column_names,
    std::shared_ptr<const arrow::KeyValueMetadata> metadata)
{
  assert(columns.size() == column_names.size());
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.resize(columns.size());
  for (int i = 0; i < columns.size(); ++i) {
    fields[i] = arrow::field(
                  column_names[i],
                  columns[i]->type());
  }
  return arrow::schema(fields, metadata);
}

std::unique_ptr<ral::frame::BlazingTable> getLimitedRows(std::shared_ptr<arrow::Table> table, cudf::size_type num_rows, bool front){
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


void normalize_types(std::unique_ptr<ral::frame::BlazingArrowTable> & table,  const std::vector<cudf::data_type> & types,
                     std::vector<cudf::size_type> column_indices) {
  // TODO percy
}

}  // namespace utilities
}  // namespace cpu












































/////////// functors


std::pair<std::shared_ptr<arrow::Table>, std::vector<cudf::size_type>>
split_arrow(std::shared_ptr<arrow::Table> table_View,
            std::vector<cudf::size_type> const& columns_to_hash,
            int num_partitions)
{
  
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




















namespace utilities {










bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::unique_ptr<ral::frame::BlazingTable>> & tables) {
	std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables_to_concat(tables.size());
	for (std::size_t i = 0; i < tables.size(); i++){
		tables_to_concat[i] = tables[i]->to_table_view();
	}

	return checkIfConcatenatingStringsWillOverflow(tables_to_concat);
}

bool checkIfConcatenatingStringsWillOverflow_gpu(const std::vector<std::shared_ptr<ral::frame::BlazingCudfTableView>> & tables) {
	if( tables.size() == 0 ) {
		return false;
	}

	// Lets only look at tables that not empty
	std::vector<size_t> non_empty_index;
	for(size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
		if (tables[table_idx]->num_columns() > 0){
			non_empty_index.push_back(table_idx);
		}
	}
	if( non_empty_index.size() == 0 ) {
		return false;
	}

	for(size_t col_idx = 0; col_idx < tables[non_empty_index[0]]->column_types().size(); col_idx++) {
		if(tables[non_empty_index[0]]->column_types()[col_idx].id() == cudf::type_id::STRING) {
			std::size_t total_bytes_size = 0;
			std::size_t total_offset_count = 0;

			for(size_t i = 0; i < non_empty_index.size(); i++) {
				size_t table_idx = non_empty_index[i];

				// Column i-th from the next tables are expected to have the same string data type
				assert( tables[table_idx]->column_types()[col_idx].id() == cudf::type_id::STRING );

				auto & column = tables[table_idx]->column(col_idx);
				auto num_children = column.num_children();
				if(num_children == 2) {

					auto offsets_column = column.child(0);
					auto chars_column = column.child(1);

					// Similarly to cudf, we focus only on the byte number of chars and the offsets count
					total_bytes_size += chars_column.size();
					total_offset_count += offsets_column.size() + 1;

					if( total_bytes_size > static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max()) ||
						total_offset_count > static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max())) {
						return true;
					}
				}
			}
		}
	}

	return false;
}

bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) {
  return ral::execution::backend_dispatcher(tables[0]->get_execution_backend(), checkIfConcatenatingStringsWillOverflow_functor(),
      tables);
}

// TODO percy arrow here we need to think about how we want to manage hybrid tables
// for now we will concat either cudf tables or arrow tables
// for now we are just validating that they are all the same type
std::unique_ptr<ral::frame::BlazingTable> concatTables(const std::vector<std::shared_ptr<BlazingTableView>> & tables) {
	assert(tables.size() >= 0);

	std::vector<std::string> names;
	std::vector<std::shared_ptr<BlazingTableView>> table_views_to_concat;
	execution::execution_backend common_backend = tables.size() > 0 ? tables[0]->get_execution_backend() : execution::execution_backend(execution::backend_id::NONE);
	for(size_t i = 0; i < tables.size(); i++) {
		if (tables[i]->column_names().size() > 0){ // lets make sure we get the names from a table that is not empty
			names = tables[i]->column_names();
      		table_views_to_concat.push_back(tables[i]);

			RAL_EXPECTS(tables[i]->get_execution_backend() == common_backend, "Concatenating tables have different backends");
		}
	}
	// TODO want to integrate data type normalization.
	// Data type normalization means that only some columns from a table would get normalized,
	// so we would need to manage the lifecycle of only a new columns that get allocated

	size_t empty_count = 0;
  
  for(size_t i = 0; i < table_views_to_concat.size(); i++) {
    if (table_views_to_concat[i]->num_rows() == 0){
      ++empty_count;
    }
  }
  
  auto out = ral::execution::backend_dispatcher(common_backend, concat_functor(),
      table_views_to_concat, empty_count, names);
	  return out;
}

std::unique_ptr<BlazingTable> getLimitedRows(std::shared_ptr<BlazingTableView> table, cudf::size_type num_rows, bool front){
	if (num_rows == 0) {
    return ral::execution::backend_dispatcher(
          table->get_execution_backend(),
          create_empty_table_like_functor(),
          table);
	} else if (num_rows < table->num_rows()) {
		std::unique_ptr<ral::frame::BlazingTable> cudf_table;
		if (front){
			std::vector<cudf::size_type> splits = {num_rows};
			auto split_table = ral::execution::backend_dispatcher(table->get_execution_backend(), split_functor(), table, splits);
			cudf_table = ral::execution::backend_dispatcher(split_table[0]->get_execution_backend(), from_table_view_to_table_functor(), split_table[0]);
		} else { // back
			std::vector<cudf::size_type> splits;
      splits.push_back(table->num_rows() - num_rows);
      auto split_table = ral::execution::backend_dispatcher(table->get_execution_backend(), split_functor(), table, splits);
      cudf_table = ral::execution::backend_dispatcher(split_table[1]->get_execution_backend(), from_table_view_to_table_functor(), split_table[1]);
		}
		return cudf_table;
	} else {
    return ral::execution::backend_dispatcher(table->get_execution_backend(), from_table_view_to_table_functor(), table);
	}
}

std::unique_ptr<ral::frame::BlazingCudfTable> create_empty_cudf_table(const std::vector<std::string> &column_names,
	const std::vector<cudf::data_type> &dtypes, std::vector<size_t> column_indices) {

	if (column_indices.size() == 0){
		column_indices.resize(column_names.size());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	std::vector<std::unique_ptr<cudf::column>> columns(column_indices.size());

	for (auto idx : column_indices) {
		columns[idx] = cudf::make_empty_column(dtypes[idx]);
	}
	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(table), column_names);
}

std::unique_ptr<cudf::table> create_empty_cudf_table(const std::vector<cudf::type_id> &dtypes) {
	std::vector<std::unique_ptr<cudf::column>> columns(dtypes.size());
	for (size_t idx =0; idx < dtypes.size(); idx++) {
		columns[idx] = cudf::make_empty_column(cudf::data_type(dtypes[idx]));
	}
	return std::make_unique<cudf::table>(std::move(columns));
}

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(std::shared_ptr<BlazingTableView> table) {
  return ral::execution::backend_dispatcher(
           table->get_execution_backend(),
           create_empty_table_like_functor(),
           table);
}


std::vector<cudf::data_type> get_common_types(const std::vector<cudf::data_type> & types1, const std::vector<cudf::data_type> & types2, bool strict){
	RAL_EXPECTS(types1.size() == types2.size(), "In get_common_types: Mismatched number of columns");
	std::vector<cudf::data_type> common_types(types1.size());
	for(size_t j = 0; j < common_types.size(); j++) {
		common_types[j] = get_common_type(types1[j], types2[j], strict);
	}
	return common_types;
}


cudf::data_type get_common_type(cudf::data_type type1, cudf::data_type type2, bool strict) {
	if(type1 == type2) {
		return type1;
	} else if((is_type_float(type1.id()) && is_type_float(type2.id())) || (is_type_integer(type1.id()) && is_type_integer(type2.id()))) {
		return (cudf::size_of(type1) >= cudf::size_of(type2))	? type1	: type2;
	} else if(is_type_timestamp(type1.id()) && is_type_timestamp(type2.id())) {
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
	} else if(is_type_duration(type1.id()) && is_type_duration(type2.id())) {
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
	else if ( is_type_string(type1.id()) && is_type_timestamp(type2.id()) ) {
		return type2;
	}
	if (strict) {
		RAL_FAIL("No common type between " + std::to_string(static_cast<int32_t>(type1.id())) + " and " + std::to_string(static_cast<int32_t>(type2.id())));
	} else {
		if(is_type_float(type1.id()) && is_type_integer(type2.id())) {
			return type1;
		} else if (is_type_float(type2.id()) && is_type_integer(type1.id())) {
			return type2;
		} else if (is_type_bool(type1.id()) && (is_type_integer(type2.id()) || is_type_float(type2.id()) || is_type_string(type2.id()) )){
			return type2;
		} else if (is_type_bool(type2.id()) && (is_type_integer(type1.id()) || is_type_float(type1.id()) || is_type_string(type1.id()) )){
			return type1;
		}
	}
}

void normalize_types_gpu(std::unique_ptr<ral::frame::BlazingTable> & gpu_table,  const std::vector<cudf::data_type> & types,
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
		if (!(columns[column_indices[i]]->view().type() == types[i])){
			std::unique_ptr<cudf::column> casted = cudf::cast(columns[column_indices[i]]->view(), types[i]);
			columns[column_indices[i]] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(casted));
		}
	}
	gpu_table = std::make_unique<ral::frame::BlazingCudfTable>(std::move(columns), table->column_names());
}

void normalize_types(std::unique_ptr<ral::frame::BlazingTable> & table,  const std::vector<cudf::data_type> & types,
  std::vector<cudf::size_type> column_indices) {
  ral::execution::backend_dispatcher(
           table->get_execution_backend(),
           normalize_types_functor(),
           table, types, column_indices);
}

}  // namespace utilities
}  // namespace ral
