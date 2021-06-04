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
#include "compute/backend_dispatcher.h"
#include "compute/api.h"


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


namespace ral {
namespace utilities {












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
