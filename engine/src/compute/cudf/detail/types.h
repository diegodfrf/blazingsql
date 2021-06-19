#pragma once


#include <string>
#include <vector>
#include "cudf/types.hpp"
#include <regex>
#include <vector>
#include <memory>
#include <arrow/type.h>

#include <cudf/aggregation.hpp>
#include <cudf/sorting.hpp>
#include <cudf/replace.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/reduction.hpp>
#include <cudf/strings/capitalize.hpp>
#include <cudf/strings/combine.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/replace_re.hpp>
#include <cudf/strings/replace.hpp>
#include <cudf/strings/substring.hpp>
#include <cudf/strings/case.hpp>
#include <cudf/strings/strip.hpp>
#include <cudf/strings/convert/convert_booleans.hpp>
#include <cudf/strings/convert/convert_datetime.hpp>
#include <cudf/strings/convert/convert_floats.hpp>
#include <cudf/strings/convert/convert_integers.hpp>
#include <cudf/unary.hpp>
#include <cudf/types.hpp>

#include "operators/operators_definitions.h"

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"
#include "utilities/CodeTimer.h"
#include <blazingdb/io/Util/StringUtil.h>

#include <execution_graph/Context.h>
#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"

#include "blazing_table/BlazingCudfTable.h"
#include "operators/operators_definitions.h"
#include "parser/expression_utils.hpp"

namespace voltron {
namespace compute {
namespace cudf_backend {
namespace types {

void normalize_types_gpu(std::unique_ptr<ral::frame::BlazingTable> & gpu_table, const std::vector<std::shared_ptr<arrow::DataType>> & types,
	std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );

std::unique_ptr<ral::frame::BlazingCudfTable> create_empty_cudf_table(
  const std::vector<std::string> &column_names,
  const std::vector<std::shared_ptr<arrow::DataType>> &dtypes, std::vector<int> column_indices = {});

std::unique_ptr<cudf::table> create_empty_cudf_table(const std::vector<cudf::type_id> &dtypes);

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

std::vector<cudf::order> toCudfOrderTypes(std::vector<voltron::compute::SortOrder> sortOrderTypes);

std::vector<cudf::null_order> toCudfNullOrderTypes(std::vector<voltron::compute::NullOrder> sortOrderNulls);

cudf::data_type get_common_cudf_type(cudf::data_type type1, cudf::data_type type2, bool strict);

bool is_type_float_cudf(cudf::type_id type);
bool is_type_integer_cudf(cudf::type_id type);
bool is_type_bool_cudf(cudf::type_id type);
bool is_type_timestamp_cudf(cudf::type_id type);
bool is_type_duration_cudf(cudf::type_id type) ;
bool is_type_string_cudf(cudf::type_id type);

// offset param is needed for `LAG` and `LEAD` aggs
template<typename cudf_aggregation_type_T>
std::unique_ptr<cudf_aggregation_type_T> makeCudfAggregation_cudf(voltron::compute::AggregateKind input, int offset = 0){
  if(input == voltron::compute::AggregateKind::SUM){
    return cudf::make_sum_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::MEAN){
    return cudf::make_mean_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::MIN){
    return cudf::make_min_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::MAX){
    return cudf::make_max_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::ROW_NUMBER) {
    return cudf::make_row_number_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::COUNT_VALID){
    return cudf::make_count_aggregation<cudf_aggregation_type_T>(cudf::null_policy::EXCLUDE);
  }else if(input == voltron::compute::AggregateKind::COUNT_ALL){
    return cudf::make_count_aggregation<cudf_aggregation_type_T>(cudf::null_policy::INCLUDE);
  }else if(input == voltron::compute::AggregateKind::SUM0){
    return cudf::make_sum_aggregation<cudf_aggregation_type_T>();
  }else if(input == voltron::compute::AggregateKind::LAG){
    return cudf::make_lag_aggregation<cudf_aggregation_type_T>(offset);
  }else if(input == voltron::compute::AggregateKind::LEAD){
    return cudf::make_lead_aggregation<cudf_aggregation_type_T>(offset);
  }else if(input == voltron::compute::AggregateKind::NTH_ELEMENT){
    // TODO: https://github.com/BlazingDB/blazingsql/issues/1531
    // return cudf::make_nth_element_aggregation<cudf_aggregation_type_T>(offset, cudf::null_policy::INCLUDE);
  }else if(input == voltron::compute::AggregateKind::COUNT_DISTINCT){
    /* Currently this conditional is unreachable.
    Calcite transforms count distincts through the
    AggregateExpandDistinctAggregates rule, so in fact,
    each count distinct is replaced by some group by clauses. */
    // return cudf::make_nunique_aggregation<cudf_aggregation_type_T>();
  }
  throw std::runtime_error(
      "In makeCudfAggregation function: AggregateKind type not supported");
}


cudf::type_id get_aggregation_output_type_cudf(cudf::type_id input_type, voltron::compute::AggregateKind aggregation, bool have_groupby);

struct cast_to_str_functor {
  template<typename T, std::enable_if_t<cudf::is_boolean<T>()> * = nullptr>
  inline std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
      return cudf::strings::from_booleans(col);
  }

  template<typename T, std::enable_if_t<cudf::is_fixed_point<T>()> * = nullptr>
  inline std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
      return cudf::strings::from_floats(col);
  }

  template<typename T, std::enable_if_t<std::is_integral<T>::value && !cudf::is_boolean<T>()> * = nullptr>
  inline std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
      return cudf::strings::from_integers(col);
  }

  template<typename T, std::enable_if_t<std::is_floating_point<T>::value> * = nullptr>
  inline std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
      return cudf::strings::from_floats(col);
  }

  template<typename T, std::enable_if_t<cudf::is_timestamp<T>()> * = nullptr>
  inline std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
      return cudf::strings::from_timestamps(col, std::is_same<cudf::timestamp_D, T>::value ? "%Y-%m-%d" : "%Y-%m-%d %H:%M:%S");
  }

  template<typename T, std::enable_if_t<cudf::is_compound<T>() or cudf::is_duration<T>()> * = nullptr>
  inline std::unique_ptr<cudf::column> operator()(const cudf::column_view & /*col*/) {
      return nullptr;
  }
};

cudf::strings::strip_type map_trim_flag_to_strip_type(const std::string & trim_flag);

/**
 * @brief A class that traverses an expression tree and calculates the final
 * output type of the expression.
 */
struct expr_output_type_visitor : public ral::parser::node_visitor
{
public:
	expr_output_type_visitor(const cudf::table_view & table);

	void visit(const ral::parser::operad_node& node) override;

	void visit(const ral::parser::operator_node& node) override;

  std::shared_ptr<arrow::DataType> get_expr_output_type();

    const std::vector<cudf::size_type> & get_variable_indices();

private:
  std::shared_ptr<arrow::DataType> expr_output_type_;
  std::vector<int> variable_indices_;

  std::map<const ral::parser::node*, std::shared_ptr<arrow::DataType>> node_to_type_map_;
  cudf::table_view table_;
};

std::shared_ptr<arrow::DataType>  cudf_type_id_to_arrow_type_cudf(cudf::type_id dtype);

cudf::data_type arrow_type_to_cudf_data_type_cudf(arrow::Type::type arrow_type);

cudf::data_type arrow_type_to_cudf_data_type_cudf(std::shared_ptr<arrow::DataType> arrow_type);

std::basic_string<char> get_typed_vector_content(cudf::type_id dtype, std::vector<int64_t> &vector);

std::unique_ptr<cudf::column> make_cudf_column_from_vector(cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size);

} // namespace io
} // namespace cudf_backend
} // namespace compute
} // namespace voltron
