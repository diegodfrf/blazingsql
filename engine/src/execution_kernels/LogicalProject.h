#pragma once

#include <execution_graph/Context.h>
#include "LogicPrimitives.h"
#include "blazing_table/BlazingColumn.h"
#include "execution_graph/backend_dispatcher.h"
#include <cudf/copying.hpp>
#include "utilities/error.hpp"
#include <cudf/stream_compaction.hpp>
#include <cudf/copying.hpp>
#include <regex>

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
#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"


namespace ral{
namespace processor{




std::string like_expression_to_regex_str(const std::string & like_exp);

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

	cudf::data_type get_expr_output_type();

    const std::vector<cudf::size_type> & get_variable_indices();

private:
    cudf::data_type expr_output_type_;
    std::vector<cudf::size_type> variable_indices_;

	std::map<const ral::parser::node*, cudf::data_type> node_to_type_map_;
	cudf::table_view table_;
};
















// se vaa
std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::Context * context);


//struct evaluate_expressions_vector_functor {
//  template <typename T>
//  std::vector<std::unique_ptr<ral::frame::BlazingTable>> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view,
//  const std::vector<std::string> & expressions) const
//  {
//    // TODO percy arrow thrown error
//    return nullptr;
//  }
//};

//template <>
//std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_vector_functor::operator()<ral::frame::BlazingArrowTable>(
//  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
//{
//  ral::frame::BlazingArrowTableView *table_view_ptr = dynamic_cast<ral::frame::BlazingArrowTableView*>(table_view.get());
//  std::vector<std::shared_ptr<arrow::ChunkedArray>> evaluated_table = ral::cpu::evaluate_expressions(table_view_ptr->view(), expressions);

//  // TODO percy arrow
//  //RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

//  return ral::cpu::applyBooleanFilter(table_view_ptr->view(), evaluated_table[0]);
//}

//template <>
//std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_vector_functor::operator()<ral::frame::BlazingCudfTable>(
//  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
//{
//    auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
//    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(cudf_table_view->view(), expressions);
//    RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");
//    return applyBooleanFilter(cudf_table_view, evaluated_table[0]->view());
//}

} // namespace processor
} // namespace ral
