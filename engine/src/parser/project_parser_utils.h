#pragma once

#include <execution_graph/Context.h>
#include <regex>

#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"

std::string like_expression_to_regex_str(const std::string & like_exp);

#ifdef CUDF_SUPPORT
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
#endif

// Use get_projections and if there are no projections or expression is empty
// then returns a filled array with the sequence of all columns (0, 1, ..., n)
std::vector<int> get_projections_wrapper(size_t num_columns, const std::string &expression = "");

std::string get_current_date_or_timestamp(std::string expression, blazingdb::manager::Context * context);
