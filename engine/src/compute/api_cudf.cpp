#pragma once

#include "compute/api.h"

#include <cudf/aggregation.hpp>
#include <cudf/filling.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/detail/gather.hpp>
#include <cudf/merge.hpp>
#include <cudf/join.hpp>
#include <cudf/reduction.hpp>
#include <cudf/replace.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/sorting.hpp>
#include <cudf/replace.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/reduction.hpp>

#include "parser/expression_utils.hpp"
#include "parser/CalciteExpressionParsing.h"

#include "operators/GroupBy.h"

using namespace ral::operators;

/////////////////////////////////////////////////// AGGREGATIONS

#include <cudf/aggregation.hpp>
#include "Util/StringUtil.h"
#include <regex>
#include <numeric>






//////////////////////////////////////// evaluateee interops
/// 

#include "execution_kernels/LogicalProject.h"
using namespace ral::processor;

#include "blazing_table/BlazingColumn.h"
#include "blazing_table/BlazingColumnView.h"
#include "blazing_table/BlazingColumnOwner.h"
#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"
#include "Interpreter/interpreter_cpp.h"
#include <cudf/copying.hpp>
#include <cudf/replace.hpp>
#include <cudf/search.hpp>
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

#include "utilities/transform.hpp"

using namespace ral;
using namespace processor;

/**
 * @brief Evaluates multiple expressions consisting of arithmetic operations and
 * SQL functions.
 *
 * The computation of the results consist of two steps:
 * 1. We evaluate all complex operations operations one by one. Complex operations
 * are operations that can't be mapped as f(input_table[row]) => output_table[row]
 * for a given row in a table e.g. string functions
 *
 * 2. We batch all simple operations and evaluate all of them in a single GPU
 * kernel call. Simple operations are operations that can be mapped as
 * f(input_table[row]) => output_table[row] for a given row in a table e.g.
 * arithmetic operations and cast between primitive types
 */

inline std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
    const cudf::table_view & table,
    const std::vector<std::string> & expressions);

/**
 * @brief Evaluates a "CASE WHEN ELSE" when any of the result expressions is a string.
 *
 * @param table The input table
 * @param op The string function to evaluate
 * @param arg_tokens The parsed function arguments
 */
inline std::unique_ptr<cudf::column> evaluate_string_case_when_else(const cudf::table_view & table,
                                                            const std::string & condition_expr,
                                                            const std::string & expr1,
                                                            const std::string & expr2)
{
    if ((!is_string(expr1) && !is_var_column(expr1) && !is_null(expr1)) || (!is_string(expr2) && !is_var_column(expr2) && !is_null(expr2))) {
        return nullptr;
    }

    if ((is_var_column(expr1) && table.column(get_index(expr1)).type().id() != cudf::type_id::STRING)
        || (is_var_column(expr2) && table.column(get_index(expr2)).type().id() != cudf::type_id::STRING)) {
        return nullptr;
    }

    RAL_EXPECTS(!is_literal(condition_expr), "CASE operator not supported for condition expression literals");

    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table;
    cudf::column_view boolean_mask_view;
    if (is_var_column(condition_expr)) {
        boolean_mask_view = table.column(get_index(condition_expr));
    } else {
        evaluated_table = evaluate_expressions(table, {condition_expr});
        RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

        boolean_mask_view = evaluated_table[0]->view();
    }

    std::unique_ptr<cudf::column> computed_col;
    if ((is_string(expr1) || is_null(expr1)) && (is_string(expr2) || is_null(expr2))) {
        std::unique_ptr<cudf::scalar> lhs = get_scalar_from_string(expr1, cudf::data_type{cudf::type_id::STRING});
        std::unique_ptr<cudf::scalar> rhs = get_scalar_from_string(expr2, cudf::data_type{cudf::type_id::STRING});
        computed_col = cudf::copy_if_else(*lhs, *rhs, boolean_mask_view);
    } else if (is_string(expr1) || is_null(expr1)) {
        std::unique_ptr<cudf::scalar> lhs = get_scalar_from_string(expr1, cudf::data_type{cudf::type_id::STRING});
        cudf::column_view rhs = table.column(get_index(expr2));
        computed_col = cudf::copy_if_else(*lhs, rhs, boolean_mask_view);
    } else if (is_string(expr2) || is_null(expr2)) {
        cudf::column_view lhs = table.column(get_index(expr1));
        std::unique_ptr<cudf::scalar> rhs = get_scalar_from_string(expr2, cudf::data_type{cudf::type_id::STRING});
        computed_col = cudf::copy_if_else(lhs, *rhs, boolean_mask_view);
    } else {
        cudf::column_view lhs = table.column(get_index(expr1));
        cudf::column_view rhs = table.column(get_index(expr2));
        computed_col = cudf::copy_if_else(lhs, rhs, boolean_mask_view);
    }

    return computed_col;
}

/**
 * @brief Evaluates a SQL string function.
 *
 * The string function is evaluated using cudf functions
 *
 * @param table The input table
 * @param op The string function to evaluate
 * @param arg_tokens The parsed function arguments
 */
inline std::unique_ptr<cudf::column> evaluate_string_functions(const cudf::table_view & table,
                                                        operator_type op,
                                                        const std::vector<std::string> & arg_tokens)
{
    std::unique_ptr<cudf::column> computed_col;
    std::string encapsulation_character = "'";

    switch (op)
    {
    case operator_type::BLZ_STR_LIKE:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "LIKE operator not supported for string literals");

        std::unique_ptr<cudf::column> computed_column;
        cudf::column_view column;
        if (is_var_column(arg_tokens[0])) {
            column = table.column(get_index(arg_tokens[0]));
        } else {
            auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
            assert(evaluated_col.size() == 1);
            computed_column = evaluated_col[0]->release();
            column = computed_column->view();
        }

        std::string literal_expression = StringUtil::removeEncapsulation(arg_tokens[1], encapsulation_character);
        std::string regex = like_expression_to_regex_str(literal_expression);

        computed_col = cudf::strings::contains_re(column, regex);
        break;
    }
    case operator_type::BLZ_STR_REPLACE:
    {
        // required args: string column, search, replacement
        assert(arg_tokens.size() == 3);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "REPLACE function not supported for string literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "REPLACE argument must be a column of type string");

        std::string target = StringUtil::removeEncapsulation(arg_tokens[1], encapsulation_character);
        std::string repl = StringUtil::removeEncapsulation(arg_tokens[2], encapsulation_character);

        computed_col = cudf::strings::replace(column, target, repl);
        break;
    }
    case operator_type::BLZ_STR_REGEXP_REPLACE:
    {
        // required args: string column, pattern, replacement
        // optional args: position, occurrence, match_type
        assert(arg_tokens.size() >= 3 && arg_tokens.size() <= 6);
        RAL_EXPECTS(arg_tokens.size() <= 4, "Optional parameters occurrence and match_type are not yet supported.");
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "REGEXP_REPLACE function not supported for string literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "REGEXP_REPLACE argument must be a column of type string");

        std::string pattern = StringUtil::removeEncapsulation(arg_tokens[1], encapsulation_character);
        std::string repl = StringUtil::removeEncapsulation(arg_tokens[2], encapsulation_character);

        // handle the position argument, if it exists
        if (arg_tokens.size() == 4) {
            int32_t start = std::stoi(arg_tokens[3]) - 1;
            RAL_EXPECTS(start >= 0, "Position must be greater than zero.");
            int32_t prefix = 0;

            auto prefix_col = cudf::strings::slice_strings(column, prefix, start);
            auto post_replace_col = cudf::strings::replace_with_backrefs(
                cudf::column_view(cudf::strings::slice_strings(column, start)->view()),
                pattern,
                repl
            );

            computed_col = cudf::strings::concatenate(
                cudf::table_view{{prefix_col->view(), post_replace_col->view()}}
            );
        } else {
            computed_col = cudf::strings::replace_with_backrefs(column, pattern, repl);
        }
        break;
    }
    case operator_type::BLZ_STR_LEFT:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "LEFT function not supported for string literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "LEFT argument must be a column of type string");

        int32_t end = std::max(std::stoi(arg_tokens[1]), 0);
        computed_col = cudf::strings::slice_strings(
            column,
            cudf::numeric_scalar<int32_t>(0, true),
            cudf::numeric_scalar<int32_t>(end, true)
        );
        break;
    }
    case operator_type::BLZ_STR_RIGHT:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "RIGHT function not supported for string literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "RIGHT argument must be a column of type string");

        int32_t offset = std::max(std::stoi(arg_tokens[1]), 0);
        computed_col = cudf::strings::slice_strings(column, -offset, cudf::numeric_scalar<int32_t>(0, offset < 1));
        break;
    }
    case operator_type::BLZ_STR_SUBSTRING:
    {
        assert(arg_tokens.size() == 2 || arg_tokens.size() == 3);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "SUBSTRING function not supported for string literals");

        if (is_var_column(arg_tokens[0]) && is_literal(arg_tokens[1]) && (arg_tokens.size() == 3 ? is_literal(arg_tokens[2]) : true)) {
            cudf::column_view column = table.column(get_index(arg_tokens[0]));
            int32_t start = std::max(std::stoi(arg_tokens[1]), 1) - 1;
            int32_t length = arg_tokens.size() == 3 ? std::stoi(arg_tokens[2]) : -1;
            int32_t end = length >= 0 ? start + length : 0;

            computed_col = cudf::strings::slice_strings(column, start, cudf::numeric_scalar<int32_t>(end, length >= 0));
        } else {
            // TODO: create a version of cudf::strings::slice_strings that uses start and length columns
            // so we can remove all the calculations for start and end

            std::unique_ptr<cudf::column> computed_string_column;
            cudf::column_view column;
            if (is_var_column(arg_tokens[0])) {
                column = table.column(get_index(arg_tokens[0]));
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
                RAL_EXPECTS(evaluated_col.size() == 1 && evaluated_col[0]->view().type().id() == cudf::type_id::STRING, "Expression does not evaluate to a string column");

                computed_string_column = evaluated_col[0]->release();
                column = computed_string_column->view();
            }

            std::unique_ptr<cudf::column> computed_start_column;
            cudf::column_view start_column;
            if (is_var_column(arg_tokens[1])) {
                computed_start_column = std::make_unique<cudf::column>(table.column(get_index(arg_tokens[1])));
            } else if(is_literal(arg_tokens[1])) {
                int32_t start = std::max(std::stoi(arg_tokens[1]), 1);

                cudf::numeric_scalar<int32_t> start_scalar(start);
                computed_start_column = cudf::make_column_from_scalar(start_scalar, table.num_rows());
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[1]});
                RAL_EXPECTS(evaluated_col.size() == 1 && is_type_integer(evaluated_col[0]->view().type().id()), "Expression does not evaluate to an integer column");

                computed_start_column = evaluated_col[0]->release();
            }
            cudf::mutable_column_view mutable_view = computed_start_column->mutable_view();
            ral::utilities::transform_start_to_zero_based_indexing(mutable_view);
            start_column = computed_start_column->view();

            std::unique_ptr<cudf::column> computed_end_column;
            cudf::column_view end_column;
            if (arg_tokens.size() == 3) {
                if (is_var_column(arg_tokens[2])) {
                    computed_end_column = std::make_unique<cudf::column>(table.column(get_index(arg_tokens[2])));
                } else if(is_literal(arg_tokens[2])) {
                    std::unique_ptr<cudf::scalar> end_scalar = get_scalar_from_string(arg_tokens[2], start_column.type());
                    computed_end_column = cudf::make_column_from_scalar(*end_scalar, table.num_rows());
                } else {
                    auto evaluated_col = evaluate_expressions(table, {arg_tokens[2]});
                    RAL_EXPECTS(evaluated_col.size() == 1 && is_type_integer(evaluated_col[0]->view().type().id()), "Expression does not evaluate to an integer column");

                    computed_end_column = evaluated_col[0]->release();
                }

                // lets make sure that the start and end are the same type
                if (!(start_column.type() == computed_end_column->type())){
                    cudf::data_type common_type = ral::utilities::get_common_type(start_column.type(), computed_end_column->type(), true);
                    if (!(start_column.type() == common_type)){
                        computed_start_column = cudf::cast(start_column, common_type);
                        start_column = computed_start_column->view();
                    }
                    if (!(computed_end_column->type() == common_type)){
                        computed_end_column = cudf::cast(computed_end_column->view(), common_type);
                    }
                }
                cudf::mutable_column_view mutable_view = computed_end_column->mutable_view();
                ral::utilities::transform_length_to_end(mutable_view, start_column);
                end_column = computed_end_column->view();
            } else {
                std::unique_ptr<cudf::scalar> end_scalar = get_max_integer_scalar(start_column.type());
                computed_end_column = cudf::make_column_from_scalar(*end_scalar, table.num_rows());
                end_column = computed_end_column->view();
            }
            std::unique_ptr<cudf::column> start_temp = nullptr;
            std::unique_ptr<cudf::column> end_temp = nullptr;
            if (start_column.has_nulls()) {
              cudf::numeric_scalar<int32_t> start_zero(0);
              start_temp = cudf::replace_nulls(start_column, start_zero);
              start_column = start_temp->view();
            }
            if (end_column.has_nulls()) {
              cudf::numeric_scalar<int32_t> end_zero(0);
              end_temp = cudf::replace_nulls(end_column, end_zero);            
              end_column = end_temp->view();
            }
            computed_col = cudf::strings::slice_strings(column, start_column, end_column);
        }
        break;
    }
    case operator_type::BLZ_STR_CONCAT:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(!(is_string(arg_tokens[0]) && is_string(arg_tokens[1])), "CONCAT operator between literals is not supported");

        if (is_var_column(arg_tokens[0]) && is_var_column(arg_tokens[1])) {
            cudf::column_view column1 = table.column(get_index(arg_tokens[0]));
            cudf::column_view column2 = table.column(get_index(arg_tokens[1]));

            computed_col = cudf::strings::concatenate(cudf::table_view{{column1, column2}});
        } else {
            std::unique_ptr<cudf::column> temp_col1;
            cudf::column_view column1;
            if (is_var_column(arg_tokens[0])) {
                column1 = table.column(get_index(arg_tokens[0]));
            } else if(is_literal(arg_tokens[0])) {
                std::string literal_str = StringUtil::removeEncapsulation(arg_tokens[0], encapsulation_character);
                cudf::string_scalar str_scalar(literal_str);
                temp_col1 = cudf::make_column_from_scalar(str_scalar, table.num_rows());
                column1 = temp_col1->view();
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
                assert(evaluated_col.size() == 1);
                temp_col1 = evaluated_col[0]->release();
                column1 = temp_col1->view();
            }

            std::unique_ptr<cudf::column> temp_col2;
            cudf::column_view column2;
            if (is_var_column(arg_tokens[1])) {
                column2 = table.column(get_index(arg_tokens[1]));
            } else if(is_literal(arg_tokens[1])) {
                std::string literal_str = StringUtil::removeEncapsulation(arg_tokens[1], encapsulation_character);
                cudf::string_scalar str_scalar(literal_str);
                temp_col2 = cudf::make_column_from_scalar(str_scalar, table.num_rows());
                column2 = temp_col2->view();
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[1]});
                assert(evaluated_col.size() == 1);
                temp_col2 = evaluated_col[0]->release();
                column2 = temp_col2->view();
            }

            computed_col = cudf::strings::concatenate(cudf::table_view{{column1, column2}});
        }
        break;
    }
    case operator_type::BLZ_CAST_VARCHAR:
    {
        assert(arg_tokens.size() == 1);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "CAST operator not supported for literals");

        std::unique_ptr<cudf::column> computed_column;
        cudf::column_view column;
        if (is_var_column(arg_tokens[0])) {
            column = table.column(get_index(arg_tokens[0]));
        } else {
            auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
            assert(evaluated_col.size() == 1);
            computed_column = evaluated_col[0]->release();
            column = computed_column->view();
        }
        if (is_type_string(column.type().id())) {
            // this should not happen, but sometimes calcite produces inefficient plans that ask to cast a string column to a "VARCHAR NOT NULL"
            computed_col = std::make_unique<cudf::column>(column);
        } else {
            computed_col = cudf::type_dispatcher(column.type(), cast_to_str_functor{}, column);
        }
        break;
    }
    case operator_type::BLZ_CAST_TINYINT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT8});
        }
        break;
    }
    case operator_type::BLZ_CAST_SMALLINT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT16});
        }
        break;
    }
    case operator_type::BLZ_CAST_INTEGER:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT32});
        }
        break;
    }
    case operator_type::BLZ_CAST_BIGINT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT64});
        }
        break;
    }
    case operator_type::BLZ_CAST_FLOAT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            computed_col = cudf::strings::to_floats(column, cudf::data_type{cudf::type_id::FLOAT32});
        }
        break;
    }
    case operator_type::BLZ_CAST_DOUBLE:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            computed_col = cudf::strings::to_floats(column, cudf::data_type{cudf::type_id::FLOAT64});
        }
        break;
    }
    case operator_type::BLZ_CAST_DATE:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}, "%Y-%m-%d");
        }
        break;
    }
    case operator_type::BLZ_CAST_TIMESTAMP_SECONDS:
    case operator_type::BLZ_CAST_TIMESTAMP_MILLISECONDS:
    case operator_type::BLZ_CAST_TIMESTAMP_MICROSECONDS:
    case operator_type::BLZ_CAST_TIMESTAMP:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (is_type_string(column.type().id())) {
            if (op == operator_type::BLZ_CAST_TIMESTAMP_SECONDS) {
                computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS}, "%Y-%m-%d %H:%M:%S");
            } else if (op == operator_type::BLZ_CAST_TIMESTAMP_MILLISECONDS) {
                computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS}, "%Y-%m-%d %H:%M:%S");
            } else if (op == operator_type::BLZ_CAST_TIMESTAMP_MICROSECONDS) {
                computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_MICROSECONDS}, "%Y-%m-%d %H:%M:%S");
            } else {
                computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS}, "%Y-%m-%d %H:%M:%S");
            }
        }
        break;
    }
    case operator_type::BLZ_TO_DATE:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(is_var_column(arg_tokens[0]) && is_string(arg_tokens[1]), "TO_DATE operator arguments must be a column and a string format");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "TO_DATE first argument must be a column of type string");

        std::string format_str = StringUtil::removeEncapsulation(arg_tokens[1], encapsulation_character);
        computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}, format_str);
        break;
    }
    case operator_type::BLZ_TO_TIMESTAMP:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(is_var_column(arg_tokens[0]) && is_string(arg_tokens[1]), "TO_TIMESTAMP operator arguments must be a column and a string format");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "TO_TIMESTAMP first argument must be a column of type string");

        std::string format_str = StringUtil::removeEncapsulation(arg_tokens[1], encapsulation_character);
        computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS}, format_str);
        break;
    }
    case operator_type::BLZ_STR_LOWER:
    {
        assert(arg_tokens.size() == 1);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "LOWER operator not supported for literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "LOWER argument must be a column of type string");

        computed_col = cudf::strings::to_lower(column);
        break;
    }
    case operator_type::BLZ_STR_UPPER:
    {
        assert(arg_tokens.size() == 1);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "UPPER operator not supported for literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "UPPER argument must be a column of type string");

        computed_col = cudf::strings::to_upper(column);
        break;
    }
    case operator_type::BLZ_STR_INITCAP:
    {
        assert(arg_tokens.size() == 1);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "INITCAP operator not supported for literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "INITCAP argument must be a column of type string");

        computed_col = cudf::strings::title(column);
        break;
    }
    case operator_type::BLZ_STR_TRIM:
    {
        assert(arg_tokens.size() == 3);
        RAL_EXPECTS(!is_literal(arg_tokens[2]), "TRIM operator not supported for literals");

        std::string trim_flag = StringUtil::removeEncapsulation(arg_tokens[0], "\"");
        std::string to_strip = StringUtil::removeEncapsulation(arg_tokens[1], encapsulation_character);
        cudf::strings::strip_type enumerated_trim_flag = map_trim_flag_to_strip_type(trim_flag);

        cudf::column_view column = table.column(get_index(arg_tokens[2]));
        RAL_EXPECTS(is_type_string(column.type().id()), "TRIM argument must be a column of type string");

        computed_col = cudf::strings::strip(column, enumerated_trim_flag, to_strip);
        break;
    }
    case operator_type::BLZ_STR_REVERSE:
    {
        assert(arg_tokens.size() == 1);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "REVERSE operator not supported for literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        RAL_EXPECTS(is_type_string(column.type().id()), "REVERSE argument must be a column of type string");

        computed_col = cudf::strings::slice_strings(
            column,
            cudf::numeric_scalar<int32_t>(0, false),
            cudf::numeric_scalar<int32_t>(0, false),
            cudf::numeric_scalar<int32_t>(-1, true)
        );
        break;
    }
    default:
        break;
    }

    return computed_col;
}

/**
 * @brief A class that traverses an expression tree and prunes nodes that
 * can't be evaluated by the interpreter.
 *
 * Any complex operation that can't be evaluated by the interpreter (e.g. string
 * functions) is evaluated here and its corresponding node replaced by a new
 * node containing the result of the operation
 */
class function_evaluator_transformer : public parser::node_transformer {
public:
    function_evaluator_transformer(const cudf::table_view & table) : table{table} {}

    inline parser::node * transform(parser::operad_node& node) override { return &node; }

    inline parser::node * transform(parser::operator_node& node) override {
        operator_type op = map_to_operator_type(node.value);

        std::unique_ptr<cudf::column> computed_col;
        std::vector<std::string> arg_tokens;
        if (op == operator_type::BLZ_FIRST_NON_MAGIC) {
            // Handle special case for CASE WHEN ELSE END operation for strings
            assert(node.children[0]->type == parser::node_type::OPERATOR);
            assert(map_to_operator_type(node.children[0]->value) == operator_type::BLZ_MAGIC_IF_NOT);

            const parser::node * magic_if_not_node = node.children[0].get();
            const parser::node * condition_node = magic_if_not_node->children[0].get();
            const parser::node * expr_node_1 = magic_if_not_node->children[1].get();
            const parser::node * expr_node_2 = node.children[1].get();

            std::string conditional_exp = parser::detail::rebuild_helper(condition_node);

            arg_tokens = {conditional_exp, expr_node_1->value, expr_node_2->value};
            computed_col = evaluate_string_case_when_else(cudf::table_view{{table, computed_columns_view()}}, conditional_exp, expr_node_1->value, expr_node_2->value);
        } else {
            arg_tokens.reserve(node.children.size());
            for (auto &&c : node.children) {
                arg_tokens.push_back(parser::detail::rebuild_helper(c.get()));
            }

            computed_col = evaluate_string_functions(cudf::table_view{{table, computed_columns_view()}}, op, arg_tokens);
        }

        // If computed_col is a not nullptr then the node was a complex operation and
        // we need to remove it from the tree so that only simple operations (that the
        // interpreter is able to handle) remain
        if (computed_col) {
            // Discard temp columns used in operations
            for (auto &&token : arg_tokens) {
                if (!is_var_column(token)) continue;

                cudf::size_type idx = get_index(token);
                if (idx >= table.num_columns()) {
                    computed_columns.erase(computed_columns.begin() + (idx - table.num_columns()));
                }
            }

            // Replace the operator node with its corresponding result
            std::string computed_var_token = "$" + std::to_string(table.num_columns() + computed_columns.size());
            computed_columns.push_back(std::move(computed_col));

            return new parser::variable_node(computed_var_token);
        }

        return &node;
    }

    inline cudf::table_view computed_columns_view() {
        std::vector<cudf::column_view> computed_views(computed_columns.size());
        std::transform(std::cbegin(computed_columns), std::cend(computed_columns), computed_views.begin(), [](auto & col){
            return col->view();
        });
        return cudf::table_view{computed_views};
    }

    inline std::vector<std::unique_ptr<cudf::column>> release_computed_columns() { return std::move(computed_columns); }

private:
    cudf::table_view table;
    std::vector<std::unique_ptr<cudf::column>> computed_columns;
};


inline std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
    const cudf::table_view & table,
    const std::vector<std::string> & expressions) {
    using interops::column_index_type;

    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> out_columns(expressions.size());

    std::vector<bool> column_used(table.num_columns(), false);
    std::vector<std::pair<int, int>> out_idx_computed_idx_pair;

    std::vector<parser::parse_tree> expr_tree_vector;
    std::vector<cudf::mutable_column_view> interpreter_out_column_views;

    function_evaluator_transformer evaluator{table};
    for(size_t i = 0; i < expressions.size(); i++){
        std::string expression = replace_calcite_regex(expressions[i]);
        expression = expand_if_logical_op(expression);

        parser::parse_tree tree;
        tree.build(expression);

        // Transform the expression tree so that only nodes that can be evaluated
        // by the interpreter remain
        tree.transform_to_custom_op();
        tree.transform(evaluator);

        if (tree.root().type == parser::node_type::LITERAL) {
            cudf::data_type literal_type = static_cast<const ral::parser::literal_node&>(tree.root()).type();
            std::unique_ptr<cudf::scalar> literal_scalar = get_scalar_from_string(tree.root().value, literal_type);
            out_columns[i] = std::make_unique<ral::frame::BlazingColumnOwner>(cudf::make_column_from_scalar(*literal_scalar, table.num_rows()));
        } else if (tree.root().type == parser::node_type::VARIABLE) {
            cudf::size_type idx = static_cast<const ral::parser::variable_node&>(tree.root()).index();
            if (idx < table.num_columns()) {
                out_columns[i] = std::make_unique<ral::frame::BlazingColumnOwner>(std::make_unique<cudf::column>(table.column(idx)));
            } else {
                out_idx_computed_idx_pair.push_back({i, idx - table.num_columns()});
            }
        } else {
        	expr_output_type_visitor visitor{cudf::table_view{{table, evaluator.computed_columns_view()}}};
	        tree.visit(visitor);

            cudf::data_type expr_out_type = visitor.get_expr_output_type();

            auto new_column = cudf::make_fixed_width_column(expr_out_type, table.num_rows(), cudf::mask_state::UNINITIALIZED);
            interpreter_out_column_views.push_back(new_column->mutable_view());
            out_columns[i] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(new_column));

            // Keep track of which columns are used in the expression
            for(auto&& idx : visitor.get_variable_indices()) {
                if (idx < table.num_columns()) {
                    column_used[idx] = true;
                }
            }

            expr_tree_vector.emplace_back(std::move(tree));
        }
    }

    auto computed_columns = evaluator.release_computed_columns();
    for (auto &&p : out_idx_computed_idx_pair) {
        out_columns[p.first] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(computed_columns[p.second]));
    }

    // Get the needed columns indices in order and keep track of the mapped indices
    std::map<column_index_type, column_index_type> col_idx_map;
    std::vector<cudf::size_type> input_col_indices;
    for(size_t i = 0; i < column_used.size(); i++) {
        if(column_used[i]) {
            col_idx_map.insert({i, col_idx_map.size()});
            input_col_indices.push_back(i);
        }
    }

    std::vector<std::unique_ptr<cudf::column>> filtered_computed_columns;
    std::vector<cudf::column_view> filtered_computed_views;
    for(size_t i = 0; i < computed_columns.size(); i++) {
        if(computed_columns[i]) {
            // If computed_columns[i] has not been moved to out_columns
            // then it will be used as input in interops
            col_idx_map.insert({table.num_columns() + i, col_idx_map.size()});
            filtered_computed_views.push_back(computed_columns[i]->view());
            filtered_computed_columns.push_back(std::move(computed_columns[i]));
        }
    }

    cudf::table_view interops_input_table{{table.select(input_col_indices), cudf::table_view{filtered_computed_views}}};

    std::vector<column_index_type> left_inputs;
    std::vector<column_index_type> right_inputs;
    std::vector<column_index_type> outputs;
    std::vector<column_index_type> final_output_positions;
    std::vector<operator_type> operators;
    std::vector<std::unique_ptr<cudf::scalar>> left_scalars;
    std::vector<std::unique_ptr<cudf::scalar>> right_scalars;

    for (size_t i = 0; i < expr_tree_vector.size(); i++) {
        final_output_positions.push_back(interops_input_table.num_columns() + i);

        interops::add_expression_to_interpreter_plan(expr_tree_vector[i],
                                                    col_idx_map,
                                                    interops_input_table.num_columns() + interpreter_out_column_views.size(),
                                                    interops_input_table.num_columns() + i,
                                                    left_inputs,
                                                    right_inputs,
                                                    outputs,
                                                    operators,
                                                    left_scalars,
                                                    right_scalars);
    }

    // TODO: Find a proper solution for plan with input or output index greater than 63
	auto max_left_it = std::max_element(left_inputs.begin(), left_inputs.end());
	auto max_right_it = std::max_element(right_inputs.begin(), right_inputs.end());
	auto max_out_it = std::max_element(outputs.begin(), outputs.end());
    if (!expr_tree_vector.empty() && std::max(std::max(*max_left_it, *max_right_it), *max_out_it) >= 64) {
        out_columns.clear();
        computed_columns.clear();

        size_t const half_size = expressions.size() / 2;
        std::vector<std::string> split_lo(expressions.begin(), expressions.begin() + half_size);
        std::vector<std::string> split_hi(expressions.begin() + half_size, expressions.end());
        auto out_cols_lo = evaluate_expressions(table, split_lo);
        auto out_cols_hi = evaluate_expressions(table, split_hi);

        std::move(out_cols_hi.begin(), out_cols_hi.end(), std::back_inserter(out_cols_lo));
        return std::move(out_cols_lo);
    }
    // END

    if(!expr_tree_vector.empty()){
        cudf::mutable_table_view out_table_view(interpreter_out_column_views);

        interops::perform_interpreter_operation(out_table_view,
                                                interops_input_table,
                                                left_inputs,
                                                right_inputs,
                                                outputs,
                                                final_output_positions,
                                                operators,
                                                left_scalars,
                                                right_scalars,
                                                table.num_rows());
    }

    return std::move(out_columns);
}













/**
Takes a table and applies a boolean filter to it
*/
inline std::unique_ptr<ral::frame::BlazingCudfTable> applyBooleanFilter(
  std::shared_ptr<ral::frame::BlazingCudfTableView> table_view,
  const cudf::column_view & boolValues){
  auto filteredTable = cudf::apply_boolean_mask(table_view->view(),boolValues);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(filteredTable), table_view->column_names());
}

inline bool check_if_has_nulls(cudf::table_view const& input, std::vector<cudf::size_type> const& keys){
  auto keys_view = input.select(keys);
  if (keys_view.num_columns() != 0 && keys_view.num_rows() != 0 && cudf::has_nulls(keys_view)) {
      return true;
  }

  return false;
}


inline std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
	std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<int> & group_column_indices) {
  auto ctable_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
	std::unique_ptr<cudf::table> output = cudf::drop_duplicates(ctable_view->view(),
		group_column_indices,
		cudf::duplicate_keep_option::KEEP_FIRST);

	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(output), table_view->column_names());
}



inline std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
		std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, const std::vector<std::string> & aggregation_input_expressions,
		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases)
{
	std::vector<std::unique_ptr<cudf::scalar>> reductions;
	std::vector<std::string> agg_output_column_names;
	for (size_t i = 0; i < aggregation_types.size(); i++){
		if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
			std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
			auto numeric_s = static_cast< cudf::scalar_type_t<int64_t>* >(scalar.get());
			numeric_s->set_value((int64_t)(table_view->num_rows()));
			reductions.emplace_back(std::move(scalar));
		} else {
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> aggregation_input_scope_holder;
			cudf::column_view aggregation_input;
			if(is_var_column(aggregation_input_expressions[i]) || is_number(aggregation_input_expressions[i])) {
				aggregation_input = table_view->view().column(get_index(aggregation_input_expressions[i]));
			} else {
				aggregation_input_scope_holder = evaluate_expressions(table_view->view(), {aggregation_input_expressions[i]});
				aggregation_input = aggregation_input_scope_holder[0]->view();
			}

			if( aggregation_types[i] == AggregateKind::COUNT_VALID) {
				std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
				auto numeric_s = static_cast< cudf::scalar_type_t<int64_t>* >(scalar.get());
				numeric_s->set_value((int64_t)(aggregation_input.size() - aggregation_input.null_count()));
				reductions.emplace_back(std::move(scalar));
			} else {
				std::unique_ptr<cudf::aggregation> agg = makeCudfAggregation<cudf::aggregation>(aggregation_types[i]);
				cudf::type_id output_type = get_aggregation_output_type(aggregation_input.type().id(), aggregation_types[i], false);
				std::unique_ptr<cudf::scalar> reduction_out = cudf::reduce(aggregation_input, agg, cudf::data_type(output_type));
				if (aggregation_types[i] == AggregateKind::SUM0 && !reduction_out->is_valid()){ // if this aggregation was a SUM0, and it was not valid, we want it to be a valid 0 instead
					std::unique_ptr<cudf::scalar> zero_scalar = get_scalar_from_string("0", reduction_out->type()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
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
	std::vector<std::unique_ptr<cudf::column>> output_columns;
	for (size_t i = 0; i < reductions.size(); i++){
		std::unique_ptr<cudf::column> temp = cudf::make_column_from_scalar(*(reductions[i]), 1);
		output_columns.emplace_back(std::move(temp));
	}
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(std::make_unique<cudf::table>(std::move(output_columns))), agg_output_column_names);
}


inline std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, const std::vector<std::string> & aggregation_input_expressions, const std::vector<AggregateKind> & aggregation_types,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices) {

	// lets get the unique expressions. This is how many aggregation requests we will need
	std::vector<std::string> unique_expressions = aggregation_input_expressions;
	std::sort( unique_expressions.begin(), unique_expressions.end() );
	auto it = std::unique( unique_expressions.begin(), unique_expressions.end() );
	unique_expressions.resize( std::distance(unique_expressions.begin(),it) );

	// We will iterate over the unique expressions and create an aggregation request for each one.
	// We do it this way, because you could have something like min(colA), max(colA), sum(colA).
	// These three aggregations would all be in one request because they have the same input
	std::vector< std::unique_ptr<ral::frame::BlazingColumn> > aggregation_inputs_scope_holder;
	std::vector<cudf::groupby::aggregation_request> requests;
	std::vector<int> agg_out_indices;
	std::vector<std::string> agg_output_column_names;
	for (size_t u = 0; u < unique_expressions.size(); u++){
		std::string expression = unique_expressions[u];

		cudf::column_view aggregation_input; // this is the input from which we will crete the aggregation request
		bool got_aggregation_input = false;
		std::vector<std::unique_ptr<cudf::aggregation>> agg_ops_for_request;
		for (size_t i = 0; i < aggregation_input_expressions.size(); i++){
			if (expression == aggregation_input_expressions[i]){

				int column_index = -1;
				// need to calculate or determine the aggregation input only once
				if (!got_aggregation_input) {
					if(expression == "" && aggregation_types[i] == AggregateKind::COUNT_ALL ) { // this is COUNT(*). Lets just pick the first column
						aggregation_input = table_view->view().column(0);
					} else if(is_var_column(expression) || is_number(expression)) {
						column_index = get_index(expression);
						aggregation_input = table_view->view().column(column_index);
					} else {
						std::vector< std::unique_ptr<ral::frame::BlazingColumn> > computed_columns = evaluate_expressions(table_view->view(), {expression});
						aggregation_inputs_scope_holder.insert(aggregation_inputs_scope_holder.end(), std::make_move_iterator(computed_columns.begin()), std::make_move_iterator(computed_columns.end()));
						aggregation_input = aggregation_inputs_scope_holder.back()->view();
					}
					got_aggregation_input = true;
				}
				agg_ops_for_request.push_back(makeCudfAggregation<cudf::aggregation>(aggregation_types[i]));
				agg_out_indices.push_back(i);  // this is to know what is the desired order of aggregations output

				// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
				if(aggregation_column_assigned_aliases[i] == "") {
					if(aggregation_types[i] == AggregateKind::COUNT_ALL) {  // COUNT(*) case
						agg_output_column_names.push_back("COUNT(*)");
					} else {
						if (column_index == -1){
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + expression + ")");
						} else {
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table_view->column_names().at(column_index) + ")");
						}
					}
				} else {
					agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
				}
			}
		}
			requests.push_back(cudf::groupby::aggregation_request {.values = aggregation_input, .aggregations = std::move(agg_ops_for_request)});
	}

	cudf::table_view keys = table_view->view().select(group_column_indices);
	cudf::groupby::groupby group_by_obj(keys, cudf::null_policy::INCLUDE);
	std::pair<std::unique_ptr<cudf::table>, std::vector<cudf::groupby::aggregation_result>> result = group_by_obj.aggregate( requests );

	// output table is grouped columns and then aggregated columns
	std::vector< std::unique_ptr<cudf::column> > output_columns = result.first->release();
	output_columns.resize(agg_out_indices.size() + group_column_indices.size());

	// lets collect all the aggregated results from the results structure and then add them to output_columns
	std::vector< std::unique_ptr<cudf::column> > agg_cols_out;
	for (size_t i = 0; i < result.second.size(); i++){
		for (size_t j = 0; j < result.second[i].results.size(); j++){
			agg_cols_out.emplace_back(std::move(result.second[i].results[j]));
		}
	}
	for (size_t i = 0; i < agg_out_indices.size(); i++){
		if (aggregation_types[agg_out_indices[i]] == AggregateKind::SUM0 && agg_cols_out[i]->null_count() > 0){
			std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string("0", agg_cols_out[i]->type()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
			std::unique_ptr<cudf::column> temp = cudf::replace_nulls(agg_cols_out[i]->view(), *scalar );
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(temp);
		} else {
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(agg_cols_out[i]);
		}
	}
	std::unique_ptr<cudf::table> output_table = std::make_unique<cudf::table>(std::move(output_columns));

	// lets put together the output names
	std::vector<std::string> output_names;
	for (size_t i = 0; i < group_column_indices.size(); i++){
		output_names.push_back(table_view->column_names()[group_column_indices[i]]);
	}
	output_names.resize(agg_out_indices.size() + group_column_indices.size());
	for (size_t i = 0; i < agg_out_indices.size(); i++){
		output_names[agg_out_indices[i] + group_column_indices.size()] = agg_output_column_names[i];
	}
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(output_table), output_names);
}




















///////////////////////////////jopinnnnnn joinnn
/// 
inline std::unique_ptr<cudf::table> reordering_columns_due_to_right_join(std::unique_ptr<cudf::table> table_ptr, size_t n_right_columns) {
	std::vector<std::unique_ptr<cudf::column>> columns_ptr = table_ptr->release();
	std::vector<std::unique_ptr<cudf::column>> columns_right_pos;

	// First let's put all the left columns
	for (size_t index = n_right_columns; index < columns_ptr.size(); ++index) {
		columns_right_pos.push_back(std::move(columns_ptr[index]));
	}

	// Now let's append right columns
	for (size_t index = 0; index < n_right_columns; ++index) {
		columns_right_pos.push_back(std::move(columns_ptr[index]));
	}

	return std::make_unique<cudf::table>(std::move(columns_right_pos));
}


//namespace voltron {
//namespace compute {

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_merger_functor::operator()<ral::frame::BlazingCudfTable>(
		std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables,
		const std::vector<cudf::order> & sortOrderTypes,
		const std::vector<int> & sortColIndices, const std::vector<cudf::null_order> & sortOrderNulls) const
{
	std::vector<cudf::table_view> cudf_table_views(tables.size());
	for(size_t i = 0; i < tables.size(); i++) {
		cudf_table_views[i] = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tables[i])->view();
	}
	std::unique_ptr<cudf::table> merged_table = cudf::merge(cudf_table_views, sortColIndices, sortOrderTypes, sortOrderNulls);

	// lets get names from a non-empty table
	std::vector<std::string> names;
	for(size_t i = 0; i < tables.size(); i++) {
		if (tables[i]->column_names().size() > 0){
			names = tables[i]->column_names();
			break;
		}
	}
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(merged_table), names);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> gather_functor::operator()<ral::frame::BlazingCudfTable>(
		std::shared_ptr<ral::frame::BlazingTableView> table,
		std::unique_ptr<cudf::column> column,
		cudf::out_of_bounds_policy out_of_bounds_policy,
		cudf::detail::negative_index_policy negative_index_policy) const
{
	// TODO percy rommel arrow
	ral::frame::BlazingCudfTableView *table_ptr = dynamic_cast<ral::frame::BlazingCudfTableView*>(table.get());
	std::unique_ptr<cudf::table> pivots = cudf::detail::gather(table_ptr->view(), column->view(), out_of_bounds_policy, negative_index_policy);

	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(pivots), table->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<int> group_column_indices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return compute_groupby_without_aggregations(cudf_table_view, group_column_indices);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> aggregations_without_groupby_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return compute_aggregations_without_groupby(cudf_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases,
    std::vector<int> group_column_indices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return compute_aggregations_with_groupby(cudf_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases, group_column_indices);
}

template<typename T>
inline std::vector<T> merge_vectors(std::vector<T> first, std::vector<T> second){
	std::vector<T> merged(first);
	merged.insert(merged.end(), second.begin(), second.end());
	return merged;
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> cross_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::cross_join(table_left->view(), table_right->view()), names);
}

template <>
inline bool check_if_has_nulls_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, std::vector<cudf::size_type> const& keys) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return check_if_has_nulls(cudf_table_view->view(), keys);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> inner_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices,
    cudf::null_equality equalityType) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  auto tb = cudf::inner_join(
              table_left->view(),
              table_right->view(),
              left_column_indices,
              right_column_indices,
              equalityType);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb), names);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> drop_nulls_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<cudf::size_type> const& keys) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::drop_nulls(cudf_table_view->view(), keys), table_view->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> left_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  auto tb = cudf::left_join(
              table_left->view(),
              table_right->view(),
              left_column_indices,
              right_column_indices);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb), names);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> full_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> left,
    std::shared_ptr<ral::frame::BlazingTableView> right,
    bool has_nulls_left,
    bool has_nulls_right,
    std::vector<cudf::size_type> const& left_column_indices,
    std::vector<cudf::size_type> const& right_column_indices) const
{
  auto table_left = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(left);
  auto table_right = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(right);
  std::vector<std::string> names = merge_vectors(table_left->column_names(), table_right->column_names());
  auto tb = cudf::full_join(
              table_left->view(),
              table_right->view(),
              left_column_indices,
              right_column_indices,
              (has_nulls_left && has_nulls_right) ? cudf::null_equality::UNEQUAL : cudf::null_equality::EQUAL);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb), names);
}

template<>
inline std::unique_ptr<ral::frame::BlazingTable> reordering_columns_due_to_right_join_functor::operator()<ral::frame::BlazingCudfTable>(
    std::unique_ptr<ral::frame::BlazingTable> table_ptr, size_t right_columns) const
{
  ral::frame::BlazingCudfTable* result_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(table_ptr.get());
  return std::make_unique<ral::frame::BlazingCudfTable>(reordering_columns_due_to_right_join(result_table_ptr->releaseCudfTable(), right_columns), 
                                                        table_ptr->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_wo_filter_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table, const std::vector<std::string> & expressions,
  const std::vector<std::string> column_names) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table);
    return std::make_unique<ral::frame::BlazingCudfTable>(evaluate_expressions(cudf_table_view->view(), expressions), column_names);
}

//template <>
//std::unique_ptr<ral::frame::BlazingTable> process_project_functor::operator()<ral::frame::BlazingCudfTable>(
//  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
//  const std::vector<std::string> & expressions,
//  const std::vector<std::string> & out_column_names) const
//{
//  auto blazing_table_in_cudf = dynamic_cast<ral::frame::BlazingCudfTable*>(blazing_table_in.get());
//  std::unique_ptr<ral::frame::BlazingTable> evaluated_table = ral::execution::backend_dispatcher(
//    blazing_table_in->get_execution_backend(),
//    evaluate_expressions_wo_filter_functor(),
//    blazing_table_in_cudf->view(), expressions, out_column_names);
  
//  //auto evaluated_table_ptr = dynamic_cast<ral::frame::BlazingCudfTable*>(evaluated_table.get());
//  //return std::make_unique<ral::frame::BlazingCudfTable>(evaluated_table_ptr->view(), out_column_names);
//  return evaluated_table;
//}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> build_only_schema::operator()<ral::frame::BlazingCudfTableView>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto *table_view_ptr = dynamic_cast<ral::frame::BlazingCudfTableView*>(table_view.get());
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf::empty_like(table_view_ptr->view()), table_view_ptr->column_names());
}


template <>
inline std::unique_ptr<ral::frame::BlazingTable> evaluate_expressions_functor::operator()<ral::frame::BlazingCudfTable>(
  std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::vector<std::string> & expressions) const
{
    auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(cudf_table_view->view(), expressions);
    RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");
    return applyBooleanFilter(cudf_table_view, evaluated_table[0]->view());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> sorted_order_gather_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
    const std::vector<cudf::order> & sortOrderTypes,
    std::vector<cudf::null_order> null_orders) const
{
  auto table = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  auto sortColumns = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(sortColumns_view);
  std::unique_ptr<cudf::column> output = cudf::sorted_order( sortColumns->view(), sortOrderTypes, null_orders );
	std::unique_ptr<cudf::table> gathered = cudf::gather( table->view(), output->view() );
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(gathered), table->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_like_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  std::unique_ptr<cudf::table> empty = cudf::empty_like(cudf_table_view->view());
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(empty), cudf_table_view->column_names());
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> create_empty_table_functor::operator()<ral::frame::BlazingCudfTable>(
    const std::vector<std::string> &column_names,
	  const std::vector<cudf::data_type> &dtypes) const
{
  return ral::utilities::create_empty_cudf_table(column_names, dtypes);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> from_table_view_to_table_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return std::make_unique<ral::frame::BlazingCudfTable>(cudf_table_view->view(), cudf_table_view->column_names());
}


template <>
inline std::unique_ptr<ral::frame::BlazingTable> sample_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    cudf::size_type const num_samples,
    std::vector<std::string> sortColNames,
    std::vector<int> sortColIndices) const
{
  std::random_device rd;
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  auto samples = cudf::sample(cudf_table_view->view().select(sortColIndices), num_samples, cudf::sample_with_replacement::FALSE, rd());
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(samples), sortColNames);
}

template <>
inline bool checkIfConcatenatingStringsWillOverflow_functor::operator()<ral::frame::BlazingCudfTable>(
    const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) const
{
  std::vector<std::shared_ptr<ral::frame::BlazingCudfTableView>> in;
  for (auto tv : tables) {
    in.push_back(std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tv));
  }
  return ral::utilities::checkIfConcatenatingStringsWillOverflow_gpu(in);
}

template <>
inline std::unique_ptr<ral::frame::BlazingTable> concat_functor::operator()<ral::frame::BlazingCudfTable>(
    std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views,
    size_t empty_count,
    std::vector<std::string> names) const
{
  std::vector<cudf::table_view> table_views_to_concat;
  for (auto tv : table_views) {
    table_views_to_concat.push_back(std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(tv)->view());
  }
  if (empty_count == table_views_to_concat.size()) {
    return std::make_unique<ral::frame::BlazingCudfTable>(table_views_to_concat[0], names);
  }
  std::unique_ptr<cudf::table> concatenated_tables = cudf::concatenate(table_views_to_concat);
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(concatenated_tables), names);
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

template <>
inline void
normalize_types_functor::operator()<ral::frame::BlazingCudfTable>(
    std::unique_ptr<ral::frame::BlazingTable> & table,
    const std::vector<cudf::data_type> & types,
    std::vector<cudf::size_type> column_indices) const
{
  ral::utilities::normalize_types_gpu(table, types, column_indices);
}

template <>
inline std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::vector<cudf::size_type>>
hash_partition_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_View,
    std::vector<cudf::size_type> const& columns_to_hash,
    int num_partitions) const
{
  auto batch_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_View);
  auto tb = cudf::hash_partition(batch_view->view(), columns_to_hash, num_partitions);
  return std::make_pair(std::make_unique<ral::frame::BlazingCudfTable>(std::move(tb.first), table_View->column_names()), tb.second);
}


template <>
inline std::shared_ptr<ral::frame::BlazingTableView> select_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    const std::vector<int> & sortColIndices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return std::make_shared<ral::frame::BlazingCudfTableView>(cudf_table_view->view().select(sortColIndices),
                                                            cudf_table_view->column_names());
}

template <>
inline std::vector<std::shared_ptr<ral::frame::BlazingTableView>>
upper_bound_split_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> sortedTable_view,
    std::shared_ptr<ral::frame::BlazingTableView> t,
    std::shared_ptr<ral::frame::BlazingTableView> values,
    std::vector<cudf::order> const& column_order,
    std::vector<cudf::null_order> const& null_precedence) const
{
  auto sortedTable = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(sortedTable_view);
  auto columns_to_search = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(t);  
  auto partitionPlan = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(values);  

  auto pivot_indexes = cudf::upper_bound(columns_to_search->view(), partitionPlan->view(), column_order, null_precedence);
	std::vector<cudf::size_type> split_indexes = ral::utilities::column_to_vector<cudf::size_type>(pivot_indexes->view());
  auto tbs = cudf::split(sortedTable->view(), split_indexes);
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> ret;
  for (auto tb : tbs) {
    ret.push_back(std::make_shared<ral::frame::BlazingCudfTableView>(tb, sortedTable_view->column_names()));
  }
  return ret;
}

//} // compute
//} // voltron
