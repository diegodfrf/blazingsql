#pragma once

#include <string>
#include <vector>
#include <arrow/type.h>
#include "operators/operators_definitions.h"

bool is_type_float(std::shared_ptr<arrow::DataType> type);
bool is_type_integer(std::shared_ptr<arrow::DataType> type);
bool is_type_bool(std::shared_ptr<arrow::DataType> type);
bool is_type_timestamp(std::shared_ptr<arrow::DataType> type);
bool is_type_duration(std::shared_ptr<arrow::DataType> type) ;
bool is_type_string(std::shared_ptr<arrow::DataType> type);

int get_index(const std::string & operand_string);

// interprets the expression and if is n-ary and logical, then returns their corresponding binary version
std::string expand_if_logical_op(std::string expression);

std::string clean_calcite_expression(const std::string & expression);

std::vector<std::string> get_tokens_in_reverse_order(const std::string & expression);

std::string get_aggregation_operation_string(std::string operator_expression);

std::string get_string_between_outer_parentheses(std::string operator_string);

int count_string_occurrence(std::string haystack, std::string needle);
