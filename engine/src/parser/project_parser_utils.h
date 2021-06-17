#pragma once

#include <execution_graph/Context.h>
#include <regex>

#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"

std::string like_expression_to_regex_str(const std::string & like_exp);

// Use get_projections and if there are no projections or expression is empty
// then returns a filled array with the sequence of all columns (0, 1, ..., n)
std::vector<int> get_projections_wrapper(size_t num_columns, const std::string &expression = "");

std::string get_current_date_or_timestamp(std::string expression, blazingdb::manager::Context * context);
