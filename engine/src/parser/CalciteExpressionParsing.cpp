#include <algorithm>
#include <limits.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

//#include <cudf.h>
#include <cudf/table/table_view.hpp>
#include <iomanip>
#include <map>
#include <regex>

#include <blazingdb/io/Util/StringUtil.h>
#include "utilities/error.hpp"
#include "CalciteExpressionParsing.h"
#include "parser/expression_utils.hpp"
#include "parser/expression_tree.hpp"


bool is_type_float(cudf::type_id type) { return (cudf::type_id::FLOAT32 == type || cudf::type_id::FLOAT64 == type); }
bool is_type_float_arrow(arrow::Type::type type) { return (arrow::Type::type::FLOAT == type || arrow::Type::type::DOUBLE == type); }

bool is_type_integer(cudf::type_id type) {
	return (cudf::type_id::INT8 == type || cudf::type_id::INT16 == type || cudf::type_id::INT32 == type ||
			cudf::type_id::INT64 == type || cudf::type_id::UINT8 == type || cudf::type_id::UINT16 == type ||
			cudf::type_id::UINT32 == type || cudf::type_id::UINT64 == type);
}

bool is_type_integer_arrow(arrow::Type::type type) {
	return (arrow::Type::type::INT8 == type || arrow::Type::type::INT16 == type || arrow::Type::type::INT32 == type ||
			arrow::Type::type::INT64 == type || arrow::Type::type::UINT8 == type || arrow::Type::type::UINT16 == type ||
			arrow::Type::type::UINT32 == type || arrow::Type::type::UINT64 == type);
}

bool is_type_bool(cudf::type_id type) { return cudf::type_id::BOOL8 == type; }

bool is_type_bool_arrow(arrow::Type::type type) { return arrow::Type::type::BOOL == type; }

bool is_type_timestamp(cudf::type_id type) {
	return (cudf::type_id::TIMESTAMP_DAYS == type || cudf::type_id::TIMESTAMP_SECONDS == type ||
			cudf::type_id::TIMESTAMP_MILLISECONDS == type || cudf::type_id::TIMESTAMP_MICROSECONDS == type ||
			cudf::type_id::TIMESTAMP_NANOSECONDS == type);
}

bool is_type_timestamp_arrow(arrow::Type::type type) { return arrow::Type::type::TIMESTAMP == type; }

bool is_type_duration(cudf::type_id type) {
	return (cudf::type_id::DURATION_DAYS == type || cudf::type_id::DURATION_SECONDS == type ||
			cudf::type_id::DURATION_MILLISECONDS == type || cudf::type_id::DURATION_MICROSECONDS == type ||
			cudf::type_id::DURATION_NANOSECONDS == type);
}

bool is_type_string(cudf::type_id type) { return cudf::type_id::STRING == type; }

bool is_type_string_arrow(arrow::Type::type type) { return arrow::Type::type::STRING == type; }

cudf::size_type get_index(const std::string & operand_string) {
	assert(is_var_column(operand_string) || is_literal(operand_string));

	return std::stoi(is_literal(operand_string) ? operand_string : operand_string.substr(1, operand_string.size() - 1));
}

std::string get_aggregation_operation_string(std::string operator_string) {

	// lets check to see if its a full expression. If its not, we assume its the aggregator, so lets return that
	if (operator_string.find("=[") == std::string::npos && operator_string.find("(") == std::string::npos)
		return operator_string;

	operator_string = operator_string.substr(
		operator_string.find("=[") + 2, (operator_string.find("]") - (operator_string.find("=[") + 2)));

	// remove expression
	return operator_string.substr(0, operator_string.find("("));
}

std::vector<std::string> get_tokens_in_reverse_order(const std::string & expression) {
	std::vector<std::string> tokens = StringUtil::splitNotInQuotes(expression, " ");
	std::reverse(tokens.begin(), tokens.end());
	return tokens;
}

// takes an expression and given a starting index pointing at either ( or [, it finds the corresponding closing char )
// or ]
int find_closing_char(const std::string & expression, int start) {
	char openChar = expression[start];

	char closeChar = openChar;
	if(openChar == '(') {
		closeChar = ')';
	} else if(openChar == '[') {
		closeChar = ']';
	} else {
		// TODO throw error
		return -1;
	}

    size_t curInd = start + 1;
	int depth = 1;
	bool inQuotes = false;

	while(curInd < expression.size()) {
		if(inQuotes) {
			if(expression[curInd] == '\'') {
				if(!(curInd + 1 < expression.size() &&
					   expression[curInd + 1] ==
						   '\'')) {  // if we are in quotes and we get a double single quotes, that is an escaped quotes
					inQuotes = false;
				}
			}
		} else {
			if(expression[curInd] == '\'') {
				inQuotes = true;
			} else if(expression[curInd] == openChar) {
				depth++;
			} else if(expression[curInd] == closeChar) {
				depth--;
				if(depth == 0) {
					return curInd;
				}
			}
		}
		curInd++;
	}
	// TODO throw error
	return -1;
}

// interprets the expression and if is n-ary and logical, then returns their corresponding binary version
std::string expand_if_logical_op(std::string expression) {
	std::string output = expression;
    size_t start_pos = 0;

	while(start_pos < expression.size()) {
		std::vector<bool> is_quoted_vector = StringUtil::generateQuotedVector(expression);

		int first_and = StringUtil::findFirstNotInQuotes(
			expression, "AND(", start_pos, is_quoted_vector);  // returns -1 if not found
		int first_or = -1;

		std::string floor_str = "FLOOR";
		if (StringUtil::contains(expression, floor_str) == false) {
			first_or = StringUtil::findFirstNotInQuotes(expression, "OR(", start_pos, is_quoted_vector);  // returns -1 if not found
		}

		int first = -1;
		std::string op = "";
		if(first_and >= 0) {
			if(first_or >= 0 && first_or < first_and) {
				first = first_or;
				op = "OR(";
			} else {
				first = first_and;
				op = "AND(";
			}
		} else {
			first = first_or;
			op = "OR(";
		}

		if(first >= 0) {
			int expression_start = first + op.size() - 1;
			int expression_end = find_closing_char(expression, expression_start);

			std::string rest = expression.substr(expression_start + 1, expression_end - (expression_start + 1));
			// the trim flag is false because trimming the expressions cause malformmed ones
			std::vector<std::string> processed = get_expressions_from_expression_list(rest, false);

			if(processed.size() == 2) {  // is already binary
				start_pos = expression_start;
				continue;
			} else {
				start_pos = first;
			}

			output = expression.substr(0, first);
			for(size_t I = 0; I < processed.size() - 1; I++) {
				output += op;
				start_pos += op.size();
			}

			output += processed[0] + ",";
			for(size_t I = 1; I < processed.size() - 1; I++) {
				output += processed[I] + "),";
			}
			output += processed[processed.size() - 1] + ")";

			if(static_cast<size_t>(expression_end) < expression.size() - 1) {
				output += expression.substr(expression_end + 1);
			}
			expression = output;
		} else {
			return output;
		}
	}

	return output;
}

std::string clean_calcite_expression(const std::string & expression) {
	std::string clean_expression = replace_calcite_regex(expression);

	ral::parser::parse_tree tree;
	tree.build(clean_expression);
	tree.transform_to_custom_op();
	clean_expression = tree.rebuildExpression();

	clean_expression = expand_if_logical_op(clean_expression);

	std::string new_string = "";
	new_string.reserve(clean_expression.size());

	for(size_t i = 0; i < clean_expression.size(); i++) {
		if(clean_expression[i] == '(') {
			new_string.push_back(' ');

		} else if(clean_expression[i] != ',' && clean_expression[i] != ')') {
			new_string.push_back(clean_expression.at(i));
		}
	}

	return new_string;
}

std::string get_string_between_outer_parentheses(std::string input_string) {
    size_t start_pos = input_string.find("(");
    size_t end_pos = input_string.rfind(")");
    if (start_pos == std::string::npos || end_pos == std::string::npos || end_pos < start_pos) {
        return "";
    }
    start_pos++;
	// end_pos--;

	return input_string.substr(start_pos, end_pos - start_pos);
}

int count_string_occurrence(std::string haystack, std::string needle) {
	size_t position = haystack.find(needle, 0);
	int count = 0;
	while(position != std::string::npos) {
		count++;
		position = haystack.find(needle, position + needle.size());
	}

	return count;
}
