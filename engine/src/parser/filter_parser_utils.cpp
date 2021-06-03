#include "filter_parser_utils.h"

namespace {

const std::string LOGICAL_FILTER = "LogicalFilter";

} // namespace

bool is_logical_filter(const std::string & query_part) {
  return query_part.find(LOGICAL_FILTER) != std::string::npos;
}
