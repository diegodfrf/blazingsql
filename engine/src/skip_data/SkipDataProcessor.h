
#ifndef SKIPDATAPROCESSOR_H_
#define SKIPDATAPROCESSOR_H_

#include <iostream>
#include <string>
#include "parser/expression_tree.hpp"
#include "blazing_table/BlazingTable.h"
#include "blazing_table/BlazingArrowTable.h"

namespace ral {
namespace skip_data {

// For unit testing
void drop_value(ral::parser::parse_tree& tree, const std::string & value);
bool apply_skip_data_rules(ral::parser::parse_tree& tree);

std::pair<std::unique_ptr<ral::frame::BlazingTable>, bool> process_skipdata_for_table(
std::shared_ptr<ral::frame::BlazingArrowTable> metadata, const std::vector<std::string> & names, std::string table_scan);

} // namespace skip_data
} // namespace ral


#endif //SKIPDATAPROCESSOR_H_
