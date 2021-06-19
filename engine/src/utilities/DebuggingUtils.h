#ifndef _BLAZING_DEBUG_UTILS_H
#define _BLAZING_DEBUG_UTILS_H

/*
 * NOTE:
 * The cmake will set the precondition definiton 'SQLDBGUTILS' when:
 * - the build type is Release or
 * - the build type is RelWithDebInfo or
 * - we are in a conda build process
 * So, when 'SQLDBGUTILS' is defined the ral will link against gtest and
 * cudf test utils and we will be able to use print_* functions
*/

#include "blazing_table/BlazingTableView.h"

namespace ral {
namespace utilities {

std::string type_string(arrow::Type::type dtype);

void print_blazing_cudf_table_view(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string table_name="");

void print_blazing_table_view_schema(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string table_name="");

std::string blazing_table_view_schema_to_string(std::shared_ptr<ral::frame::BlazingTableView> table_view, const std::string table_name);

// std::string cache_data_schema_to_string(ral::cache::CacheData * cache_data);

}  // namespace utilities
}  // namespace ral

#endif  //_BLAZING_DEBUG_UTILS_H
