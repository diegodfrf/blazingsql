//#include "gtest/gtest.h"

#include <cudf/sorting.hpp>

#include <operators/GroupBy.h>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/base_fixture.hpp>
#include <cudf_test/type_lists.hpp>
#include <cudf_test/table_utilities.hpp>
#include <cudf_test/column_utilities.hpp>
#include "tests/utilities/BlazingUnitTest.h"
#include "io/io.h"
#include "io/Schema.h"

TEST(StrCompare, CStrEqual) {
  
  std::cout << "11111\n";
  TableSchema a;
  std::cout << "222\n";
  TableSchema tableSchema;
  std::cout << "3333\n";
  tableSchema = a;
  std::cout << "4444\n";
  std::vector<cudf::type_id> types;
  auto c = ral::io::Schema(tableSchema.names,
        tableSchema.calcite_to_file_indices,
        types,
        tableSchema.in_file,
        tableSchema.row_groups_ids);
  
  
   std::cout << "PLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL\n";

}
