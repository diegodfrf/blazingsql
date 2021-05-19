#include "io/data_parser/sql/PostgreSQLParser.h"
#include "io/data_provider/sql/PostgreSQLDataProvider.h"
#include "sql_provider_test_utils.h"
#include "tests/utilities/BlazingUnitTest.h"
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>

struct PostgreSQLProviderTest : public BlazingUnitTest {};

TEST_F(PostgreSQLProviderTest, DISABLED_postgresql_select_all) {
  ral::io::sql_info sql;
  sql.host = "localhost";
  sql.port = 5432;
  sql.user = "myadmin";
  sql.password = "";
  sql.schema = "pagila";
  sql.table = "prueba5";
  sql.table_filter = "";
  sql.table_batch_size = 2000;

  auto postgresql_provider =
      std::make_shared<ral::io::postgresql_data_provider>(sql, 1, 0);
  ral::io::postgresql_parser parser;

  ParseSchema(postgresql_provider, parser);
}
