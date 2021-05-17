#include "io/data_parser/sql/SQLiteParser.h"
#include "io/data_provider/sql/SQLiteDataProvider.h"
#include "sql_provider_test_utils.h"
#include "tests/utilities/BlazingUnitTest.h"
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>

struct SQLiteProviderTest : public BlazingUnitTest {};

TEST_F(SQLiteProviderTest, DISABLED_sqlite_select_all) {
	ral::io::sql_info sql;
	sql.schema = "/blazingsql/db.sqlite3";
	sql.table = "prueba2";
	sql.table_filter = "";
	sql.table_batch_size = 2000;

	auto sqlite_provider =
		std::make_shared<ral::io::sqlite_data_provider>(sql, 1, 0);
	ral::io::sqlite_parser parser;

	ParseSchema(sqlite_provider, parser);
}
