#include "io/data_parser/sql/SnowFlakeParser.h"
#include "io/data_provider/sql/SnowFlakeDataProvider.h"
#include "sql_provider_test_utils.h"
#include "tests/utilities/BlazingUnitTest.h"
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>

struct SnowFlakeProviderTest : public BlazingUnitTest {};

TEST_F(SnowFlakeProviderTest, DISABLED_snowflake_select_all) {
	ral::io::sql_info sql;
	sql.user = "snowusr";
	sql.password = "snowpwd";
	sql.schema = "BDEMO";
	sql.table = "PRUEBA1";
	sql.table_filter = "";
	sql.table_batch_size = 2000;
	sql.sub_schema = "PUBLIC";
	sql.dsn = "snowflake";

	auto snowflake_provider =
		std::make_shared<ral::io::snowflake_data_provider>(sql, 1, 0);
	ral::io::snowflake_parser parser;

	ParseSchema(snowflake_provider, parser);
}
