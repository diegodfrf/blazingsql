#include "io/data_parser/sql/AbstractSQLParser.h"
#include "io/data_provider/sql/AbstractSQLDataProvider.h"
#include "tests/utilities/BlazingUnitTest.h"
#include "utilities/DebuggingUtils.h"
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>


const std::unordered_map<cudf::type_id, const char *> & MapDataTypeName() {
	static std::unordered_map<cudf::type_id, const char *> dt2name{
		{cudf::type_id::INT8, "INT8"},
		{cudf::type_id::INT16, "INT16"},
		{cudf::type_id::INT32, "INT32"},
		{cudf::type_id::INT64, "INT64"},
		{cudf::type_id::UINT8, "UINT8"},
		{cudf::type_id::UINT16, "UINT16"},
		{cudf::type_id::UINT32, "UINT32"},
		{cudf::type_id::UINT64, "UINT64"},
		{cudf::type_id::FLOAT32, "FLOAT32"},
		{cudf::type_id::FLOAT64, "FLOAT64"},
		{cudf::type_id::DECIMAL64, "DECIMAL64"},
		{cudf::type_id::BOOL8, "BOOL8"},
		{cudf::type_id::STRING, "STRING"},
	};
	return dt2name;
}

static inline void ParseSchema(
	const std::shared_ptr<ral::io::abstractsql_data_provider> & provider,
	ral::io::abstractsql_parser & parser) {
	ral::io::Schema schema;
	auto handle = provider->get_next(true);

	parser.parse_schema(handle, schema);

	const std::unordered_map<cudf::type_id, const char *> & dt2name =
		MapDataTypeName();

	std::cout << "SCHEMA" << std::endl
			  << "  length = " << schema.get_num_columns() << std::endl
			  << "  columns" << std::endl;
	for(std::size_t i = 0; i < schema.get_num_columns(); i++) {
		const std::string & name = schema.get_name(i);
		std::cout << "    " << name << ": ";
		try {
			const std::string dtypename = dt2name.at(schema.get_dtype(i));
			std::cout << dtypename << std::endl;
		} catch(std::exception &) {
			std::cout << static_cast<int>(schema.get_dtype(i)) << std::endl;
		}
	}

	auto num_cols = schema.get_num_columns();

	std::vector<int> column_indices(num_cols);
	std::iota(column_indices.begin(), column_indices.end(), 0);

	std::vector<cudf::size_type> row_groups;
	auto table = parser.parse_batch(handle, schema, column_indices, row_groups);

	std::cout << "TABLE" << std::endl
			  << " ncolumns =  " << table->num_columns() << std::endl
			  << " nrows =  " << table->num_rows() << std::endl;

	auto tv = table->toBlazingTableView();

	for(cudf::size_type i = 0; i < static_cast<cudf::size_type>(num_cols);
		i++) {
		cudf::test::print(tv.column(i));
	}
}
