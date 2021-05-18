#include <spdlog/spdlog.h>
#include "tests/utilities/BlazingUnitTest.h"

#include <src/cache_machine/GPUCacheData.h>
#include <src/utilities/DebuggingUtils.h>

#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/table_utilities.hpp>
#include <cudf_test/column_utilities.hpp>
#include "bmr/BufferProvider.h"

using blazingdb::manager::Context;
using blazingdb::transport::Node;

struct CacheDataTest : public BlazingUnitTest {

	CacheDataTest(){

		ral::memory::set_allocation_pools(4000000, 10,
										4000000, 10, false,nullptr);

		blazing_host_memory_resource::getInstance().initialize(0.5);
	}
	~CacheDataTest(){
		ral::memory::empty_pools();
	}

};

template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
    auto sequence = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return TypeParam(i); });
    std::vector<TypeParam> data(sequence, sequence + size);
    cudf::test::fixed_width_column_wrapper<TypeParam> col(data.begin(), data.end());
    return col.release();
}

std::unique_ptr<ral::frame::BlazingTable> build_custom_table() {
	cudf::size_type size = 10;

	auto num_column_1 = make_col<int32_t>(size);
	auto num_column_2 = make_col<int64_t>(size);
	auto num_column_3 = make_col<float>(size);
	auto num_column_4 = make_col<double>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(num_column_1));
	columns.push_back(std::move(num_column_2));
	columns.push_back(std::move(num_column_3));
	columns.push_back(std::move(num_column_4));

	cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l", "a", "b", "c"}, {1, 0, 1, 1, 1, 1, 1, 1, 0, 1});

	std::unique_ptr<cudf::column> str_col = std::make_unique<cudf::column>(std::move(col2));
	columns.push_back(std::move(str_col));

	std::vector<std::string> column_names = {"INT64", "INT32", "FLOAT64", "FLOAT32", "STRING"};

	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(table), column_names);
}

TEST_F(CacheDataTest, CacheDataCloneTest) {

	auto blz_table = build_custom_table();
	std::size_t num_rows = blz_table->num_rows();
	std::size_t num_columns = blz_table->num_columns();

	std::unique_ptr<ral::frame::BlazingCudfTable> cudf_table(dynamic_cast<ral::frame::BlazingCudfTable*>(blz_table.release()));
	auto table_ptr = std::make_unique<ral::cache::GPUCacheData>(std::move(cudf_table));
	std::unique_ptr<ral::cache::CacheData> cloned_data = table_ptr->clone();
	table_ptr.reset();

	EXPECT_EQ(cloned_data->num_rows(), num_rows);
	EXPECT_EQ(cloned_data->num_columns(), num_columns);
}
