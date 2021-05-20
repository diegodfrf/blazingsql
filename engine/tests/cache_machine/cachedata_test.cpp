#include <spdlog/spdlog.h>
#include "tests/utilities/BlazingUnitTest.h"

#include <src/cache_machine/GPUCacheData.h>
#include <src/utilities/DebuggingUtils.h>
#include "execution_graph/backend.hpp"

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

TEST_F(CacheDataTest, CacheDataCloneTest) {

	using T = int32_t;
	cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
	cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
	cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
	cudf::table_view cudf_table_in_view {{col1, col2, col3}};

	std::unique_ptr<cudf::table> cudf_table = std::make_unique<cudf::table>(cudf_table_in_view);

	std::vector<std::string> names({"A", "B", "C"});
	std::unique_ptr<ral::frame::BlazingCudfTable> blz_table = std::make_unique<ral::frame::BlazingCudfTable>(std::move(cudf_table), names);

	std::size_t num_rows = blz_table->num_rows();
	std::size_t num_columns = blz_table->num_columns();

	auto table_ptr = std::make_unique<ral::cache::GPUCacheData>(std::move(blz_table));
	std::unique_ptr<ral::cache::CacheData> cloned_data = table_ptr->clone();
	table_ptr.reset();

	EXPECT_EQ(cloned_data->num_rows(), num_rows);
	EXPECT_EQ(cloned_data->num_columns(), num_columns);

	auto decached_table = cloned_data->decache(ral::execution::execution_backend(ral::execution::backend_id::CUDF));
	auto blz_table_view = decached_table->to_table_view();
	auto cloned_table = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(blz_table_view)->view();

	cudf::test::fixed_width_column_wrapper<T> expect_col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
	cudf::test::strings_column_wrapper expect_col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
	cudf::test::fixed_width_column_wrapper<T> expect_col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
	cudf::table_view expect_cudf_table_view {{expect_col1, expect_col2, expect_col3}};

	cudf::test::expect_tables_equal(expect_cudf_table_view, cloned_table);
}
