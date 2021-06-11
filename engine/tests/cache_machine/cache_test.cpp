#include <spdlog/spdlog.h>

#include <memory>

#include "execution_kernels/BatchFilterProcessing.h"
#include "execution_kernels/BatchProjectionProcessing.h"
#include "execution_kernels/kernel.h"

// #include <src/cache_machine/CacheMachine.h>
// #include <src/utilities/DebuggingUtils.h>

#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/table_utilities.hpp>

#include "bmr/BufferProvider.h"
#include "bmr/initializer.h"
#include "cudf_test/column_wrapper.hpp"
#include "cudf_test/type_lists.hpp"  // cudf::test::NumericTypes
#include "execution_graph/Context.h"
#include "execution_graph/executor.h"
#include "execution_graph/graph.h"
#include "execution_graph/port.h"
#include "execution_kernels/BatchProcessing.h"
#include "execution_kernels/kernel.h"

using blazingdb::manager::Context;
using blazingdb::transport::Node;

struct CacheMachineTest : public  ::testing::Test {
  CacheMachineTest() {
    BlazingRMMInitialize("pool_memory_resource", 32 * 1024 * 1024, 256 * 1024 * 1024);
    float host_memory_quota = 0.75;  // default value
    blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
    ral::memory::set_allocation_pools(4000000, 10, 4000000, 10, false, nullptr);
    int executor_threads = 10;
    ral::execution::execution_backend preferred_compute(ral::execution::backend_id::CUDF);
    ral::execution::executor::init_executor(executor_threads, 0.8, preferred_compute);
  }
  virtual void TearDown() override {
    ral::memory::empty_pools();
    BlazingRMMFinalize();
  }
};

template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
    auto sequence = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return TypeParam(i); });
    std::vector<TypeParam> data(sequence, sequence + size);
    cudf::test::fixed_width_column_wrapper<TypeParam> col(data.begin(), data.end());
    return col.release();
}
std::unique_ptr<ral::frame::BlazingCudfTable> build_custom_table() {
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

  cudf::test::strings_column_wrapper col2(
      {"d", "e", "a", "d", "k", "d", "l", "a", "b", "c"}, {1, 0, 1, 1, 1, 1, 1, 1, 0, 1});

  std::unique_ptr<cudf::column> str_col = std::make_unique<cudf::column>(std::move(col2));
  columns.push_back(std::move(str_col));

  std::vector<std::string> column_names = {"INT64", "INT32", "FLOAT64", "FLOAT32",
                                           "STRING"};

  auto table = std::make_unique<cudf::table>(std::move(columns));
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(table), column_names);
}

std::unique_ptr<ral::frame::BlazingCudfTable> build_custom_one_column_table() {
  cudf::size_type size = 10;

  auto num_column_1 = make_col<int32_t>(size);
  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(num_column_1));
  std::vector<std::string> column_names = {"INT64"};

  auto table = std::make_unique<cudf::table>(std::move(columns));
  return std::make_unique<ral::frame::BlazingCudfTable>(std::move(table), column_names);
}

TEST_F(CacheMachineTest, CacheMachineTest) {
	std::vector<Node> nodes;
	Node master_node;
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;
	std::string current_timestamp;
	std::shared_ptr<Context> context = std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options, current_timestamp, "cudf", "cudf");
	ral::cache::CacheMachine cacheMachine(context,"");

	for(int i = 0; i < 10; ++i) {
		auto table = build_custom_table();
		std::cout << ">> " << i << "|" << table->size_in_bytes() << std::endl;
		cacheMachine.addToCache(std::move(table));
		if(i % 5 == 0) {
			auto cacheTable = cacheMachine.pullFromCache(ral::execution::execution_backend(ral::execution::backend_id::CUDF));
		}
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}


TEST_F(CacheMachineTest, CPUCacheMachineTest) {
	std::vector<Node> nodes;
	Node master_node;
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;
	std::string current_timestamp;
	int cache_level_override = 1;
	std::shared_ptr<Context> context = std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options, current_timestamp, "cudf", "cudf");

	ral::cache::CacheMachine cacheMachine(context, "", true, cache_level_override);

	//add tables
	for(int i = 0; i < 10; ++i) {
		auto table = build_custom_table();

		cacheMachine.addToCache(std::move(table));
	}
	auto compare_table = build_custom_table();

	for(int i = 0; i < 10; ++i) {
		auto cacheTable = cacheMachine.pullFromCache(ral::execution::execution_backend(ral::execution::backend_id::CUDF));
		auto cacheTable_view =
			std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(
				cacheTable->to_table_view());

		cudf::test::expect_tables_equivalent(compare_table->view(), cacheTable_view->view());
	}

	std::this_thread::sleep_for(std::chrono::seconds(1));
}
