#include <spdlog/spdlog.h>
// #include "tests/utilities/BlazingUnitTest.h"

#include <chrono>
#include <thread>

#include "blazing_table/BlazingCudfTable.h"
#include "execution_kernels/BatchProjectionProcessing.h"
//#include <gtest/gtest.h>
#include <cudf_test/table_utilities.hpp>

#include "bmr/initializer.h"
#include "compute/cudf/detail/types.h"
#include "cudf_test/column_wrapper.hpp"
#include "cudf_test/type_lists.hpp"  // cudf::test::NumericTypes
#include "execution_graph/Context.h"
#include "execution_graph/PhysicalPlanGenerator.h"
#include "execution_graph/executor.h"
#include "execution_graph/graph.h"
#include "execution_graph/manager.h"
#include "execution_graph/port.h"
#include "execution_kernels/BatchProcessing.h"
#include "execution_kernels/kernel.h"
#include "generators/data_generator.h"
#include "io/data_provider/UriDataProvider.h"
#include "operators/Concatenate.h"
#include "parser/expression_utils.hpp"
#include "utilities/DebuggingUtils.h"

using blazingdb::transport::Node;
using ral::cache::CacheMachine;
using ral::cache::kernel;
using ral::cache::kstatus;
using ral::frame::BlazingCudfTable;
using Context = blazingdb::manager::Context;

#ifndef DATASET_PATH
#error DATASET_PATH must be defined for precompiling
#define DATASET_PATH "/"
#endif

// Just creates a Context
std::shared_ptr<Context> make_context(const std::string& output_type = "cudf",
                                      const std::string& preferred_compute = "cudf") {
  int num_nodes = 1;
  std::vector<Node> nodes(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    nodes[i] = Node(std::to_string(i));
  }
  Node master_node("0");
  std::string logicalPlan;
  std::map<std::string, std::string> config_options;
  std::string current_timestamp;
  std::shared_ptr<Context> context =
      std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options,
                                current_timestamp, output_type, preferred_compute);
  return context;
}

std::unique_ptr<ral::frame::BlazingTable> runQuery(
    std::string& json, std::string output_type, std::string preferred_compute,
    blazingdb::test::parquet_loader_tuple data_loader);

template <typename TO, typename FROM>
std::unique_ptr<TO> static_unique_pointer_cast(std::unique_ptr<FROM>&& old) {
  return std::unique_ptr<TO>{static_cast<TO*>(old.release())};
  // conversion: unique_ptr<FROM>->FROM*->TO*->unique_ptr<TO>
}

struct ComputeTestParam {
  std::string logical_plan;
  ral::execution::execution_backend preferred_compute;
  ral::execution::backend_id compare_with;

  std::string get_output_type() const {
    return preferred_compute.id() == ral::execution::backend_id::ARROW ? "pandas"
                                                                       : "cudf";
  }
  std::string get_preferred_compute() const {
    return preferred_compute.id() == ral::execution::backend_id::ARROW ? "arrow" : "cudf";
  }

  void compare_results(std::unique_ptr<ral::frame::BlazingTable>&& result) {
    if (this->compare_with != ral::execution::backend_id::NONE) {
      if (this->preferred_compute.id() == ral::execution::backend_id::ARROW) {
        std::unique_ptr<ral::frame::BlazingArrowTable> arrow_table =
            static_unique_pointer_cast<ral::frame::BlazingArrowTable>(std::move(result));
        ral::frame::BlazingCudfTable blazingCudfTable =
            ral::frame::BlazingCudfTable(std::move(arrow_table));  //
        auto result_view = blazingCudfTable.to_table_view();
        ral::utilities::print_blazing_cudf_table_view(result_view, "result");
        auto* result_view_ptr =
            dynamic_cast<ral::frame::BlazingCudfTableView*>(result_view.get());

        auto data_loader = blazingdb::test::load_table(
            DATASET_PATH, "nation",
            ral::execution::execution_backend{ral::execution::backend_id::CUDF});
        auto expected_result = runQuery(this->logical_plan, "cudf", "cudf", data_loader);
        auto expected_result_view = expected_result->to_table_view();
        ral::utilities::print_blazing_cudf_table_view(expected_result_view,
                                                      "expected_result");
        auto* expect_cudf_table =
            dynamic_cast<ral::frame::BlazingCudfTableView*>(expected_result_view.get());

        // TODO: use cudf::test::expect_tables_equal only when nulls are really important
        cudf::test::expect_tables_equivalent(expect_cudf_table->view(),
                                             result_view_ptr->view());
      } else if (this->preferred_compute.id() == ral::execution::backend_id::CUDF) {
        auto result_view = result->to_table_view();
        ral::utilities::print_blazing_cudf_table_view(result_view, "result");
        auto* result_view_ptr =
            dynamic_cast<ral::frame::BlazingCudfTableView*>(result_view.get());

        // TODO: compare cudf results with some expected result
        /*auto data_loader = blazingdb::test::load_table(DATASET_PATH, "nation",
        ral::execution::execution_backend{ral::execution::backend_id::CUDF}); auto
        expected_result = runQuery(this->logical_plan, "cudf", "cudf", data_loader); auto
        expected_result_view = expected_result->to_table_view();
        ral::utilities::print_blazing_cudf_table_view(expected_result_view,
        "expected_result"); auto *expect_cudf_table =
        dynamic_cast<ral::frame::BlazingCudfTableView*>(expected_result_view.get());
        cudf::test::expect_tables_equivalent(expect_cudf_table->view(),
        result_view_ptr->view());*/
      }
    }
  }
};

::std::ostream& operator<<(::std::ostream& os, const ComputeTestParam& param) {
  return os << param.logical_plan << "\n=> " << param.get_preferred_compute();
}

struct ComputeTest : public ::testing::TestWithParam<ComputeTestParam> {
  virtual void SetUp() override {
    this->parameter = GetParam();  // test parameter

    BlazingRMMInitialize("pool_memory_resource", 32 * 1024 * 1024, 256 * 1024 * 1024);
    float host_memory_quota = 0.75;  // default value
    blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
    ral::memory::set_allocation_pools(4000000, 10, 4000000, 10, false, nullptr);
    int executor_threads = 10;
    ral::execution::executor::init_executor(executor_threads, 0.8,
                                            this->parameter.preferred_compute);
  }

  virtual void TearDown() override {
    ral::memory::empty_pools();
    BlazingRMMFinalize();
  }
  ComputeTestParam parameter;
};

const std::vector<ComputeTestParam> compute_test_params = {
    ComputeTestParam{
        .logical_plan = R"({
                'expr': 'BindableTableScan(table=[[main, nation]], filters=[[<($0, 6)]], projects=[[0, 2]], aliases=[[n_nationkey, n_regionkey]])',
                  'children': []
                })",
        .preferred_compute =
            ral::execution::execution_backend{ral::execution::backend_id::ARROW},
        .compare_with = ral::execution::backend_id::CUDF
    },
    // TODO: Fix this case: Error converting arrowTable to cudfTable
    /*ComputeTestParam{
        .logical_plan = R"({
                'expr': 'LogicalAggregate(group=[{0, 1}])',
		'children': [
                  {
                          'expr': 'BindableTableScan(table=[[main, nation]], projects=[[2, 0]], aliases=[[n_regionkey, n_nationkey]]',
                          'children': []
                  }
                ]
	})",
        .preferred_compute = ral::execution::execution_backend{ral::execution::backend_id::ARROW},
        .compare_with = ral::execution::backend_id::CUDF
    },*/
};

TEST_P(ComputeTest, SimpleQueriesTest){
  auto data_loader = blazingdb::test::load_table(DATASET_PATH, "nation", this->parameter.preferred_compute);
  auto result = runQuery(this->parameter.logical_plan, this->parameter.get_output_type(), this->parameter.get_preferred_compute(), data_loader);
  this->parameter.compare_results(std::move(result));
}

INSTANTIATE_TEST_CASE_P(ComputeTestCase, ComputeTest, testing::ValuesIn(compute_test_params));

void transform_json_tree(boost::property_tree::ptree &p_tree, std::vector<std::string> &table_scans) {
  std::string expr = p_tree.get<std::string>("expr", "");
  if (is_scan(expr)){
    table_scans.push_back(expr);
  }
  for (auto &child : p_tree.get_child("children")) {
    transform_json_tree(child.second, table_scans);
  }
}

std::vector<std::string> get_all_table_scans(std::string json) {
  std::vector<std::string> table_scans;

  std::istringstream input(json);
  boost::property_tree::ptree p_tree;
  boost::property_tree::read_json(input, p_tree);
  transform_json_tree(p_tree, table_scans);
  return table_scans;
}

std::unique_ptr<ral::frame::BlazingTable> runQuery(std::string& json, std::string output_type, std::string preferred_compute, blazingdb::test::parquet_loader_tuple data_loader) {
  std::replace(json.begin(), json.end(), '\'', '\"');

  std::shared_ptr<Context> context = make_context(output_type, preferred_compute);
  ral::cache::CacheMachine cacheMachine(context, "");

  std::shared_ptr<ral::io::parquet_parser> parser;
  std::shared_ptr<ral::io::uri_data_provider> provider;
  ral::io::Schema schema;

  std::tie(parser, provider, schema) = data_loader;
  ral::io::data_loader loader(parser, provider);

  std::vector<ral::io::data_loader> input_loaders = {loader};
  std::vector<ral::io::Schema> schemas = {schema};
  std::vector<std::string> table_names = {"nation"};

  //  getTableScanInfo(logical_plan_simple_str)
  std::vector<std::string> table_scans = get_all_table_scans(json) ;
  auto tree = std::make_shared<ral::batch::tree_processor>(
      ral::batch::node(), context, input_loaders, schemas, table_names, table_scans,
      false);
  auto graph_pair = tree->build_batch_graph(json);

  auto print = std::make_shared<ral::batch::Print>();
  auto graph = std::get<0>(graph_pair);
  auto max_kernel_id = std::get<1>(graph_pair);
  auto output = std::shared_ptr<ral::cache::kernel>(
      new ral::batch::OutputKernel(max_kernel_id, context));

  ral::cache::cache_settings cache_machine_config;
  cache_machine_config.type = context->getTotalNodes() == 1
                              ? ral::cache::CacheType::CONCATENATING
                              : ral::cache::CacheType::SIMPLE;
  cache_machine_config.context = context;
  cache_machine_config.concat_all = true;

  graph->addPair(ral::cache::kpair(graph->get_last_kernel(), output, cache_machine_config));
  graph->check_for_simple_scan_with_limit_query();
  graph->check_and_complete_work_flow();
  graph->set_kernels_order();
  std::map<std::string, std::string> config_options;
  auto mem_monitor = std::make_shared<ral::MemoryMonitor>(tree, config_options);
  graph->set_memory_monitor(mem_monitor);
  graph->start_execute(1);

  std::vector<std::unique_ptr<ral::frame::BlazingTable>> frames;
  frames = get_execute_graph_results(graph);
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views;
  for (int i = 0; i < frames.size(); ++i) {
    table_views.push_back(frames[i]->to_table_view());
  }
  return concatTables(table_views);
}
