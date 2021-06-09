#pragma once

#include "../io/io.h"
#include "common.h"
#include <string>
#include <vector>

#include "blazing_table/BlazingTableView.h"
#include <execution_graph/graph.h>
#include "../../src/utilities/error.hpp"

std::string runGeneratePhysicalGraph(uint32_t masterIndex,
                                     std::vector<std::string> worker_ids,
                                     int32_t ctxToken,
                                     std::string query,
                                     std::string output_type,
                                     std::string preferred_compute);

std::shared_ptr<ral::cache::graph> runGenerateGraph(uint32_t masterIndex,
	std::vector<std::string> worker_ids,
	std::vector<std::string> tableNames,
	std::vector<std::string> tableScans,
	std::vector<TableSchema> tableSchemas,
	std::vector<std::vector<std::string>> tableSchemaCppArgKeys,
	std::vector<std::vector<std::string>> tableSchemaCppArgValues,
	std::vector<std::vector<std::string>> filesAll,
	std::vector<int> fileTypes,
	int32_t ctxToken,
	std::string query,
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values,
	std::map<std::string, std::string> config_options,
	std::string sql,
	std::string current_timestamp,
  std::string output_type,
  std::string preferred_compute);

void startExecuteGraph(std::shared_ptr<ral::cache::graph> graph, int ctx_token);
std::unique_ptr<PartitionedResultSet> getExecuteGraphResult(std::shared_ptr<ral::cache::graph> graph, int ctx_token);

TableScanInfo getTableScanInfo(std::string logicalPlan);

std::unique_ptr<ResultSet> runSkipData(
	std::shared_ptr<ral::frame::BlazingTableView> metadata,
	std::vector<std::string> all_column_names,
	std::string query);


extern "C" {
std::pair<std::unique_ptr<PartitionedResultSet>, error_code_t> runQuery_C(int32_t masterIndex,
	std::vector<std::string> tableNames,
	std::vector<std::string> tableScans,
	std::vector<TableSchema> tableSchemas,
	std::vector<std::vector<std::string>> tableSchemaCppArgKeys,
	std::vector<std::vector<std::string>> tableSchemaCppArgValues,
	std::vector<std::vector<std::string>> filesAll,
	std::vector<int> fileTypes,
	int32_t ctxToken,
	std::string query,
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values,
	std::map<std::string, std::string> config_options);

std::pair<TableScanInfo, error_code_t> getTableScanInfo_C(std::string logicalPlan);

std::pair<std::unique_ptr<ResultSet>, error_code_t> runSkipData_C(
	std::shared_ptr<ral::frame::BlazingTableView> metadata, 
	std::vector<std::string> all_column_names, 
	std::string query);


} // extern "C"
