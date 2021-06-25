#pragma once

#include <map>

// TODO percy make sub mod io
#include <arrow/io/file.h>
#include <arrow/scalar.h>
#include "io/DataType.h"

#include "blazing_table/BlazingTable.h"
#include "blazing_table/BlazingHostTable.h"
#include "io/Schema.h"

#include "operators/operators_definitions.h"
#include "cache_machine/CacheData.h"
// WARNING NEVER INVOKE backend_dispatcher in the api_x.cpp files
//namespace voltron {
//namespace compute {

struct sorted_merger_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
		std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tables,
		const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
		const std::vector<int> & sortColIndices,
      const std::vector<voltron::compute::NullOrder> & sortOrderNulls) const
  {
    throw std::runtime_error("ERROR: sorted_merger_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct get_pivot_points_table_functor{
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
    int number_partitions,
    std::shared_ptr<ral::frame::BlazingTableView> sortedSamples) const
  {
    throw std::runtime_error("ERROR: get_pivot_points_table_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};


struct groupby_without_aggregations_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::vector<int> group_column_indices) const
  {
    throw std::runtime_error("ERROR: groupby_without_aggregations_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct aggregations_without_groupby_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::vector<std::string> aggregation_input_expressions,
      std::vector<voltron::compute::AggregateKind> aggregation_types,
      std::vector<std::string> aggregation_column_assigned_aliases) const
  {
    throw std::runtime_error("ERROR: aggregations_without_groupby_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct aggregations_with_groupby_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::vector<std::string> aggregation_input_expressions,
      std::vector<voltron::compute::AggregateKind> aggregation_types,
      std::vector<std::string> aggregation_column_assigned_aliases,
      std::vector<int> group_column_indices) const
  {
    throw std::runtime_error("ERROR: aggregations_with_groupby_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct cross_join_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> left,
      std::shared_ptr<ral::frame::BlazingTableView> right) const
  {
    throw std::runtime_error("ERROR: cross_join_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct check_if_has_nulls_functor {
  template <typename T>
  bool operator()(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<int> const& keys) const
  {
    throw std::runtime_error("ERROR: check_if_has_nulls_functor This default dispatcher operator should not be called.");
    return false;
  }
};

struct inner_join_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> left,
      std::shared_ptr<ral::frame::BlazingTableView> right,
      std::vector<int> const& left_column_indices,
      std::vector<int> const& right_column_indices,
      voltron::compute::NullEquality equalityType) const
  {
    throw std::runtime_error("ERROR: inner_join_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct drop_nulls_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::vector<int> const& keys) const
  {
    throw std::runtime_error("ERROR: drop_nulls_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct left_join_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> left,
      std::shared_ptr<ral::frame::BlazingTableView> right,
      std::vector<int> const& left_column_indices,
      std::vector<int> const& right_column_indices) const
  {
	throw std::runtime_error("ERROR: left_join_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct full_join_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> left,
      std::shared_ptr<ral::frame::BlazingTableView> right,
      bool has_nulls_left,
      bool has_nulls_right,
      std::vector<int> const& left_column_indices,
      std::vector<int> const& right_column_indices) const
  {
	throw std::runtime_error("ERROR: full_join_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct reordering_columns_due_to_right_join_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::unique_ptr<ral::frame::BlazingTable> table_ptr, size_t right_columns) const
  {
    throw std::runtime_error("ERROR: reordering_columns_due_to_right_join_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

//struct process_project_functor {
//  template <typename T>
//  std::unique_ptr<ral::frame::BlazingTable> operator()(
//      std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
//      const std::vector<std::string> & expressions,
//      const std::vector<std::string> & out_column_names) const {
//    // TODO percy arrow thrown error
//    return nullptr;
//  }
//};

struct build_only_schema {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view) const {
    throw std::runtime_error("ERROR: build_only_schema This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct evaluate_expressions_wo_filter_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table,
  const std::vector<std::string> & expressions, 
  const std::vector<std::string> column_names) const
  {
    throw std::runtime_error("ERROR: evaluate_expressions_wo_filter_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct apply_boolean_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view,
  std::shared_ptr<ral::frame::BlazingTableView> bool_column_values) const
  {
    throw std::runtime_error("ERROR: apply_boolean_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};


struct evaluate_expressions_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::shared_ptr<ral::frame::BlazingTableView> table_view,
  const std::vector<std::string> & expressions) const
  {
    throw std::runtime_error("ERROR: evaluate_expressions_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct sorted_order_gather_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::shared_ptr<ral::frame::BlazingTableView> sortColumns_view,
      const std::vector<voltron::compute::SortOrder> & sortOrderTypes,
      std::vector<voltron::compute::NullOrder> null_orders) const
  {
    throw std::runtime_error("ERROR: sorted_order_gather_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct create_empty_table_like_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view) const
  {
    throw std::runtime_error("ERROR: create_empty_table_like_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

// if column_indices.size>0 will create based on those indexes
struct create_empty_table_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      const std::vector<std::string> &column_names,
	    const std::vector<std::shared_ptr<arrow::DataType>> &dtypes,
      std::vector<int> column_indices = {}) const
  {
    throw std::runtime_error("ERROR: create_empty_table_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct from_table_view_to_table_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view) const
  {
    throw std::runtime_error("ERROR: from_table_view_to_table_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct sample_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      int const num_samples,
      std::vector<std::string> sortColNames,
      std::vector<int> sortColIndices) const
  {
    throw std::runtime_error("ERROR: sample_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};

struct checkIfConcatenatingStringsWillOverflow_functor {
  template <typename T>
  inline bool operator()(const std::vector<std::shared_ptr<ral::frame::BlazingTableView>> & tables) const
  {
    throw std::runtime_error("ERROR: checkIfConcatenatingStringsWillOverflow_functor This default dispatcher operator should not be called.");
    return false;
  }
};


struct concat_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::vector<std::shared_ptr<ral::frame::BlazingTableView>> table_views,
      size_t empty_count,
      std::vector<std::string> names) const
  {
    throw std::runtime_error("ERROR: concat_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};


struct split_functor {
  template <typename T>
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_View,
      std::vector<int> const& splits) const
  {
    throw std::runtime_error("ERROR: split_functor This default dispatcher operator should not be called.");
  }
};

struct normalize_types_functor {
  template <typename T>
  void operator()(
      std::unique_ptr<ral::frame::BlazingTable> & table,
      const std::vector<std::shared_ptr<arrow::DataType>> & types,
      std::vector<int> column_indices) const
  {
    throw std::runtime_error("ERROR: normalize_types_functor This default dispatcher operator should not be called.");
  }
};

struct hash_partition_functor {
  template <typename T>
  inline std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::vector<int>> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_View,
      std::vector<int> const& columns_to_hash,
      int num_partitions) const
  {
    throw std::runtime_error("ERROR: hash_partition_functor This default dispatcher operator should not be called.");
  }
};

struct select_functor {
  template <typename T>
  std::shared_ptr<ral::frame::BlazingTableView> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      const std::vector<int> & sortColIndices) const
  {
    return nullptr;
  }
};

// TODO percy arrow rommel enable this when we have arrow 4
struct upper_bound_split_functor {
  template <typename T>
  std::vector<std::shared_ptr<ral::frame::BlazingTableView>> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> sortedTable_view,
      std::shared_ptr<ral::frame::BlazingTableView> t,
      std::shared_ptr<ral::frame::BlazingTableView> values,
      std::vector<voltron::compute::SortOrder> const& column_order,
      std::vector<voltron::compute::NullOrder> const& null_precedence) const
  {
    throw std::runtime_error("ERROR: upper_bound_split_functor This default dispatcher operator should not be called.");
  }
};

template <ral::io::DataType DataSourceType>
struct io_read_file_data_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<arrow::io::RandomAccessFile> file,
      std::vector<int> column_indices,
      std::vector<std::string> col_names,
      std::vector<int> row_groups,
      const std::map<std::string, std::string> &args_map = {}) const
  {
    throw std::runtime_error("ERROR: io_read_parquet_functor This default dispatcher operator should not be called.");
  }
};

template <ral::io::DataType DataSourceType>
struct io_parse_file_schema_functor {
  template <typename T>
  void operator()(
      ral::io::Schema & schema_out,
      std::shared_ptr<arrow::io::RandomAccessFile> file,
      const std::map<std::string, std::string> &args_map = {}) const
  {
    throw std::runtime_error("ERROR: io_read_parquet_functor This default dispatcher operator should not be called.");
  }
};

struct decache_io_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable>  operator()(
    std::unique_ptr<ral::frame::BlazingTable> table,
    std::vector<int> projections,
    ral::io::Schema schema,
    std::vector<int> column_indices_in_file,
    std::map<std::string, std::string> column_values) const
  {
    throw std::runtime_error("ERROR: decache_io_functor This default dispatcher operator should not be called.");
  }
};

struct make_blazinghosttable_functor {
	template <typename T>
	std::unique_ptr<ral::frame::BlazingHostTable> operator()(std::unique_ptr<ral::frame::BlazingTable> table, bool use_pinned){
		throw std::runtime_error("ERROR: make_blazinghosttable_functor This default dispatcher operator should not be called.");
    	return nullptr;
	}
};


struct make_cachedata_functor {
  template <typename T>
  std::unique_ptr<ral::cache::CacheData> operator()(std::unique_ptr<ral::frame::BlazingTable> table) {
    throw std::runtime_error("ERROR: make_cachedata_functor This default dispatcher operator should not be called.");
    return nullptr;
  }
};


struct write_orc_functor {
  template <typename T>
  void operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::string file_path) const
  {
    throw std::runtime_error("ERROR: write_orc_functor This default dispatcher operator should not be called.");
  }
};

struct read_orc_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(std::string file_path, const std::vector<std::string> &col_names) const
  {
    throw std::runtime_error("ERROR: write_orc_functor This default dispatcher operator should not be called.");
  }
};


//} // compute
//} // voltron

#ifdef CUDF_SUPPORT
#include "compute/cudf/api_cudf.cpp"
#endif

#include "compute/arrow/api_arrow.cpp"