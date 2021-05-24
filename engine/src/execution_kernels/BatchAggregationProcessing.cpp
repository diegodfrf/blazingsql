#include "BatchAggregationProcessing.h"
#include "execution_graph/executor.h"
#include "utilities/CommonOperations.h"
#include <cudf/partitioning.hpp>
#include <arrow/compute/api.h>

// TODO percy arrow move code
#include "utilities/CommonOperations.h"
#include "parser/expression_utils.hpp"
#include "execution_kernels/LogicalProject.h"
#include "operators/GroupBy.h"
#include "parser/CalciteExpressionParsing.h"
#include <cudf/aggregation.hpp>
#include <cudf/reduction.hpp>
#include <cudf/detail/interop.hpp>

namespace ral {
namespace cpu {

std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
 	std::shared_ptr<arrow::Table> table, const std::vector<int> & group_column_indices) {

   std::vector<std::shared_ptr<arrow::Array>> result;
   result.resize(group_column_indices.size());
 
   auto f = [&table] (int col_idx) -> std::shared_ptr<arrow::Array> {
     auto uniques = arrow::compute::Unique(table->column(col_idx));
     if (!uniques.ok()) {
       // TODO throw/handle error here
       return nullptr;
     }
     return std::move(uniques.ValueOrDie());
   };

   std::transform(group_column_indices.begin(), group_column_indices.end(), result.begin(), f);

   return std::make_unique<ral::frame::BlazingArrowTable>(arrow::Table::Make(
     table->schema(),
     std::move(result),
     table->num_rows()));
 }

std::shared_ptr<arrow::Scalar> arrow_reduce(std::shared_ptr<arrow::ChunkedArray> col,
                                std::unique_ptr<cudf::aggregation> const &agg,
                                cudf::data_type output_dtype)
{
 switch (agg->kind) {
   case cudf::aggregation::SUM: {
     auto result = arrow::compute::Sum(col);
     // TODO percy arrow error
     return result.ValueOrDie().scalar();
   } break;
   case cudf::aggregation::PRODUCT: {
    
   } break;
   case cudf::aggregation::MIN: {
    
   } break;
   case cudf::aggregation::MAX: {
    
   } break;
   case cudf::aggregation::COUNT_VALID: {
    
   } break;
   case cudf::aggregation::COUNT_ALL: {
    
   } break;
   case cudf::aggregation::ANY: {
    
   } break;
   case cudf::aggregation::ALL: {
    
   } break;
   case cudf::aggregation::SUM_OF_SQUARES: {
    
   } break;
   case cudf::aggregation::MEAN: {
    
   } break;
   case cudf::aggregation::VARIANCE: {
    
   } break;
   case cudf::aggregation::STD: {
    
   } break;
   case cudf::aggregation::MEDIAN: {
    
   } break;
   case cudf::aggregation::QUANTILE: {
    
   } break;
   case cudf::aggregation::ARGMAX: {
    
   } break;
   case cudf::aggregation::ARGMIN: {
    
   } break;
   case cudf::aggregation::NUNIQUE: {
    
   } break;
   case cudf::aggregation::NTH_ELEMENT: {
    
   } break;
   case cudf::aggregation::ROW_NUMBER: {
    
   } break;
   case cudf::aggregation::COLLECT_LIST: {
    
   } break;
   case cudf::aggregation::COLLECT_SET: {
    
   } break;
   case cudf::aggregation::LEAD: {
    
   } break;
   case cudf::aggregation::LAG: {
    
   } break;
   case cudf::aggregation::PTX: {
    
   } break;
   case cudf::aggregation::CUDA: {
    
   } break;
  };
  return nullptr;
}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
 		std::shared_ptr<ral::frame::BlazingArrowTableView> table_view, const std::vector<std::string> & aggregation_input_expressions,
 		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases)
{
  using namespace ral::operators;
  
    std::shared_ptr<arrow::Table> table = table_view->view();

 	std::vector<std::shared_ptr<arrow::Scalar>> reductions;
 	std::vector<std::string> agg_output_column_names;
 	for (size_t i = 0; i < aggregation_types.size(); i++){
 		if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
            std::shared_ptr<arrow::Int64Scalar> scalar = std::make_shared<arrow::Int64Scalar>(table->num_rows());
 			reductions.emplace_back(std::move(scalar));
 		} else {
 			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> aggregation_input_scope_holder;
            std::shared_ptr<arrow::ChunkedArray> aggregation_input;
 			if(is_var_column(aggregation_input_expressions[i]) || is_number(aggregation_input_expressions[i])) {
 				aggregation_input = table->column(get_index(aggregation_input_expressions[i]));

 			} else {
         //TODO percy rommel arrow
 				//aggregation_input_scope_holder = ral::processor::evaluate_expressions(table.view(), {aggregation_input_expressions[i]});
 				//aggregation_input = aggregation_input_scope_holder[0]->view();
 			}

 			if( aggregation_types[i] == AggregateKind::COUNT_VALID) {
                std::shared_ptr<arrow::Int64Scalar> scalar = std::make_shared<arrow::Int64Scalar>(aggregation_input->length() - aggregation_input->null_count());
 				reductions.emplace_back(std::move(scalar));
 			} else {
                std::unique_ptr<cudf::aggregation> agg = makeCudfAggregation<cudf::aggregation>(aggregation_types[i]);
                cudf::type_id theinput_type = cudf::detail::arrow_to_cudf_type(*aggregation_input->type()).id();
 				cudf::type_id output_type = get_aggregation_output_type(theinput_type, aggregation_types[i], false);

 				std::shared_ptr<arrow::Scalar> reduction_out = arrow_reduce(aggregation_input, agg, cudf::data_type(output_type));

 				if (aggregation_types[i] == AggregateKind::SUM0 && !reduction_out->is_valid){ // if this aggregation was a SUM0, and it was not valid, we want it to be a valid 0 instead
                    auto dt = cudf::detail::arrow_to_cudf_type(*reduction_out->type);
 					std::shared_ptr<arrow::Int64Scalar> zero_scalar = std::make_shared<arrow::Int64Scalar>(0);
 					reductions.emplace_back(std::move(zero_scalar));
 				} else {
 					reductions.emplace_back(std::move(reduction_out));
 				}
 			}
 		}
 		// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
 		if(aggregation_column_assigned_aliases[i] == "") {
 			if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
 				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(*)");
 			} else {
 				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table_view->column_names().at(get_index(aggregation_input_expressions[i])) + ")");
 			}
 		} else {
 			agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
 		}
 	}
 	// convert scalars into columns
 	std::vector<std::shared_ptr<arrow::ChunkedArray>> output_columns;
 	for (size_t i = 0; i < reductions.size(); i++){
 		std::shared_ptr<arrow::Array> temp = arrow::MakeArrayFromScalar((*reductions[i].get()), 1).ValueOrDie();
 		output_columns.emplace_back(std::make_shared<arrow::ChunkedArray>(temp));
 	}
  
  auto new_schema = ral::cpu::utilities::build_arrow_schema(
        output_columns,
        agg_output_column_names,
        table->schema()->metadata());
  
  auto tt = arrow::Table::Make(new_schema, output_columns);
  return std::make_unique<ral::frame::BlazingArrowTable>(tt);
}

} // namespace cpu
} // namespace ral

namespace ral {
namespace batch {

// BEGIN ComputeAggregateKernel

ComputeAggregateKernel::ComputeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::ComputeAggregateKernel} {
    this->query_graph = query_graph;
}


struct groupby_without_aggregations_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::vector<int> group_column_indices) const
  {
    // TODO percy arrow thrown error
    throw std::runtime_error("ERROR: This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<int> group_column_indices) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  return ral::cpu::compute_groupby_without_aggregations(arrow_table_view->view(), group_column_indices);
}

template <>
std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<int> group_column_indices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return ral::operators::compute_groupby_without_aggregations(cudf_table_view, group_column_indices);
}

/// compute_aggregations_without_groupby


struct aggregations_without_groupby_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::vector<std::string> aggregation_input_expressions,
      std::vector<AggregateKind> aggregation_types,
      std::vector<std::string> aggregation_column_assigned_aliases) const
  {
    // TODO percy arrow thrown error
    throw std::runtime_error("ERROR: This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
std::unique_ptr<ral::frame::BlazingTable> aggregations_without_groupby_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases) const
{
  auto arrow_table_view = std::dynamic_pointer_cast<ral::frame::BlazingArrowTableView>(table_view);
  return ral::cpu::compute_aggregations_without_groupby(arrow_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);
}

template <>
std::unique_ptr<ral::frame::BlazingTable> aggregations_without_groupby_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return ral::operators::compute_aggregations_without_groupby(cudf_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);
}

struct aggregations_with_groupby_functor {
  template <typename T>
  std::unique_ptr<ral::frame::BlazingTable> operator()(
      std::shared_ptr<ral::frame::BlazingTableView> table_view,
      std::vector<std::string> aggregation_input_expressions,
      std::vector<AggregateKind> aggregation_types,
      std::vector<std::string> aggregation_column_assigned_aliases,
      std::vector<int> group_column_indices) const
  {
    // TODO percy arrow thrown error
    throw std::runtime_error("ERROR: This default dispatcher operator should not be called.");
    return nullptr;
  }
};

template <>
std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby_functor::operator()<ral::frame::BlazingArrowTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases,
    std::vector<int> group_column_indices) const
{
  throw std::runtime_error("ERROR: BlazingSQL doesn't support this Arrow operator yet.");
  return nullptr;
}

template <>
std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby_functor::operator()<ral::frame::BlazingCudfTable>(
    std::shared_ptr<ral::frame::BlazingTableView> table_view,
    std::vector<std::string> aggregation_input_expressions,
    std::vector<AggregateKind> aggregation_types,
    std::vector<std::string> aggregation_column_assigned_aliases,
    std::vector<int> group_column_indices) const
{
  auto cudf_table_view = std::dynamic_pointer_cast<ral::frame::BlazingCudfTableView>(table_view);
  return ral::operators::compute_aggregations_with_groupby(cudf_table_view, aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases, group_column_indices);
}

ral::execution::task_result ComputeAggregateKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    try{
        auto & input = inputs[0];
        std::unique_ptr<ral::frame::BlazingTable> columns;
        if(this->aggregation_types.size() == 0) {
            columns = ral::execution::backend_dispatcher(
                      input->get_execution_backend(),
                      groupby_without_aggregations_functor(),
                      input->to_table_view(),
                      this->group_column_indices);
        } else if (this->group_column_indices.size() == 0) {
            columns = ral::execution::backend_dispatcher(
                        input->get_execution_backend(),
                        aggregations_without_groupby_functor(),
                        input->to_table_view(),
                        this->aggregation_input_expressions,
                        this->aggregation_types,
                        aggregation_column_assigned_aliases);
        } else {
            columns = ral::execution::backend_dispatcher(
                        input->get_execution_backend(),
                        aggregations_with_groupby_functor(),
                        input->to_table_view(),
                        this->aggregation_input_expressions,
                        this->aggregation_types,
                        aggregation_column_assigned_aliases,
                        group_column_indices);
        }
        output->addToCache(std::move(columns));
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus ComputeAggregateKernel::run() {
    CodeTimer timer;

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
    RAL_EXPECTS(cache_data != nullptr, "In ComputeAggregateKernel: The input cache data cannot be null");

    // in case UNION exists, we want to know the num of columns
    std::tie(this->group_column_indices, this->aggregation_input_expressions, this->aggregation_types,
        aggregation_column_assigned_aliases) = ral::operators::parseGroupByExpression(this->expression, cache_data->num_columns());

    while(cache_data != nullptr ){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        cache_data = this->input_cache()->pullCacheData();
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Compute Aggregate Kernel tasks created",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    }

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
    });

    if(auto ep = ral::execution::executor::get_instance()->last_exception()){
        std::rethrow_exception(ep);
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="ComputeAggregate Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }
    return kstatus::proceed;
}

std::pair<bool, uint64_t> ComputeAggregateKernel::get_estimated_output_num_rows(){
    if(this->aggregation_types.size() > 0 && this->group_column_indices.size() == 0) { // aggregation without groupby
        return std::make_pair(true, 1);
    } else {
        std::pair<bool, uint64_t> total_in = this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
        if (total_in.first){
            double out_so_far = (double)this->output_.total_rows_added();
            double in_so_far = (double)this->total_input_rows_processed;
            if (in_so_far == 0) {
                return std::make_pair(false, 0);
            } else {
                return std::make_pair(true, (uint64_t)( ((double)total_in.second) *out_so_far/in_so_far) );
            }
        } else {
            return std::make_pair(false, 0);
        }
    }
}

// END ComputeAggregateKernel


std::vector<std::shared_ptr<ral::frame::BlazingTableView>> prepare_partitions(std::shared_ptr<ral::frame::BlazingTableView> table_view,
                                                                                                        int num_partitions,
                                                                              std::vector<cudf::size_type> columns_to_hash)
{
    std::vector<std::shared_ptr<ral::frame::BlazingTableView>> partitioned;
    std::unique_ptr<ral::frame::BlazingTable> hashed_data; // Keep table alive in this scope
    if (table_view->num_rows() > 0) {
        std::vector<cudf::size_type> hashed_data_offsets;
        std::tie(hashed_data, hashed_data_offsets) = ral::execution::backend_dispatcher(table_view->get_execution_backend(),
                                                                                       hash_partition_functor(), table_view, columns_to_hash, num_partitions);
        
        // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
        std::vector<cudf::size_type> split_indexes(hashed_data_offsets.begin() + 1, hashed_data_offsets.end());
        partitioned = ral::execution::backend_dispatcher(hashed_data->get_execution_backend(), split_functor(), hashed_data->to_table_view(), split_indexes);
    } else {
        //  copy empty view
        for (auto i = 0; i < num_partitions; i++) {
            partitioned.push_back(table_view);
        }
    }

    return partitioned;
}



/// compute_aggregations_without_groupby

// BEGIN DistributeAggregateKernel

DistributeAggregateKernel::DistributeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : distributing_kernel{kernel_id, queryString, context, kernel_type::DistributeAggregateKernel} {
    this->query_graph = query_graph;
    set_number_of_message_trackers(1); //default
}

ral::execution::task_result DistributeAggregateKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    auto & input = inputs[0];

    // num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future.
    // If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
    int num_partitions = this->context->getTotalNodes();

    // If its an aggregation without group by we want to send all the results to the master node
    auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
    if (group_column_indices.size() == 0) {
        try{
            if(this->context->isMasterNode(self_node)) {
                bool added = this->output_.get_cache()->addToCache(std::move(input),"",false);
                if (added) {
                    increment_node_count(self_node.id());
                }
            } else {
                if (!set_empty_part_for_non_master_node){ // we want to keep in the non-master nodes something, so that the cache is not empty

                    std::unique_ptr<ral::frame::BlazingTable> empty = ral::execution::backend_dispatcher(
                                                                        input->get_execution_backend(),
                                                                        create_empty_table_like_functor(),
                                                                        input->to_table_view());

                    bool added = this->add_to_output_cache(std::move(empty), "", true);
                    set_empty_part_for_non_master_node = true;
                    if (added) {
                        increment_node_count(self_node.id());
                    }
                }

                send_message(std::move(input),
                    true, //specific_cache
                    "", //cache_id
                    {this->context->getMasterNode().id()}); //target_id
            }
        }catch(const rmm::bad_alloc& e){
            return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
        }catch(const std::exception& e){
            return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
        }

    } else {

        try{
            auto partitions = prepare_partitions(input->to_table_view(), num_partitions, this->columns_to_hash);

            scatter(partitions,
                output.get(),
                "", //message_id_prefix
                "" //cache_id
            );
        }catch(const rmm::bad_alloc& e){
            return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
        }catch(const std::exception& e){
            return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
        }
        
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus DistributeAggregateKernel::run() {
    CodeTimer timer;

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();

    // in case UNION exists, we want to know the num of columns
    std::tie(group_column_indices, aggregation_input_expressions, aggregation_types,
        aggregation_column_assigned_aliases) = ral::operators::parseGroupByExpression(this->expression, cache_data->num_columns());

    // we want to update the `columns_to_hash` because the input could have more columns after ComputeAggregateKernel
    std::tie(columns_to_hash, std::ignore, std::ignore, std::ignore) = ral::operators::modGroupByParametersPostComputeAggregations(group_column_indices,
                                                                                             aggregation_types, cache_data->column_names());

    while(cache_data != nullptr ){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        cache_data = this->input_cache()->pullCacheData();
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="DistributeAggregate Kernel tasks created",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    }

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
    });

    if(auto ep = ral::execution::executor::get_instance()->last_exception()){
        std::rethrow_exception(ep);
    }

    send_total_partition_counts(
        "", //message_prefix
        "" //cache_id
    );

    int total_count = get_total_partition_counts();
    this->output_cache()->wait_for_count(total_count);

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="DistributeAggregate Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END DistributeAggregateKernel

// BEGIN MergeAggregateKernel

MergeAggregateKernel::MergeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::MergeAggregateKernel} {
    this->query_graph = query_graph;
}

ral::execution::task_result MergeAggregateKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    try{
        std::vector<std::shared_ptr<ral::frame::BlazingTableView>> tableViewsToConcat;
        for (std::size_t i = 0; i < inputs.size(); i++){
            tableViewsToConcat.emplace_back(inputs[i]->to_table_view());
        }

        CodeTimer eventTimer;
        if( ral::utilities::checkIfConcatenatingStringsWillOverflow(tableViewsToConcat)) {
            if(logger) {
                logger->warn("{query_id}|{step}|{substep}|{info}",
                                "query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
                                "step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
                                "substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
                                "info"_a="In MergeAggregateKernel::run Concatenating Strings will overflow strings length");
            }
        }
        auto concatenated = ral::utilities::concatTables(tableViewsToConcat);

        auto log_input_num_rows = concatenated ? concatenated->num_rows() : 0;
        auto log_input_num_bytes = concatenated ? concatenated->size_in_bytes() : 0;

        std::vector<int> group_column_indices;
        std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases;
        std::vector<AggregateKind> aggregation_types;
        std::tie(group_column_indices, aggregation_input_expressions, aggregation_types,
            aggregation_column_assigned_aliases) = ral::operators::parseGroupByExpression(this->expression,concatenated->num_columns());

        std::vector<int> mod_group_column_indices;
        std::vector<std::string> mod_aggregation_input_expressions, mod_aggregation_column_assigned_aliases, merging_column_names;
        std::vector<AggregateKind> mod_aggregation_types;
        std::tie(mod_group_column_indices, mod_aggregation_input_expressions, mod_aggregation_types,
            mod_aggregation_column_assigned_aliases) = ral::operators::modGroupByParametersPostComputeAggregations(
            group_column_indices, aggregation_types, concatenated->column_names());

        std::unique_ptr<ral::frame::BlazingTable> columns = nullptr;
        if(aggregation_types.size() == 0) {
            columns = ral::execution::backend_dispatcher(
                    concatenated->get_execution_backend(),
                    groupby_without_aggregations_functor(),
                    concatenated->to_table_view(),
                    mod_group_column_indices);
        } else if (group_column_indices.size() == 0) {
            // aggregations without groupby are only merged on the master node
            if( context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode()) ) {
                columns = ral::execution::backend_dispatcher(
                        concatenated->get_execution_backend(),
                        aggregations_without_groupby_functor(),
                        concatenated->to_table_view(),
                        mod_aggregation_input_expressions,
                        mod_aggregation_types,
                        mod_aggregation_column_assigned_aliases);
            } else {
                // with aggregations without groupby the distribution phase should deposit an empty dataframe with the right schema into the cache, which is then output here
                columns = std::move(concatenated);
            }
        } else {
            columns = ral::execution::backend_dispatcher(
                        concatenated->get_execution_backend(),
                        aggregations_with_groupby_functor(),
                        concatenated->to_table_view(),
                        mod_aggregation_input_expressions,
                        mod_aggregation_types,
                        mod_aggregation_column_assigned_aliases,
                        mod_group_column_indices);
        }
        eventTimer.stop();

        auto log_output_num_rows = columns->num_rows();
        auto log_output_num_bytes = columns->size_in_bytes();

        output->addToCache(std::move(columns));
        columns = nullptr;
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus MergeAggregateKernel::run() {
    CodeTimer timer;

    // This Kernel needs all of the input before it can do any output. So lets wait until all the input is available
    this->input_cache()->wait_until_finished();

    int batch_count=0;
    try {
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;

        while(this->input_cache()->wait_for_next()){
            std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();

            if(cache_data != nullptr){
                inputs.push_back(std::move(cache_data));
                batch_count++;
            }
        }

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        if(logger){
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="Merge Aggregate Kernel tasks created",
                                        "duration"_a=timer.elapsed_time(),
                                        "kernel_id"_a=this->get_id());
        }

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock,[this]{
            return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
        });

        if(auto ep = ral::execution::executor::get_instance()->last_exception()){
            std::rethrow_exception(ep);
        }
    } catch(const std::exception& e) {
        if(logger){
            logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                        "query_id"_a=context->getContextToken(),
                        "step"_a=context->getQueryStep(),
                        "substep"_a=context->getQuerySubstep(),
                        "info"_a="In MergeAggregate kernel for {}. What: {}"_format(expression, e.what()),
                        "duration"_a="");
        }
        throw;
    }

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="MergeAggregate Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END MergeAggregateKernel

} // namespace batch
} // namespace ral
