#include "BatchScanProcessing.h"
#include <src/operators/LogicalFilter.h>
#include "io/data_parser/CSVParser.h"
#include "execution_graph/executor.h"
#include "cache_machine/common/CacheDataIO.h"

#ifdef MYSQL_SUPPORT
#include "io/data_provider/sql/MySQLDataProvider.h"
#endif

#ifdef POSTGRESQL_SUPPORT
#include "io/data_provider/sql/PostgreSQLDataProvider.h"
#endif

#ifdef SQLITE_SUPPORT
#include "io/data_provider/sql/SQLiteDataProvider.h"
#endif

#ifdef SNOWFLAKE_SUPPORT
#include "io/data_provider/sql/SnowFlakeDataProvider.h"
#endif

#include "parser/project_parser_utils.h"
#include "compute/api.h"

namespace ral {
namespace batch {


// BEGIN TableScan

TableScan::TableScan(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<ral::io::data_provider> provider, std::shared_ptr<ral::io::data_parser> parser, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::TableScanKernel), provider(provider), parser(parser), schema(schema), num_batches(0)
{
    if (parser->type() == ral::io::DataType::CUDF || parser->type() == ral::io::DataType::DASK_CUDF){
        num_batches = std::max(provider->get_num_handles(), (size_t)1);
    } else if (parser->type() == ral::io::DataType::MYSQL)	{
#ifdef MYSQL_SUPPORT
      ral::io::set_sql_projections<ral::io::mysql_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns()));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support MySQL integration");
#endif
    } else if (parser->type() == ral::io::DataType::POSTGRESQL)	{
#ifdef POSTGRESQL_SUPPORT
      ral::io::set_sql_projections<ral::io::postgresql_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns()));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support PostgreSQL integration");
#endif
    } else if (parser->type() == ral::io::DataType::SQLITE)	{
#ifdef SQLITE_SUPPORT
      ral::io::set_sql_projections<ral::io::sqlite_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns()));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support SQLite integration");
#endif
    } else if (parser->type() == ral::io::DataType::SNOWFLAKE)	{
#ifdef SNOWFLAKE_SUPPORT
      ral::io::set_sql_projections<ral::io::snowflake_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns()));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support SnowFlake integration");
#endif
    } else {
        num_batches = provider->get_num_handles();
    }

    this->query_graph = query_graph;
}

#ifdef CUDF_SUPPORT
ral::execution::task_result TableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
#else
ral::execution::task_result TableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    const std::map<std::string, std::string>& /*args*/) {
#endif

    try{
        output->addToCache(std::move(inputs[0]));
#ifdef CUDF_SUPPORT
    }catch(const rmm::bad_alloc& e){
#else
    }catch(const std::bad_alloc& e){
#endif
        //can still recover if the input was not a GPUCacheData
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus TableScan::run() {
    CodeTimer timer;

    std::vector<int> projections(schema.get_num_columns());
    std::iota(projections.begin(), projections.end(), 0);

    //if its empty we can just add it to the cache without scheduling
    if (!provider->has_next()) {
        this->add_to_output_cache(ral::execution::backend_dispatcher(this->context->preferred_compute(),
                                                                     create_empty_table_functor(),
                                                                     schema.get_names(), schema.get_dtypes(), projections));
    } else {
        bool is_batched_csv = false;
        while(provider->has_next()) {
            //retrieve the file handle but do not open the file
            //this will allow us to prevent from having too many open file handles by being
            //able to limit the number of file tasks
            auto handle = provider->get_next(true);
            auto file_schema = schema.fileSchema(file_index);
            auto row_group_ids = schema.get_rowgroup_ids(file_index);
            //this is the part where we make the task now

            std::unique_ptr<ral::cache::CacheData> input = std::make_unique<ral::cache::CacheDataIO>(handle, parser, schema, file_schema, row_group_ids, projections);

            std::vector<std::unique_ptr<ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(input));

            auto output_cache = this->output_cache();
            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    output_cache,
                    this);

            file_index++;
        }

        if(logger) {
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="TableScan Kernel tasks created",
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
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="TableScan Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

std::pair<bool, uint64_t> TableScan::get_estimated_output_num_rows(){
    double rows_so_far = (double)this->output_.total_rows_added();
    double batches_so_far = (double)this->output_.total_batches_added();
    if (batches_so_far == 0 || num_batches == 0) {
        return std::make_pair(false, 0);
    } else {
        return std::make_pair(true, (uint64_t)(rows_so_far/(batches_so_far/((double)num_batches))));
    }
}

// END TableScan

// BEGIN BindableTableScan

BindableTableScan::BindableTableScan(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<ral::io::data_provider> provider, std::shared_ptr<ral::io::data_parser> parser, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::BindableTableScanKernel), provider(provider), parser(parser), schema(schema) {
    this->query_graph = query_graph;
    this->filterable = is_filtered_bindable_scan(expression);
    this->predicate_pushdown_done = false;

    if (parser->type() == ral::io::DataType::CUDF || parser->type() == ral::io::DataType::DASK_CUDF){
        num_batches = std::max(provider->get_num_handles(), (size_t)1);
    } else if (parser->type() == ral::io::DataType::MYSQL) {
#ifdef MYSQL_SUPPORT
      ral::io::set_sql_projections<ral::io::mysql_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns(), queryString));
      predicate_pushdown_done = ral::io::set_sql_predicate_pushdown<ral::io::mysql_data_provider>(provider.get(), queryString);
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support MySQL integration");
#endif
    } else if (parser->type() == ral::io::DataType::POSTGRESQL)	{
#ifdef POSTGRESQL_SUPPORT
      ral::io::set_sql_projections<ral::io::postgresql_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns(), queryString));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support PostgreSQL integration");
#endif
    } else if (parser->type() == ral::io::DataType::SQLITE)	{
#ifdef SQLITE_SUPPORT
      ral::io::set_sql_projections<ral::io::sqlite_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns(), queryString));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support SQLite integration");
#endif
    } else if (parser->type() == ral::io::DataType::SNOWFLAKE)	{
#ifdef SNOWFLAKE_SUPPORT
      ral::io::set_sql_projections<ral::io::snowflake_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns(), queryString));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support SnowFlake integration");
#endif
    } else {
        num_batches = provider->get_num_handles();
    }
}

#ifdef CUDF_SUPPORT
ral::execution::task_result BindableTableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
#else
ral::execution::task_result BindableTableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    const std::map<std::string, std::string>& /*args*/) {
#endif
    auto & input = inputs[0];
    std::unique_ptr<ral::frame::BlazingTable> filtered_input;

    try{
        if(this->filterable && !this->predicate_pushdown_done) {
            filtered_input = ral::operators::process_filter(input->to_table_view(), expression, this->context.get());
            filtered_input->set_column_names(fix_column_aliases(filtered_input->column_names(), expression));
            output->addToCache(std::move(filtered_input));
        } else {
            input->set_column_names(fix_column_aliases(input->column_names(), expression));
            output->addToCache(std::move(input));
        }
#ifdef CUDF_SUPPORT
    }catch(const rmm::bad_alloc& e){
#else
    }catch(const std::bad_alloc& e){
#endif
        //can still recover if the input was not a GPUCacheData
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus BindableTableScan::run() {
    CodeTimer timer;

    auto projections = get_projections_wrapper(schema.get_num_columns(), expression);

    //if its empty we can just add it to the cache without scheduling
    if (!provider->has_next()) {
        auto empty = ral::execution::backend_dispatcher(this->context->preferred_compute(),
                                                        create_empty_table_functor(),
                                                        schema.get_names(), schema.get_dtypes(), projections);
        empty->set_column_names(fix_column_aliases(empty->column_names(), expression));
        this->add_to_output_cache(std::move(empty));
    } else {
        bool is_batched_csv = false;
        while(provider->has_next()) {
            //retrieve the file handle but do not open the file
            //this will allow us to prevent from having too many open file handles by being
            //able to limit the number of file tasks
            auto handle = provider->get_next(true);
            auto file_schema = schema.fileSchema(file_index);
            auto row_group_ids = schema.get_rowgroup_ids(file_index);
            //this is the part where we make the task now

            std::unique_ptr<ral::cache::CacheData> input = std::make_unique<ral::cache::CacheDataIO>(handle, parser, schema, file_schema, row_group_ids, projections);

            std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
            inputs.push_back(std::move(input));
            auto output_cache = this->output_cache();
            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    output_cache,
                    this);

            file_index++;
        }

        if(logger){
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="BindableTableScan Kernel tasks created",
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
    }

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="BindableTableScan Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }
    return kstatus::proceed;
}

std::pair<bool, uint64_t> BindableTableScan::get_estimated_output_num_rows(){
    double rows_so_far = (double)this->output_.total_rows_added();
    double current_batch = (double)file_index;
    if (current_batch == 0 || num_batches == 0){
        return std::make_pair(false, 0);
    } else {
        return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/(double)num_batches)));
    }
}

// END BindableTableScan

} // namespace batch
} // namespace ral