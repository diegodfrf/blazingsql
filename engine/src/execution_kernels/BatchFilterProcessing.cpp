#include "BatchFilterProcessing.h"
#include <src/operators/LogicalFilter.h>
#include "execution_graph/executor.h"
#include "cache_machine/CacheData.h"

namespace ral {
namespace batch {

// BEGIN Filter

Filter::Filter(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::FilterKernel)
{
    this->query_graph = query_graph;
}

#ifdef CUDF_SUPPORT
ral::execution::task_result Filter::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
#else
ral::execution::task_result Filter::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    const std::map<std::string, std::string>& /*args*/) {
#endif
    std::unique_ptr<ral::frame::BlazingTable> columns;
    try{
        auto & input = inputs[0];
        columns = ral::operators::process_filter(input->to_table_view(), expression, this->context.get());
        output->addToCache(std::move(columns));
#ifdef CUDF_SUPPORT
    }catch(const rmm::bad_alloc& e){
#else
    }catch(const std::bad_alloc& e){
#endif
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus Filter::run() {
    CodeTimer timer;

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
    while(cache_data != nullptr){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        cache_data = this->input_cache()->pullCacheData();
    }

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="Filter Kernel tasks created",
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
                                    "info"_a="Filter Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

std::pair<bool, uint64_t> Filter::get_estimated_output_num_rows(){
    std::pair<bool, uint64_t> total_in = this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
    if (total_in.first){
        double out_so_far = (double)this->output_.total_rows_added();
        double in_so_far = (double)this->total_input_rows_processed;
        if (in_so_far == 0){
            return std::make_pair(false, 0);
        } else {
            return std::make_pair(true, (uint64_t)( ((double)total_in.second) *out_so_far/in_so_far) );
        }
    } else {
        return std::make_pair(false, 0);
    }
}

// END Filter

} // namespace batch
} // namespace ral