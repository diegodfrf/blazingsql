#include "BatchProjectionProcessing.h"
#include "operators/LogicalProject.h"
#include "execution_graph/Context.h"
#include "execution_graph/executor.h"

namespace ral {
namespace batch {

using Context = blazingdb::manager::Context;
// BEGIN Projection

Projection::Projection(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::ProjectKernel)
{
    this->query_graph = query_graph;
}

ral::execution::task_result Projection::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    try{
        auto & input = inputs[0];
        auto columns = ral::operators::process_project(std::move(input), expression, this->context.get());
        output->addToCache(std::move(columns));
    }catch(const rmm::bad_alloc& e){
        //can still recover if the input was not a GPUCacheData
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus Projection::run() {
    CodeTimer timer;

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
    RAL_EXPECTS(cache_data != nullptr, "ERROR: Projection::run() first input CacheData was nullptr");

    // When this kernel will project all the columns (with or without aliases)
    // we want to avoid caching and decahing for this kernel
    bool bypassing_project, bypassing_project_with_aliases;
    std::vector<std::string> aliases;
    std::vector<std::string> column_names = cache_data->column_names();
    std::tie(bypassing_project, bypassing_project_with_aliases, aliases) = bypassingProject(this->expression, column_names);

    while(cache_data != nullptr){
        if (bypassing_project_with_aliases) {
            cache_data->set_names(aliases);
            this->add_to_output_cache(std::move(cache_data));
        } else if (bypassing_project) {
            this->add_to_output_cache(std::move(cache_data));
        } else {
            std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(cache_data));

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    this->output_cache(),
                    this);
        }
        cache_data = this->input_cache()->pullCacheData();
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Projection Kernel tasks created",
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
                                "info"_a="Projection Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    }
    return kstatus::proceed;
}

// END Projection

} // namespace batch
} // namespace ral