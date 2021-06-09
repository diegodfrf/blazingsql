#pragma once

#include "execution_kernels/kernel.h"

using namespace ral::cache;
using namespace blazingdb::manager;

namespace ral {
namespace batch {

/**
 * @brief This kernel only returns the subset columns contained in the logical projection expression.
 */
class Projection : public kernel {
public:
    /**
     * Constructor for Projection
     * @param kernel_id Kernel identifier.
     * @param queryString Original logical expression that the kernel will execute.
     * @param context Shared context associated to the running query.
     * @param query_graph Shared pointer of the current execution graph.
     */
    Projection(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

    std::string kernel_name() { return "Projection";}

    ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
        std::shared_ptr<ral::cache::CacheMachine> output,
        cudaStream_t stream, const std::map<std::string, std::string>& args) override;

    /**
     * Executes the batch processing.
     * Loads the data from their input port, and after processing it,
     * the results are stored in their output port.
     * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
     */
    kstatus run() override;
};

} // namespace batch
} // namespace ral