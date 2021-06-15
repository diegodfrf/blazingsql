#pragma once

#include "execution_kernels/kernel.h"

using namespace ral::cache;
using namespace blazingdb::manager;

namespace ral {
namespace batch {

/**
 * @brief This kernel filters the data according to the specified conditions.
 */
class Filter : public kernel {
public:
    /**
     * Constructor for TableScan
     * @param kernel_id Kernel identifier.
     * @param queryString Original logical expression that the kernel will execute.
     * @param context Shared context associated to the running query.
     * @param query_graph Shared pointer of the current execution graph.
     */
    Filter(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

    std::string kernel_name() { return "Filter";}

#ifdef CUDF_SUPPORT
    ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
        std::shared_ptr<ral::cache::CacheMachine> output,
        cudaStream_t stream, const std::map<std::string, std::string>& args) override;
#else
    ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
        std::shared_ptr<ral::cache::CacheMachine> output,
        const std::map<std::string, std::string>& args) override;
#endif

    /**
     * Executes the batch processing.
     * Loads the data from their input port, and after processing it,
     * the results are stored in their output port.
     * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
     */
    kstatus run() override;

    /**
     * Returns the estimated num_rows for the output at one point.
     * @return A pair representing that there is no data to be processed, or the estimated number of output rows.
     */
    std::pair<bool, uint64_t> get_estimated_output_num_rows() override;
};

} // namespace batch
} // namespace ral