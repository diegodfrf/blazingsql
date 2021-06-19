#pragma once

#include "execution_kernels/kernel.h"
#include "execution_graph/Context.h"

using namespace ral::cache;
using namespace blazingdb::manager;

namespace ral {
namespace batch {

/**
 * @brief This kernel loads the data from the specified data source.
 */
class TableScan : public kernel {
public:
    /**
     * Constructor for TableScan
     * @param kernel_id Kernel identifier.
     * @param queryString Original logical expression that the kernel will execute.
     * @param loader Data loader responsible for executing the batching load.
     * @param schema Table schema associated to the data to be loaded.
     * @param context Shared context associated to the running query.
     * @param query_graph Shared pointer of the current execution graph.
     */
    TableScan(std::size_t kernel_id, const std::string & queryString,
        std::shared_ptr<ral::io::data_provider> provider,
        std::shared_ptr<ral::io::data_parser> parser, ral::io::Schema & schema,
        std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

    std::string kernel_name() { return "TableScan";}

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

private:
    std::shared_ptr<ral::io::data_provider> provider;
    std::shared_ptr<ral::io::data_parser> parser;
    ral::io::Schema  schema; /**< Table schema associated to the data to be loaded. */
    size_t file_index = 0;
    size_t num_batches;
};

/**
 * @brief This kernel loads the data and delivers only columns that are requested.
 * It also filters the data if there are one or more filters, and sets their column aliases
 * accordingly.
 */
class BindableTableScan : public kernel {
public:
    /**
     * Constructor for BindableTableScan
     * @param kernel_id Kernel identifier.
     * @param queryString Original logical expression that the kernel will execute.
     * @param loader Data loader responsible for executing the batching load.
     * @param schema Table schema associated to the data to be loaded.
     * @param context Shared context associated to the running query.
     * @param query_graph Shared pointer of the current execution graph.
     */
    BindableTableScan(std::size_t kernel_id, const std::string & queryString,
        std::shared_ptr<ral::io::data_provider> provider, std::shared_ptr<ral::io::data_parser> parser,
        ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

    std::string kernel_name() { return "BindableTableScan";}

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

private:
    std::shared_ptr<ral::io::data_provider> provider;
    std::shared_ptr<ral::io::data_parser> parser;
    ral::io::Schema  schema; /**< Table schema associated to the data to be loaded. */
    size_t file_index = 0;
    size_t num_batches;
    bool filterable;
    bool predicate_pushdown_done;
};

} // namespace batch
} // namespace ral