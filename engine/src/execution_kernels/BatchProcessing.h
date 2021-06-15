#pragma once

#include <mutex>
#include <typeinfo>
#include "cache_machine/CacheMachine.h"
#include "execution_graph/graph.h"
#include "io/Schema.h"
#include "io/DataLoader.h"
#include "execution_kernels/kernel.h"
#include "blazing_table/BlazingTable.h"

#include "cache_machine/CacheDataIO.h"
#include "cache_machine/ArrowCacheData.h"

#include "io/data_parser/CSVParser.h"
#include "io/data_parser/JSONParser.h"
#include "io/data_parser/OrcParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_parser/ArrowParser.h"

namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using namespace fmt::literals;

using frame_type = std::vector<std::unique_ptr<ral::frame::BlazingTable>>;
using Context = blazingdb::manager::Context;

/**
 * @brief This is the standard data sequencer that just pulls data from an input cache one batch at a time.
 */
class BatchSequence {
public:
    /**
     * Constructor for the BatchSequence
     * @param cache The input cache from where the data will be pulled.
     * @param kernel The kernel that will actually receive the pulled data.
     * @param ordered Indicates whether the order should be kept at data pulling.
     */
    BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr, bool ordered = true);

    /**
     * Updates the input cache machine.
     * @param cache The pointer to the new input cache.
     */
    void set_source(std::shared_ptr<ral::cache::CacheMachine> cache);

    /**
     * Get the next message as a unique pointer to a BlazingTable.
     * If there are no more messages on the queue we get a nullptr.
     * @return Unique pointer to a BlazingTable containing the next decached message.
     */
    std::unique_ptr<ral::cache::CacheData> next();

    /**
     * Blocks executing thread until a new message is ready or when the message queue is empty.
     * @return true A new message is ready.
     * @return false There are no more messages on the cache.
     */
    bool wait_for_next();

    /**
     * Indicates if the message queue is not empty at this point on time.
     * @return true There is at least one message in the queue.
     * @return false Message queue is empty.
     */
    bool has_next_now();

private:
    std::shared_ptr<ral::cache::CacheMachine> cache; /**< Cache machine from which the data will be pulled. */
    const ral::cache::kernel * kernel; /**< Pointer to the kernel that will receive the cache data. */
    bool ordered; /**< Indicates whether the order should be kept when pulling data from the cache. */
};

/**
 * @brief This data sequencer works as a bypass to take data from one input to an output without decacheing.
 */
class BatchSequenceBypass {
public:
    /**
     * Constructor for the BatchSequenceBypass
     * @param cache The input cache from where the data will be pulled.
     * @param kernel The kernel that will actually receive the pulled data.
     */
    BatchSequenceBypass(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr);

    /**
     * Updates the input cache machine.
     * @param cache The pointer to the new input cache.
     */
    void set_source(std::shared_ptr<ral::cache::CacheMachine> cache);

    /**
     * Get the next message as a CacheData object.
     * @return CacheData containing the next message without decacheing.
     */
    std::unique_ptr<ral::cache::CacheData> next();

    /**
     * Blocks executing thread until a new message is ready or when the message queue is empty.
     * @return true A new message is ready.
     * @return false There are no more messages on the cache.
     */
    bool wait_for_next();

    /**
     * Indicates if the message queue is not empty at this point on time.
     * @return true There is at least one message in the queue.
     * @return false Message queue is empty.
     */
    bool has_next_now();

private:
    std::shared_ptr<ral::cache::CacheMachine> cache; /**< Cache machine from which the data will be pulled. */
    const ral::cache::kernel * kernel; /**< Pointer to the kernel that will receive the cache data. */
};


/**
 * @brief This kernel allows printing the preceding input caches to the standard output.
 */
class Print : public kernel {
public:
    /**
     * Constructor
     */
    Print() : kernel(0,"Print", nullptr, kernel_type::PrintKernel) { ofs = &(std::cout); }
    Print(std::ostream & stream) : kernel(0,"Print", nullptr, kernel_type::PrintKernel) { ofs = &stream; }

    std::string kernel_name() { return "Print";}

    /**
     * Executes the batch processing.
     * Loads the data from their input port, and after processing it,
     * the results are stored in their output port.
     * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
     */
    virtual kstatus run();

protected:
    std::ostream * ofs = nullptr; /**< Target output stream object. */
    std::mutex print_lock; /**< Mutex for making the printing thread-safe. */
};


/**
 * @brief This kernel represents the last step of the execution graph.
 * Basically it allows to extract the result of the different levels of
 * memory abstractions in the form of a concrete table.
 */
class OutputKernel : public kernel {
public:
    /**
     * Constructor for OutputKernel
     * @param kernel_id Kernel identifier.
     * @param context Shared context associated to the running query.
     */
    OutputKernel(std::size_t kernel_id, std::shared_ptr<Context> context) : kernel(kernel_id,"OutputKernel", context, kernel_type::OutputKernel), done(false) { }

    std::string kernel_name() { return "Output";}

#ifdef CUDF_SUPPORT
    ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > /*inputs*/,
        std::shared_ptr<ral::cache::CacheMachine> /*output*/,
        cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) override {
#else
    ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > /*inputs*/,
        std::shared_ptr<ral::cache::CacheMachine> /*output*/,
        const std::map<std::string, std::string>& /*args*/) override {
#endif
            //for now the output kernel is not using do_process
            //i believe the output should be a cachemachine itself
            //obviating this concern
            ral::execution::task_result temp = {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> >()};
            return std::move(temp);
        }
    /**
     * Executes the batch processing.
     * Loads the data from their input port, and after processing it,
     * the results are stored in their output port.
     * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
     */
    kstatus run() override;

    /**
     * Returns the vector containing the final processed output.
     * @return frame_type A vector of unique_ptr of BlazingTables.
     */
    frame_type release();


    /**
     * Returns true when the OutputKernel is done
     */
    bool is_done();

protected:
    frame_type output; /**< Vector of tables with the final output. */
    std::atomic<bool> done;
};

} // namespace batch
} // namespace ral
