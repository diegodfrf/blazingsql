#include "BatchProcessing.h"
#include "utilities/CodeTimer.h"
#include "communication/CommunicationData.h"
#include "ExceptionHandling/BlazingThread.h"
#include "parser/expression_utils.hpp"
#include "execution_graph/executor.h"
#include <cudf/types.hpp>
#include <src/utilities/DebuggingUtils.h>
#include "io/data_provider/sql/AbstractSQLDataProvider.h"
#include "compute/backend_dispatcher.h"
#include "parser/project_parser_utils.h"

namespace ral {
namespace batch {

// BEGIN BatchSequence

BatchSequence::BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache, const ral::cache::kernel * kernel, bool ordered)
: cache{cache}, kernel{kernel}, ordered{ordered}
{}

void BatchSequence::set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
    this->cache = cache;
}

std::unique_ptr<ral::cache::CacheData> BatchSequence::next() {
    if (ordered) {
        return cache->pullCacheData();
    } else {
      // TODO percy arrow william
      throw std::runtime_error("ERROR: BatchSequence::next BlazingSQL doesn't support this Arrow operator yet.");
      //return cache->pullUnorderedFromCache();
    }
}

bool BatchSequence::wait_for_next() {
    if (kernel) {
        std::string message_id = std::to_string((int)kernel->get_type_id()) + "_" + std::to_string(kernel->get_id());
    }

    return cache->wait_for_next();
}

bool BatchSequence::has_next_now() {
    return cache->has_next_now();
}

// END BatchSequence

// BEGIN BatchSequenceBypass

BatchSequenceBypass::BatchSequenceBypass(std::shared_ptr<ral::cache::CacheMachine> cache, const ral::cache::kernel * kernel)
: cache{cache}, kernel{kernel}
{}

void BatchSequenceBypass::set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
    this->cache = cache;
}

std::unique_ptr<ral::cache::CacheData> BatchSequenceBypass::next() {
    return cache->pullCacheData();
}

bool BatchSequenceBypass::wait_for_next() {
    return cache->wait_for_next();
}

bool BatchSequenceBypass::has_next_now() {
    return cache->has_next_now();
}

// END BatchSequenceBypass

// BEGIN Print

kstatus Print::run() {
    // WSM TODO need to reimplement this without using BatchSequence
    // std::lock_guard<std::mutex> lg(print_lock);
    // BatchSequence input(this->input_cache(), this);
    // while (input.wait_for_next() ) {
    //     auto batch = input.next();
    //     // TODO percy rommel arrow
    //     //ral::utilities::print_blazing_cudf_table_view(batch->to_table_view());
    // }
    return kstatus::stop;
}

// END Print

// BEGIN OutputKernel

kstatus OutputKernel::run() {
  ral::execution::backend_id output_type = ral::execution::backend_id::CUDF;
  if (this->context->output_type() == "pandas") {
    output_type = ral::execution::backend_id::ARROW;
  }

    while (this->input_.get_cache()->wait_for_next()) {
        std::unique_ptr<frame::BlazingTable> temp_output = this->input_.get_cache()->pullFromCache(ral::execution::execution_backend(output_type));

        if(temp_output){
            output.emplace_back(std::move(temp_output));
        }
    }
    done = true;

    return kstatus::stop;
}

frame_type OutputKernel::release() {
    return std::move(output);
}

bool OutputKernel::is_done() {
    return done.load();
}

// END OutputKernel

} // namespace batch
} // namespace ral
