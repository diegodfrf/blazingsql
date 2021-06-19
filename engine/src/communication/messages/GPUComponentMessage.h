#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <transport/ColumnTransport.h>
#include <transport/Node.h>

#ifdef CUDF_SUPPORT
#include <cudf/copying.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/types.hpp>
#include <cudf/strings/strings_column_view.hpp>

#include <communication/messages/MessageUtil.cuh>
#endif

#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <tuple>

#include <blazing_table/BlazingHostTable.h>
namespace ral {
namespace communication {
namespace messages {


using Node = blazingdb::transport::Node;
using ColumnTransport = blazingdb::transport::ColumnTransport;

#ifdef CUDF_SUPPORT
using gpu_raw_buffer_container = std::tuple<std::vector<std::size_t>, std::vector<const char *>,
											std::vector<ColumnTransport>,
											std::vector<std::unique_ptr<rmm::device_buffer>> >;

gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(std::shared_ptr<ral::frame::BlazingCudfTableView> table_view);

std::unique_ptr<ral::frame::BlazingHostTable> serialize_gpu_message_to_host_table(std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, bool use_pinned = false);
#endif

std::unique_ptr<ral::frame::BlazingHostTable> serialize_arrow_message_to_host_table(std::shared_ptr<ral::frame::BlazingArrowTableView> table_view, bool use_pinned = false);



}  // namespace messages
}  // namespace communication
}  // namespace ral
