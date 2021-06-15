#pragma once

#include <vector>
#include <map>
#include <memory>
#include <tuple>
#include <transport/ColumnTransport.h>

#ifdef CUDF_SUPPORT
#include <rmm/device_buffer.hpp>
#include "blazing_table/BlazingCudfTable.h"
#endif

namespace comm {


/**
 * @brief Deserializes column data and metadata into a BlazingTable
 *
 * @param columns_offsets A vector of ColumnTransport containing column metadata
 * @param raw_buffers A vector of device_buffer containing column data
 *
 * @returns A unique_ptr to BlazingTable created with data from the columns_offsets
 * and raw_buffers vectors.
 */
#ifdef CUDF_SUPPORT
std::unique_ptr<ral::frame::BlazingCudfTable> deserialize_from_gpu_raw_buffers(
  const std::vector<blazingdb::transport::ColumnTransport> & columns_offsets,
  const std::vector<rmm::device_buffer> & raw_buffers,
  cudaStream_t stream = 0);
#endif

} // namespace comm
