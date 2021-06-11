#pragma once

#include <vector>
#include <string>
#include <memory>
#include "cudf/types.hpp"
#include "transport/ColumnTransport.h"
#include "bmr/BufferProvider.h"
#include "blazing_table/BlazingCudfTable.h"
#include "blazing_table/BlazingArrowTable.h"

namespace ral {
namespace frame {

using ColumnTransport = blazingdb::transport::ColumnTransport;

/**
	@brief A class that represents the BlazingTable store in host memory.
    This implementation uses only raw allocations, ColumnTransports and chunked_column_infos that represent a BlazingTable.
    The reference to implement this class was based on the way how BlazingTable objects are send/received 
    by the communication library.
*/ 
class BlazingHostTable  {
public:

    // for data from cudf
    BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
        std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
        std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations);

    ~BlazingHostTable();

    std::vector<std::shared_ptr<arrow::DataType>> column_types() const;

    std::vector<std::string> column_names() const;

    void set_column_names(std::vector<std::string> names);

    cudf::size_type num_rows() const ;

    cudf::size_type num_columns() const ;

    std::size_t size_in_bytes() const;

    void setPartitionId(const size_t &part_id) ;

    size_t get_part_id();

    std::unique_ptr<BlazingHostTable> clone();

    const std::vector<ColumnTransport> & get_columns_offsets() const ;

    std::unique_ptr<BlazingCudfTable> get_cudf_table() const;
    std::unique_ptr<BlazingArrowTable> get_arrow_table() const;

    std::vector<ral::memory::blazing_allocation_chunk> get_raw_buffers() const;

    const std::vector<ral::memory::blazing_chunked_column_info> &  get_blazing_chunked_column_infos() const;

private:
    std::vector<ColumnTransport> columns_offsets;
    std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos;
    std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> allocations;
    size_t part_id;
};

}  // namespace frame
}  // namespace ral
