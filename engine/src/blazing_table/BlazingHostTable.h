#pragma once

#include <vector>
#include <string>
#include <memory>
#include "cudf/types.hpp"
#include "transport/ColumnTransport.h"
#include "bmr/BufferProvider.h"
#include "execution_kernels/LogicPrimitives.h"

namespace ral {
namespace frame {

using ColumnTransport = blazingdb::transport::ColumnTransport;

/**
	@brief A class that represents the BlazingTable store in host memory.
    This implementation uses only raw allocations, ColumnTransports and chunked_column_infos that represent a BlazingTable.
    The reference to implement this class was based on the way how BlazingTable objects are send/received 
    by the communication library.
*/ 
class BlazingHostTable {
public:

    // for data from cudf
    BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
        std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
        std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations);

    // for data from arrow
    BlazingHostTable(std::shared_ptr<arrow::Table> arrow_table);

    ~BlazingHostTable();

    std::vector<cudf::data_type> get_schema() const;

    std::vector<std::string> names() const;

    void set_names(std::vector<std::string> names);

    cudf::size_type num_rows() const ;

    cudf::size_type num_columns() const ;

    std::size_t sizeInBytes() ;

    void setPartitionId(const size_t &part_id) ;

    size_t get_part_id() ;

    const std::vector<ColumnTransport> & get_columns_offsets() const ;

    std::unique_ptr<BlazingCudfTable> get_cudf_table() const;
    std::unique_ptr<BlazingArrowTable> get_arrow_table() const;

    std::vector<ral::memory::blazing_allocation_chunk> get_raw_buffers() const;

    const std::vector<ral::memory::blazing_chunked_column_info> &  get_blazing_chunked_column_infos() const;

    bool is_arrow() const { return (this->arrow_table != nullptr); }

private:
    std::vector<ColumnTransport> columns_offsets;
    std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos;
    std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> allocations;
    size_t part_id;
    std::shared_ptr<arrow::Table> arrow_table;
};

}  // namespace frame
}  // namespace ral
