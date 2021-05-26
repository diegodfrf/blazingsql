#include "BlazingHostTable.h"
#include "bmr/BlazingMemoryResource.h"
#include "bmr/BufferProvider.h"
#include "communication/CommunicationInterface/serializer.hpp"
#include <arrow/array/builder_primitive.h>
#include <arrow/table.h>
#include "cudf/types.hpp"


using namespace fmt::literals;

namespace ral {
namespace frame {

// BEGIN internal utils

template <class B, class D>
static inline std::unique_ptr<B> unique_base(std::unique_ptr<D> && d) {
	if (B * b = static_cast<B *>(d.get())) {
		d.release();
		return std::unique_ptr<B>(b, std::move(d.get_deleter()));
	}
	throw std::bad_alloc{};
}

static inline std::tuple<std::unique_ptr<arrow::ArrayBuilder>,
	std::shared_ptr<arrow::Field>>
MakeArrayBuilderField(
	const blazingdb::transport::ColumnTransport & columnTransport) {
	const blazingdb::transport::ColumnTransport::MetaData & metadata =
		columnTransport.metadata;

	const cudf::type_id type_id = static_cast<cudf::type_id>(metadata.dtype);
	const std::string columnName{metadata.col_name};

	// TODO: get column name
	switch (type_id) {
	case cudf::type_id::INT8:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int8Builder>()),
			arrow::field(columnName, arrow::int8()));
	case cudf::type_id::INT16:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int16Builder>()),
			arrow::field(columnName, arrow::int16()));
	case cudf::type_id::INT32:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int32Builder>()),
			arrow::field(columnName, arrow::int32()));
	case cudf::type_id::INT64:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int64Builder>()),
			arrow::field(columnName, arrow::int64()));
	case cudf::type_id::FLOAT32:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::FloatBuilder>()),
			arrow::field(columnName, arrow::float32()));
	case cudf::type_id::FLOAT64:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::DoubleBuilder>()),
			arrow::field(columnName, arrow::float64()));
	case cudf::type_id::UINT8:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt8Builder>()),
			arrow::field(columnName, arrow::uint8()));
	case cudf::type_id::UINT16:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt16Builder>()),
			arrow::field(columnName, arrow::uint16()));
	case cudf::type_id::UINT32:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt32Builder>()),
			arrow::field(columnName, arrow::uint32()));
	case cudf::type_id::UINT64:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt64Builder>()),
			arrow::field(columnName, arrow::uint64()));
	default:
		throw std::runtime_error{
			"Unsupported column cudf type id for array builder"};
	}
}

template <class BuilderType>
static inline void AppendNumericTypedValue(
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const std::unique_ptr<ral::memory::blazing_allocation_chunk> & allocation,
	const std::size_t offset) {
	using value_type = typename BuilderType::value_type;
	value_type * data =
		reinterpret_cast<value_type *>(allocation->data + offset);
	std::size_t length = allocation->size / sizeof(value_type);

	for (std::size_t i = 0; i < length; i++) {
		// TODO: nulls
		BuilderType & builder = *static_cast<BuilderType *>(arrayBuilder.get());
		arrow::Status status = builder.Append(data[i]);
		if (!status.ok()) {
			throw std::runtime_error{"Builder appending"};
		}
	}
}

static inline void AppendValues(
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const blazingdb::transport::ColumnTransport & columnTransport,
	const std::unique_ptr<ral::memory::blazing_allocation_chunk> & allocation,
  const std::size_t offset) {
	const cudf::type_id type_id =
		static_cast<cudf::type_id>(columnTransport.metadata.dtype);
	switch (type_id) {
  case cudf::type_id::INT8:
    return AppendNumericTypedValue<arrow::Int8Builder>(
      arrayBuilder, allocation, offset);
	case cudf::type_id::INT16:
		return AppendNumericTypedValue<arrow::Int16Builder>(
			arrayBuilder, allocation, offset);
	case cudf::type_id::INT32:
		return AppendNumericTypedValue<arrow::Int32Builder>(
			arrayBuilder, allocation, offset);
  case cudf::type_id::INT64:
    return AppendNumericTypedValue<arrow::Int64Builder>(
      arrayBuilder, allocation, offset);
  case cudf::type_id::FLOAT32:
    return AppendNumericTypedValue<arrow::FloatBuilder>(
      arrayBuilder, allocation, offset);
  case cudf::type_id::FLOAT64:
    return AppendNumericTypedValue<arrow::DoubleBuilder>(
      arrayBuilder, allocation, offset);
  case cudf::type_id::UINT8:
    return AppendNumericTypedValue<arrow::UInt8Builder>(
      arrayBuilder, allocation, offset);
  case cudf::type_id::UINT16:
    return AppendNumericTypedValue<arrow::UInt16Builder>(
      arrayBuilder, allocation, offset);
  case cudf::type_id::UINT32:
    return AppendNumericTypedValue<arrow::UInt32Builder>(
      arrayBuilder, allocation, offset);
  case cudf::type_id::UINT64:
    return AppendNumericTypedValue<arrow::UInt64Builder>(
      arrayBuilder, allocation, offset);
	default:
		throw std::runtime_error{
			"Unsupported column cudf type id for append value"};
	}
}

static inline std::size_t AppendChunkToArrayBuilder(
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const blazingdb::transport::ColumnTransport & columnTransport,
	const ral::memory::blazing_chunked_column_info & chunkedColumnInfo,
	const std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> &
		allocations) {
	std::size_t position = 0;

	for (std::size_t i = 0; i < chunkedColumnInfo.chunk_index.size(); i++) {
		const std::size_t chunk_index = chunkedColumnInfo.chunk_index[i];
		const std::size_t offset = chunkedColumnInfo.offset[i];
		const std::size_t chunk_size = chunkedColumnInfo.size[i];

		const std::unique_ptr<ral::memory::blazing_allocation_chunk> &
			allocation = allocations[chunk_index];

		AppendValues(arrayBuilder, columnTransport, allocation, offset);

		position += chunk_size;
	}

	return position;
}

// END internal utils

BlazingHostTable::BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
            std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
            std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations)
        : columns_offsets{columns_offsets}, chunked_column_infos{std::move(chunked_column_infos)}, allocations{std::move(allocations)} {

    auto size = size_in_bytes();
    blazing_host_memory_resource::getInstance().allocate(size); // this only increments the memory usage counter for the host memory. This does not actually allocate

}

BlazingHostTable::~BlazingHostTable() {
    auto size = size_in_bytes();
    blazing_host_memory_resource::getInstance().deallocate(size); // this only decrements the memory usage counter for the host memory. This does not actually allocate
    for(auto i = 0; i < allocations.size(); i++){
        auto pool = allocations[i]->allocation->pool;
        pool->free_chunk(std::move(allocations[i]));
    }
}

std::vector<cudf::data_type> BlazingHostTable::column_types() const {
    std::vector<cudf::data_type> data_types(this->num_columns());
    std::transform(columns_offsets.begin(), columns_offsets.end(), data_types.begin(), [](auto &col) {
        int32_t dtype = col.metadata.dtype;
        return cudf::data_type{cudf::type_id(dtype)};
    });
    return data_types;
}

std::vector<std::string> BlazingHostTable::column_names() const {
    std::vector<std::string> col_names(this->num_columns());
    std::transform(columns_offsets.begin(), columns_offsets.end(), col_names.begin(),
                   [](auto &col) { return col.metadata.col_name; });
    return col_names;
}

void BlazingHostTable::set_column_names(std::vector<std::string> names) {
    for(size_t i = 0; i < names.size(); i++){
        strcpy(columns_offsets[i].metadata.col_name, names[i].c_str());
    }
}

cudf::size_type BlazingHostTable::num_rows() const {
  return columns_offsets.empty() ? 0 : columns_offsets.front().metadata.size;
}

cudf::size_type BlazingHostTable::num_columns() const {
    return columns_offsets.size();
}

std::size_t BlazingHostTable::size_in_bytes() const {
    std::size_t total_size = 0L;
    for (auto &col : columns_offsets) {
        total_size += col.size_in_bytes;
    }
    return total_size;
}

void BlazingHostTable::setPartitionId(const size_t &part_id) {
    this->part_id = part_id;
}

size_t BlazingHostTable::get_part_id() {
    return this->part_id;
}

const std::vector<ColumnTransport> &BlazingHostTable::get_columns_offsets() const {
    return columns_offsets;
}

std::unique_ptr<BlazingArrowTable> BlazingHostTable::get_arrow_table() const {
	int buffer_index = 0;

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.reserve(columns_offsets.size());

	std::vector<std::shared_ptr<arrow::Field>> fields;
	fields.reserve(columns_offsets.size());

	for (const ral::memory::blazing_chunked_column_info & chunked_column_info :
		chunked_column_infos) {
		std::size_t position = 0;

		const blazingdb::transport::ColumnTransport & column_offset =
			columns_offsets[buffer_index];

		std::unique_ptr<arrow::ArrayBuilder> arrayBuilder;
		std::shared_ptr<arrow::Field> field;
		std::tie(arrayBuilder, field) = MakeArrayBuilderField(column_offset);

		fields.emplace_back(field);

		position += AppendChunkToArrayBuilder(
			arrayBuilder, column_offset, chunked_column_info, allocations);

		std::shared_ptr<arrow::Array> array;
		arrow::Status status = arrayBuilder->Finish(&array);
		if (!status.ok()) {
			throw std::runtime_error{"Building array"};
		}
		arrays.push_back(array);

		buffer_index++;
	}

	std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);

	return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

std::unique_ptr<BlazingCudfTable> BlazingHostTable::get_cudf_table() const {
    std::vector<rmm::device_buffer> gpu_raw_buffers(chunked_column_infos.size());
   try{
       int buffer_index = 0;
       for(auto & chunked_column_info : chunked_column_infos){
           gpu_raw_buffers[buffer_index].resize(chunked_column_info.use_size);
           size_t position = 0;
           for(size_t i = 0; i < chunked_column_info.chunk_index.size(); i++){
               size_t chunk_index = chunked_column_info.chunk_index[i];
               size_t offset = chunked_column_info.offset[i];
               size_t chunk_size = chunked_column_info.size[i];
               cudaMemcpyAsync((void *) (gpu_raw_buffers[buffer_index].data() + position), allocations[chunk_index]->data + offset, chunk_size, cudaMemcpyHostToDevice,0);
               position += chunk_size;
           }
           buffer_index++;
       }
       cudaStreamSynchronize(0);
   }catch(std::exception & e){
       auto logger = spdlog::get("batch_logger");
       if (logger){
           logger->error("|||{info}|||||",
                   "info"_a="ERROR in BlazingHostTable::get_gpu_table(). What: {}"_format(e.what()));
       }
       throw;
   }

   return std::move(comm::deserialize_from_gpu_raw_buffers(columns_offsets, gpu_raw_buffers));
}

std::vector<ral::memory::blazing_allocation_chunk> BlazingHostTable::get_raw_buffers() const {
    std::vector<ral::memory::blazing_allocation_chunk> chunks;
    for(auto & chunk : allocations){
        ral::memory::blazing_allocation_chunk new_chunk;
        new_chunk.size = chunk->size;
        new_chunk.data = chunk->data;
        new_chunk.allocation = nullptr;
        chunks.push_back(new_chunk);
    }

    return chunks;
}

const std::vector<ral::memory::blazing_chunked_column_info> & BlazingHostTable::get_blazing_chunked_column_infos() const {
    return this->chunked_column_infos;
}

std::unique_ptr<BlazingHostTable> BlazingHostTable::clone() {
	std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos_clone = this->chunked_column_infos;
	std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> allocations_clone;

	for(auto& allocation : allocations){
		auto& pool = allocation->allocation->pool;
		std::unique_ptr<ral::memory::blazing_allocation_chunk> allocation_clone = pool->get_chunk();

		allocation_clone->size = allocation->size;
		memcpy(allocation_clone->data, allocation->data, allocation->size);
		allocation_clone->allocation = allocation->allocation;

		allocations_clone.push_back(std::move(allocation_clone));
	}

	return std::make_unique<ral::frame::BlazingHostTable>(this->columns_offsets, std::move(chunked_column_infos_clone), std::move(allocations_clone));
}

}  // namespace frame
}  // namespace ral
