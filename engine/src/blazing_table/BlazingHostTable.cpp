#include <sstream>

#include "BlazingHostTable.h"

#include "bmr/BlazingMemoryResource.h"

#ifdef CUDF_SUPPORT
#include "communication/CommunicationInterface/serializer.hpp"
#endif

#include "bmr/BufferProvider.h"
#include "parser/types_parser_utils.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/table.h>

namespace ral {
namespace frame {

// BEGIN internal utils

template <class B, class D>
static inline std::unique_ptr<B> unique_base(std::unique_ptr<D> && d) {
  // this cast from concrete arrow builder to base keeping the deleter
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

	const arrow::Type::type type_id = static_cast<arrow::Type::type>(metadata.dtype);
	const std::string columnName{metadata.col_name};

	// TODO: get column name
	switch (type_id) {
	case arrow::Type::INT8:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int8Builder>()),
			arrow::field(columnName, arrow::int8()));
	case arrow::Type::INT16:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int16Builder>()),
			arrow::field(columnName, arrow::int16()));
	case arrow::Type::INT32:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int32Builder>()),
			arrow::field(columnName, arrow::int32()));
	case arrow::Type::INT64:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::Int64Builder>()),
			arrow::field(columnName, arrow::int64()));
	case arrow::Type::FLOAT:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::FloatBuilder>()),
			arrow::field(columnName, arrow::float32()));
	case arrow::Type::DOUBLE:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::DoubleBuilder>()),
			arrow::field(columnName, arrow::float64()));
	case arrow::Type::UINT8:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt8Builder>()),
			arrow::field(columnName, arrow::uint8()));
	case arrow::Type::UINT16:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt16Builder>()),
			arrow::field(columnName, arrow::uint16()));
	case arrow::Type::UINT32:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt32Builder>()),
			arrow::field(columnName, arrow::uint32()));
	case arrow::Type::UINT64:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::UInt64Builder>()),
			arrow::field(columnName, arrow::uint64()));
	case arrow::Type::STRING:
		return std::make_tuple(unique_base<arrow::ArrayBuilder>(
								   std::make_unique<arrow::StringBuilder>()),
			arrow::field(columnName, arrow::utf8()));
	default:
		throw std::runtime_error{
			"Unsupported column cudf type id for array builder"};
	}
}

template <class BuilderType>
static inline void AppendNumericTypedValue(const std::size_t columnIndex,
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
			std::ostringstream oss;
			oss << "Error appending a numeric value to column index="
				<< columnIndex << " with data position i=" << i
				<< " for builder type " << builder.type()->name();
			throw std::runtime_error{oss.str()};
		}
	}
}

static inline void AppendValues(const std::size_t columnIndex,
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const blazingdb::transport::ColumnTransport & columnTransport,
	const std::unique_ptr<ral::memory::blazing_allocation_chunk> & allocation,
	const std::size_t offset) {
	const arrow::Type::type type_id =
		static_cast<arrow::Type::type>(columnTransport.metadata.dtype);
	switch (type_id) {
	case arrow::Type::INT8:
		return AppendNumericTypedValue<arrow::Int8Builder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::INT16:
		return AppendNumericTypedValue<arrow::Int16Builder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::INT32:
		return AppendNumericTypedValue<arrow::Int32Builder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::INT64:
		return AppendNumericTypedValue<arrow::Int64Builder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::FLOAT:
		return AppendNumericTypedValue<arrow::FloatBuilder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::DOUBLE:
		return AppendNumericTypedValue<arrow::DoubleBuilder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::UINT8:
		return AppendNumericTypedValue<arrow::UInt8Builder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::UINT16:
		return AppendNumericTypedValue<arrow::UInt16Builder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::UINT32:
		return AppendNumericTypedValue<arrow::UInt32Builder>(
			columnIndex, arrayBuilder, allocation, offset);
	case arrow::Type::UINT64:
		return AppendNumericTypedValue<arrow::UInt64Builder>(
			columnIndex, arrayBuilder, allocation, offset);
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

		AppendValues(i, arrayBuilder, columnTransport, allocation, offset);

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

std::vector<std::shared_ptr<arrow::DataType>> BlazingHostTable::column_types() const {
    std::vector<std::shared_ptr<arrow::DataType>> data_types(this->num_columns());
    std::transform(columns_offsets.begin(), columns_offsets.end(), data_types.begin(), [](auto &col) {
		return get_arrow_datatype_from_int_value(col.metadata.dtype);
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

std::size_t BlazingHostTable::num_rows() const {
  return columns_offsets.empty() ? 0 : columns_offsets.front().metadata.size;
}

std::size_t BlazingHostTable::num_columns() const {
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

		const blazingdb::transport::ColumnTransport & column_offset =
			columns_offsets[buffer_index];

		std::unique_ptr<arrow::ArrayBuilder> arrayBuilder;
		std::shared_ptr<arrow::Field> field;
		std::tie(arrayBuilder, field) = MakeArrayBuilderField(column_offset);

		fields.emplace_back(field);

		AppendChunkToArrayBuilder(
			arrayBuilder, column_offset, chunked_column_info, allocations);

		std::shared_ptr<arrow::Array> array;
		arrow::Status status = arrayBuilder->Finish(&array);
		if (!status.ok()) {
			std::ostringstream oss;
			oss << "Error building array with builder type "
				<< arrayBuilder->type()->name() << " and buffer index "
				<< buffer_index;
			throw std::runtime_error{oss.str()};
		}
		arrays.push_back(array);

		buffer_index++;
	}

	std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);

	return std::make_unique<ral::frame::BlazingArrowTable>(table);
}

#ifdef CUDF_SUPPORT
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
          // TODO percy arrow
          //logger->error("|||{info}|||||", "info"_a="ERROR in BlazingHostTable::get_gpu_table(). What: {}"_format(e.what()));
       }
       throw;
   }

   return std::move(comm::deserialize_from_gpu_raw_buffers(columns_offsets, gpu_raw_buffers));
}
#endif

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
