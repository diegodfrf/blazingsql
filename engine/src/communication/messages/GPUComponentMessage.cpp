#include "GPUComponentMessage.h"
#include "transport/ColumnTransport.h"
#include "bmr/BufferProvider.h"

#include <arrow/array.h>
#include <arrow/array/concatenate.h>

using namespace fmt::literals;

namespace ral {
namespace communication {
namespace messages {

gpu_raw_buffer_container serialize_gpu_message_to_gpu_containers(std::shared_ptr<ral::frame::BlazingCudfTableView> table_view){
	std::vector<std::size_t> buffer_sizes;
	std::vector<const char *> raw_buffers;
	std::vector<ColumnTransport> column_offset;
	std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;
	for(int i = 0; i < table_view->num_columns(); ++i) {
		const cudf::column_view&column = table_view->column(i);
		ColumnTransport col_transport = ColumnTransport{ColumnTransport::MetaData{
															.dtype = (int32_t)column.type().id(),
															.size = column.size(),
															.null_count = column.null_count(),
															.col_name = {},
														},
			.data = -1,
			.valid = -1,
			.strings_data = -1,
			.strings_offsets = -1,
			.strings_nullmask = -1,
			.strings_data_size = 0,
			.strings_offsets_size = 0,
			.size_in_bytes = 0};
		strcpy(col_transport.metadata.col_name, table_view->column_names().at(i).c_str());

		if (column.size() == 0) {
			// do nothing
		} else if(column.type().id() == cudf::type_id::STRING) {
				cudf::strings_column_view str_col_view{column};

				auto offsets_column = str_col_view.offsets();
				auto chars_column = str_col_view.chars();

				if (str_col_view.size() + 1 == offsets_column.size()){
					// this column does not come from a buffer than had been zero-copy partitioned

					col_transport.strings_data = raw_buffers.size();
					buffer_sizes.push_back(chars_column.size());
					col_transport.size_in_bytes += chars_column.size();
					raw_buffers.push_back(chars_column.head<char>());
					col_transport.strings_data_size = chars_column.size();

					col_transport.strings_offsets = raw_buffers.size();
					col_transport.strings_offsets_size = offsets_column.size() * sizeof(int32_t);
					buffer_sizes.push_back(col_transport.strings_offsets_size);
					col_transport.size_in_bytes += col_transport.strings_offsets_size;
					raw_buffers.push_back(offsets_column.head<char>());

					if(str_col_view.has_nulls()) {
						col_transport.strings_nullmask = raw_buffers.size();
						buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(str_col_view.size()));
						col_transport.size_in_bytes += cudf::bitmask_allocation_size_bytes(str_col_view.size());
						raw_buffers.push_back((const char *)str_col_view.null_mask());
					}
				} else {
					// this column comes from a column that was zero-copy partitioned

					std::pair<int32_t, int32_t> char_col_start_end = getCharsColumnStartAndEnd(str_col_view);

					std::unique_ptr<cudf::column> new_offsets = getRebasedStringOffsets(str_col_view, char_col_start_end.first);

					col_transport.strings_data = raw_buffers.size();
					col_transport.strings_data_size = char_col_start_end.second - char_col_start_end.first;
					buffer_sizes.push_back(col_transport.strings_data_size);
					col_transport.size_in_bytes += col_transport.strings_data_size;

					raw_buffers.push_back(chars_column.head<char>() + char_col_start_end.first);

					col_transport.strings_offsets = raw_buffers.size();
					col_transport.strings_offsets_size = new_offsets->size() * sizeof(int32_t);
					buffer_sizes.push_back(col_transport.strings_offsets_size);
					col_transport.size_in_bytes += col_transport.strings_offsets_size;

					raw_buffers.push_back(new_offsets->view().head<char>());

					cudf::column::contents new_offsets_contents = new_offsets->release();
					temp_scope_holder.emplace_back(std::move(new_offsets_contents.data));

					if(str_col_view.has_nulls()) {
						col_transport.strings_nullmask = raw_buffers.size();
						buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(str_col_view.size()));
						col_transport.size_in_bytes += cudf::bitmask_allocation_size_bytes(str_col_view.size());
						temp_scope_holder.emplace_back(std::make_unique<rmm::device_buffer>(
							cudf::copy_bitmask(str_col_view.null_mask(), str_col_view.offset(), str_col_view.offset() + str_col_view.size())));
						raw_buffers.push_back((const char *)temp_scope_holder.back()->data());
					}
				}
		} else {
			col_transport.data = raw_buffers.size();
			buffer_sizes.push_back((std::size_t) column.size() * cudf::size_of(column.type()));
			col_transport.size_in_bytes += (std::size_t) column.size() * cudf::size_of(column.type());

			raw_buffers.push_back(column.head<char>() + column.offset() * cudf::size_of(column.type())); // here we are getting the beginning of the buffer and manually calculating the offset.
			if(column.has_nulls()) {
				col_transport.valid = raw_buffers.size();
				buffer_sizes.push_back(cudf::bitmask_allocation_size_bytes(column.size()));
				col_transport.size_in_bytes += cudf::bitmask_allocation_size_bytes(column.size());
				if (column.offset() == 0){
					raw_buffers.push_back((const char *)column.null_mask());
				} else {
					temp_scope_holder.emplace_back(std::make_unique<rmm::device_buffer>(
						cudf::copy_bitmask(column)));
					raw_buffers.push_back((const char *)temp_scope_holder.back()->data());
				}
			}
		}
		column_offset.push_back(col_transport);
	}
	return std::make_tuple(buffer_sizes, raw_buffers, column_offset, std::move(temp_scope_holder));
}

std::unique_ptr<ral::frame::BlazingHostTable> serialize_gpu_message_to_host_table(std::shared_ptr<ral::frame::BlazingCudfTableView> table_view, bool use_pinned) {
	std::vector<std::size_t> buffer_sizes;
	std::vector<const char *> raw_buffers;
	std::vector<ColumnTransport> column_offset;
	std::vector<std::unique_ptr<rmm::device_buffer>> temp_scope_holder;


	std::tie(buffer_sizes, raw_buffers, column_offset, temp_scope_holder) = serialize_gpu_message_to_gpu_containers(table_view);


	typedef std::pair< std::vector<ral::memory::blazing_chunked_column_info>, std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk> >> buffer_alloc_type;
	buffer_alloc_type buffers_and_allocations = ral::memory::convert_gpu_buffers_to_chunks(buffer_sizes,use_pinned);


	auto & allocations = buffers_and_allocations.second;
	size_t buffer_index = 0;
	for(auto & chunked_column_info : buffers_and_allocations.first){
		size_t position = 0;
		for(size_t i = 0; i < chunked_column_info.chunk_index.size(); i++){
			size_t chunk_index = chunked_column_info.chunk_index[i];
			size_t offset = chunked_column_info.offset[i];
			size_t chunk_size = chunked_column_info.size[i];
			cudaMemcpyAsync((void *) (allocations[chunk_index]->data + offset), raw_buffers[buffer_index] + position, chunk_size, cudaMemcpyDeviceToHost,0);
			position += chunk_size;
		}
		buffer_index++;
	}
	cudaStreamSynchronize(0);


	auto table = std::make_unique<ral::frame::BlazingHostTable>(column_offset, std::move(buffers_and_allocations.first),std::move(buffers_and_allocations.second));
	return table;
}

template <class DataType>
void AllocateDataSize(const std::shared_ptr<arrow::Array> & arrayColumn,
	std::unique_ptr<char[]> & data_,
	std::size_t * const columnSize_) {
	static_assert(std::is_base_of_v<arrow::DataType, DataType>);
	using ArrayType = arrow::NumericArray<DataType>;
	using value_type = typename ArrayType::value_type;

	const std::shared_ptr<ArrayType> numericArray =
		std::dynamic_pointer_cast<ArrayType>(arrayColumn);

	const value_type * raw_values = numericArray->raw_values();
	const std::size_t length = numericArray->length();

	std::unique_ptr<value_type[]> data = std::make_unique<value_type[]>(length);
	std::copy_n(raw_values, length, data.get());

	data_.reset(reinterpret_cast<char *>(data.release()));
	*columnSize_ = length * sizeof(value_type);
}

std::unique_ptr<ral::frame::BlazingHostTable>
serialize_arrow_message_to_host_table(
	std::shared_ptr<ral::frame::BlazingArrowTableView> table_view,
	bool use_pinned) {
	std::shared_ptr<arrow::Table> table = table_view->view();
	std::shared_ptr<arrow::Schema> schema = table->schema();

	// create column transports from schema
	const std::size_t columnLength = schema->num_fields();

	// arguments for blazing host table creation
	std::vector<blazingdb::transport::ColumnTransport> columnTransports;
	std::vector<ral::memory::blazing_chunked_column_info> chunkedColumnInfos;
	std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>>
		allocationChunks;

	columnTransports.reserve(columnLength);
	chunkedColumnInfos.reserve(columnLength);
	allocationChunks.reserve(columnLength);

	// get info from arrow columns to host table
	static std::unordered_map<arrow::Type::type, cudf::type_id> typeMap{
		{arrow::Type::type::INT8, cudf::type_id::INT8},
		{arrow::Type::type::INT16, cudf::type_id::INT16},
		{arrow::Type::type::INT32, cudf::type_id::INT32},
		{arrow::Type::type::INT64, cudf::type_id::INT64},
		{arrow::Type::type::FLOAT, cudf::type_id::FLOAT32},
		{arrow::Type::type::DOUBLE, cudf::type_id::FLOAT64},
		{arrow::Type::type::UINT8, cudf::type_id::UINT8},
		{arrow::Type::type::UINT16, cudf::type_id::UINT16},
		{arrow::Type::type::UINT32, cudf::type_id::UINT32},
		{arrow::Type::type::UINT64, cudf::type_id::UINT64}};

	std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
	std::vector<std::shared_ptr<arrow::ChunkedArray>> columns =
		table->columns();

	std::vector<std::pair<std::shared_ptr<arrow::Field>,
		std::shared_ptr<arrow::ChunkedArray>>>
		arrowColumnInfos;
	arrowColumnInfos.reserve(columnLength);
	std::transform(fields.cbegin(),
		fields.cend(),
		columns.cbegin(),
		std::back_inserter(arrowColumnInfos),
		std::make_pair<const std::shared_ptr<arrow::Field> &,
			const std::shared_ptr<arrow::ChunkedArray> &>);

	std::shared_ptr<ral::memory::allocation_pool> pool =
		use_pinned ? ral::memory::buffer_providers::get_pinned_buffer_provider()
				   : ral::memory::buffer_providers::get_host_buffer_provider();

	std::size_t chunkIndex = 0;

	std::for_each(arrowColumnInfos.cbegin(),
		arrowColumnInfos.cend(),
		[&columnTransports,
			&chunkedColumnInfos,
			&allocationChunks,
			&pool,
			&chunkIndex](const std::pair<std::shared_ptr<arrow::Field>,
			std::shared_ptr<arrow::ChunkedArray>> &
				arrowColumnInfoPair) mutable {
			std::shared_ptr<arrow::Field> field;
			std::shared_ptr<arrow::ChunkedArray> chunkedArray;
			std::tie(field, chunkedArray) = arrowColumnInfoPair;

			arrow::Type::type arrowTypeId = field->type()->id();
			cudf::type_id type_id = typeMap.at(arrowTypeId);

			std::shared_ptr<arrow::Array> arrayColumn = *arrow::Concatenate(
				chunkedArray->chunks(), arrow::default_memory_pool());

			// column transport construction
			blazingdb::transport::ColumnTransport::MetaData metadata;
			metadata.size = arrayColumn->length();
			metadata.null_count = arrayColumn->null_count();
			metadata.dtype =
				static_cast<std::underlying_type_t<cudf::type_id>>(type_id);

			const std::string & fieldName = field->name();
			bzero(metadata.col_name,
				sizeof(
					blazingdb::transport::ColumnTransport::MetaData::col_name));
			std::strcpy(metadata.col_name, fieldName.c_str());

			blazingdb::transport::ColumnTransport columnTransport;
			columnTransport.metadata = metadata;
			columnTransport.strings_data = -1;
			columnTransport.valid = -1;
			columnTransport.data = chunkIndex;
			columnTransports.push_back(columnTransport);

			// chunked columns construction
			std::size_t columnSize;
			std::unique_ptr<char[]> data;

			switch (arrowTypeId) {
			case arrow::Type::type::INT8:
				AllocateDataSize<arrow::Int8Type>(
					arrayColumn, data, &columnSize);
				break;
			case arrow::Type::type::INT16:
				AllocateDataSize<arrow::Int16Type>(
					arrayColumn, data, &columnSize);
				break;
			case arrow::Type::type::INT32:
				AllocateDataSize<arrow::Int32Type>(
					arrayColumn, data, &columnSize);
				break;
			case arrow::Type::type::INT64:
				AllocateDataSize<arrow::Int64Type>(
					arrayColumn, data, &columnSize);
				break;
			default: throw std::runtime_error{"unsupported arrow type "};
			}

			std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>>
				allocationChunks;
			ral::memory::blazing_chunked_column_info chunkedColumnInfo;
			std::size_t allocationSize = 0;
			std::size_t chunkOffset = 0;

			chunkedColumnInfo.use_size = columnSize;

			while (allocationSize < columnSize) {
				std::unique_ptr<ral::memory::blazing_allocation_chunk>
					allocationChunk = pool->get_chunk();

				char * allocationChunkData = allocationChunk->data;
				const std::size_t allocationChunkSize = allocationChunk->size;
				allocationSize += allocationChunkSize;

				const std::size_t dataChunkSize =
					std::min(columnSize, allocationChunkSize);

				char * allocationChunkTail =
					std::copy_n(data.get() + chunkOffset,
						dataChunkSize,
						allocationChunkData);
				const std::size_t chunkSize =
					std::distance(allocationChunkData, allocationChunkTail);

				allocationChunks.emplace_back(std::move(allocationChunk));

				chunkedColumnInfo.chunk_index.emplace_back(chunkIndex++);
				chunkedColumnInfo.offset.emplace_back(chunkOffset);
				chunkedColumnInfo.size.emplace_back(chunkSize);

				chunkOffset += chunkSize;
			}
			chunkedColumnInfos.push_back(chunkedColumnInfo);
		});

	std::unique_ptr<ral::frame::BlazingHostTable> blazingHostTable =
		std::make_unique<ral::frame::BlazingHostTable>(columnTransports,
			std::move(chunkedColumnInfos),
			std::move(allocationChunks));

	return blazingHostTable;
}

std::unique_ptr<ral::frame::BlazingCudfTable> deserialize_from_gpu_raw_buffers(const std::vector<ColumnTransport> & columns_offsets,
									  const std::vector<rmm::device_buffer> & raw_buffers) {
	auto num_columns = columns_offsets.size();
	std::vector<std::unique_ptr<cudf::column>> received_samples(num_columns);
	std::vector<std::string> column_names(num_columns);

	assert(raw_buffers.size() >= 0);
	for(size_t i = 0; i < num_columns; ++i) {
		auto data_offset = columns_offsets[i].data;
		auto string_offset = columns_offsets[i].strings_data;
		if(string_offset != -1) {
			cudf::size_type num_strings = columns_offsets[i].metadata.size;
			std::unique_ptr<cudf::column> offsets_column
				= std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::INT32}, num_strings + 1, std::move(raw_buffers[columns_offsets[i].strings_offsets]));

			cudf::size_type total_bytes = columns_offsets[i].strings_data_size;
			std::unique_ptr<cudf::column> chars_column	= std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::INT8}, total_bytes, std::move(raw_buffers[columns_offsets[i].strings_data]));
			rmm::device_buffer null_mask;
			if (columns_offsets[i].strings_nullmask != -1)
				null_mask = rmm::device_buffer(std::move(raw_buffers[columns_offsets[i].strings_nullmask]));

			cudf::size_type null_count = columns_offsets[i].metadata.null_count;
			auto unique_column = cudf::make_strings_column(num_strings, std::move(offsets_column), std::move(chars_column), null_count, std::move(null_mask));
			received_samples[i] = std::move(unique_column);

		} else {
			cudf::data_type dtype = cudf::data_type{cudf::type_id(columns_offsets[i].metadata.dtype)};
			cudf::size_type column_size  =  (cudf::size_type)columns_offsets[i].metadata.size;

			if(columns_offsets[i].valid != -1) {
				// this is a valid
				auto valid_offset = columns_offsets[i].valid;
				auto unique_column = std::make_unique<cudf::column>(dtype, column_size, std::move(raw_buffers[data_offset]), std::move(raw_buffers[valid_offset]));
				received_samples[i] = std::move(unique_column);
			} else if (data_offset != -1){
				auto unique_column = std::make_unique<cudf::column>(dtype, column_size, std::move(raw_buffers[data_offset]));
				received_samples[i] = std::move(unique_column);
			} else {
				auto unique_column = cudf::make_empty_column(dtype);
				received_samples[i] = std::move(unique_column);
			}
		}
		column_names[i] = std::string{columns_offsets[i].metadata.col_name};
	}
	auto unique_table = std::make_unique<cudf::table>(std::move(received_samples));
	return std::make_unique<ral::frame::BlazingCudfTable>(std::move(unique_table), column_names);
}


}  // namespace messages
}  // namespace communication
}  // namespace ral
