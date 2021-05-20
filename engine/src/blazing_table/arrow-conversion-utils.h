#include <arrow/array/builder_primitive.h>
#include <arrow/table.h>

#include "bmr/BufferProvider.h"
#include "cudf/types.hpp"
#include "transport/ColumnTransport.h"

static inline std::tuple<std::unique_ptr<arrow::ArrayBuilder>,
	std::shared_ptr<arrow::Field>>
MakeArrayBuilderField(
	const blazingdb::transport::ColumnTransport & columnTransport) {
	const cudf::type_id type_id =
		static_cast<cudf::type_id>(columnTransport.metadata.size);

	// TODO: get column name
	switch (type_id) {
	case cudf::type_id::INT8:
		return std::make_tuple(std::make_unique<arrow::Int32Builder>(),
			arrow::field("int8", arrow::int8()));
	case cudf::type_id::INT16:
		return std::make_tuple(std::make_unique<arrow::Int32Builder>(),
			arrow::field("int16", arrow::int16()));
	case cudf::type_id::INT32:
		return std::make_tuple(std::make_unique<arrow::Int32Builder>(),
			arrow::field("int32", arrow::int32()));
	case cudf::type_id::INT64:
		return std::make_tuple(std::make_unique<arrow::Int32Builder>(),
			arrow::field("int64", arrow::int64()));
	default:
		throw std::runtime_error{"Unsupported column cudf type id to arrow"};
	}
}

static inline void AppendTypedValue(
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const std::unique_ptr<ral::memory::blazing_allocation_chunk> & allocation) {
	std::int32_t * data = reinterpret_cast<std::int32_t *>(allocation->data);
	std::size_t length = allocation->size / sizeof(std::int32_t);

	for (std::size_t i = 0; i < length; i++) {
		// TODO: nulls
		arrow::Int32Builder & int32Builder =
			*static_cast<arrow::Int32Builder *>(arrayBuilder.get());
		arrow::Status status = int32Builder.Append(data[i]);
		if (!status.ok()) {
			throw std::runtime_error{"Builder appending"};
		}
	}
}

static inline void AppendValues(
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const std::unique_ptr<ral::memory::blazing_allocation_chunk> & allocation) {
	const cudf::type_id type_id =
		static_cast<cudf::type_id>(columnTransport.metadata.size);
}

static inline std::size_t AppendChunkToArrayBiulder(
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const ral::memory::blazing_chunked_column_info & chunkedColumnInfo,
	const std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> &
		allocations) {
	std::size_t position = 0;

	for (std::size_t i = 0; i < chunkedColumnInfo.chunk_index.size(); i++) {
		std::size_t chunk_index = chunkedColumnInfo.chunk_index[i];
		// std::size_t offset = chunkedColumnInfo.offset[i];
		std::size_t chunk_size = chunkedColumnInfo.size[i];

		const std::unique_ptr<ral::memory::blazing_allocation_chunk> &
			allocation = allocations[chunk_index];

		AppendValues(arrayBuilder, allocation);

		position += chunk_size;
	}

	return position;
}
