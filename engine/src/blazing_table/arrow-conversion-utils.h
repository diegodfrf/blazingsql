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
		return std::make_tuple(std::make_unique<arrow::Int8Builder>(),
			arrow::field("int8", arrow::int8()));
	case cudf::type_id::INT16:
		return std::make_tuple(std::make_unique<arrow::Int16Builder>(),
			arrow::field("int16", arrow::int16()));
	case cudf::type_id::INT32:
		return std::make_tuple(std::make_unique<arrow::Int32Builder>(),
			arrow::field("int32", arrow::int32()));
	case cudf::type_id::INT64:
		return std::make_tuple(std::make_unique<arrow::Int64Builder>(),
			arrow::field("int64", arrow::int64()));
	default:
		throw std::runtime_error{
			"Unsupported column cudf type id for array builder"};
	}
}

template <class BuilderType>
static inline void AppendNumericTypedValue(
	std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
	const std::unique_ptr<ral::memory::blazing_allocation_chunk> & allocation) {
	using value_type = typename BuilderType::value_type;
	value_type * data = reinterpret_cast<value_type>(allocation->data);
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
	const std::unique_ptr<ral::memory::blazing_allocation_chunk> & allocation) {
	const cudf::type_id type_id =
		static_cast<cudf::type_id>(columnTransport.metadata.size);
	switch (type_id) {
	case cudf::type_id::INT8:
		AppendNumericTypedValue<arrow::Int8Builder>(arrayBuilder, allocation);
	case cudf::type_id::INT16:
		AppendNumericTypedValue<arrow::Int16Builder>(arrayBuilder, allocation);
	case cudf::type_id::INT32:
		AppendNumericTypedValue<arrow::Int32Builder>(arrayBuilder, allocation);
		return;
	case cudf::type_id::INT64:
		AppendNumericTypedValue<arrow::Int64Builder>(arrayBuilder, allocation);
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
		std::size_t chunk_index = chunkedColumnInfo.chunk_index[i];
		// std::size_t offset = chunkedColumnInfo.offset[i];
		std::size_t chunk_size = chunkedColumnInfo.size[i];

		const std::unique_ptr<ral::memory::blazing_allocation_chunk> &
			allocation = allocations[chunk_index];

		AppendValues(arrayBuilder, columnTransport, allocation);

		position += chunk_size;
	}

	return position;
}
