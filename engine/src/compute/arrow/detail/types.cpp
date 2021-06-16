#include "compute/arrow/detail/types.h"
#include <arrow/type.h>
#include <arrow/builder.h>
#include <assert.h>

std::shared_ptr<arrow::Schema> build_arrow_schema(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>> &columns,
    const std::vector<std::string> &column_names,
    std::shared_ptr<const arrow::KeyValueMetadata> metadata)
{
  assert(columns.size() == column_names.size());
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.resize(columns.size());
  for (int i = 0; i < columns.size(); ++i) {
    fields[i] = arrow::field(
                  column_names[i],
                  columns[i]->type());
  }
  return arrow::schema(fields, metadata);
}

template <class B, class D>
static inline std::unique_ptr<B> unique_base(std::unique_ptr<D> && d) {
  // this cast from concrete arrow builder to base keeping the deleter
	if (B * b = static_cast<B *>(d.get())) {
		d.release();
		return std::unique_ptr<B>(b, std::move(d.get_deleter()));
	}
	throw std::bad_alloc{};
}

std::unique_ptr<arrow::ArrayBuilder>
MakeArrayBuilder(std::shared_ptr<arrow::DataType> dtype) {
	switch (dtype->id()) {
	case arrow::Type::INT8:
		return unique_base<arrow::ArrayBuilder>(
                  std::make_unique<arrow::Int8Builder>());
	case arrow::Type::INT16:
		return unique_base<arrow::ArrayBuilder>(
			            std::make_unique<arrow::Int16Builder>());
	case arrow::Type::INT32:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::Int32Builder>());
	case arrow::Type::INT64:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::Int64Builder>());
	case arrow::Type::FLOAT:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::FloatBuilder>());
	case arrow::Type::DOUBLE:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::DoubleBuilder>());
	case arrow::Type::UINT8:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::UInt8Builder>());
	case arrow::Type::UINT16:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::UInt16Builder>());
	case arrow::Type::UINT32:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::UInt32Builder>());
	case arrow::Type::UINT64:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::UInt64Builder>());
	case arrow::Type::STRING:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::StringBuilder>());
	case arrow::Type::TIMESTAMP:
		return unique_base<arrow::ArrayBuilder>(
						std::make_unique<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::type::MILLI),
							arrow::default_memory_pool()));
	default:
		throw std::runtime_error{
			"Unsupported column arrow type for array builder"};
	}
}
