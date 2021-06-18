#pragma once

#include <sstream>
#include <arrow/chunked_array.h>

std::shared_ptr<arrow::Schema> build_arrow_schema(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>> &columns,
    const std::vector<std::string> &column_names,
    std::shared_ptr<const arrow::KeyValueMetadata> metadata = NULLPTR);

std::unique_ptr<arrow::ArrayBuilder>
    MakeArrayBuilder(std::shared_ptr<arrow::DataType> dtype);

template <class BuilderType, class T>
static inline void AppendNumericTypedValue(const std::vector<T>& data,
        std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder) {
    using value_type = typename BuilderType::value_type;

    for (std::size_t i = 0; i < data.size(); i++) {
        BuilderType & builder = *static_cast<BuilderType *>(arrayBuilder.get());
        arrow::Status status = builder.Append(data[i]);
        if (!status.ok()) {
            std::ostringstream oss;
            oss << "Error appending a numeric value "
                << " for builder type " << builder.type()->name();
            throw std::runtime_error{oss.str()};
        }
    }
}

template <class T>
static inline void AppendValues(const std::vector<T>& data,
        std::unique_ptr<arrow::ArrayBuilder> & arrayBuilder,
        std::shared_ptr<arrow::DataType> dtype) {
    switch (dtype->id()) {
    case arrow::Type::INT8:
        return AppendNumericTypedValue<arrow::Int8Builder, T>(
            data, arrayBuilder);
    case arrow::Type::INT16:
        return AppendNumericTypedValue<arrow::Int16Builder, T>(
            data, arrayBuilder);
    case arrow::Type::INT32:
        return AppendNumericTypedValue<arrow::Int32Builder, T>(
            data, arrayBuilder);
    case arrow::Type::INT64:
        return AppendNumericTypedValue<arrow::Int64Builder, T>(
            data, arrayBuilder);
    case arrow::Type::FLOAT:
        return AppendNumericTypedValue<arrow::FloatBuilder, T>(
            data, arrayBuilder);
    case arrow::Type::DOUBLE:
        return AppendNumericTypedValue<arrow::DoubleBuilder, T>(
            data, arrayBuilder);
    case arrow::Type::UINT8:
        return AppendNumericTypedValue<arrow::UInt8Builder, T>(
            data, arrayBuilder);
    case arrow::Type::UINT16:
        return AppendNumericTypedValue<arrow::UInt16Builder, T>(
            data, arrayBuilder);
    case arrow::Type::UINT32:
        return AppendNumericTypedValue<arrow::UInt32Builder, T>(
            data, arrayBuilder);
    case arrow::Type::UINT64:
        return AppendNumericTypedValue<arrow::UInt64Builder, T>(
            data, arrayBuilder);
    case arrow::Type::TIMESTAMP:
        return AppendNumericTypedValue<arrow::TimestampBuilder, T>(
            data, arrayBuilder);
    default:
        throw std::runtime_error{
            "Unsupported column arrow type for append value"};
    }
}
