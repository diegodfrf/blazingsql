#include <numeric>

#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include "blazing_table/BlazingHostTable.h"
#include "utilities/DebuggingUtils.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

TEST(BlazingHostTable, arrow_slice) {
  /*
  * input:   {10, 12, 14, 16, 18, 20, 22, 24, 26, 28}
  * indices: {1, 3, 5, 9, 2, 4, 8, 8}
  * output:  {{12, 14}, {20, 22, 24, 26}, {14, 16}, {}}
  */
  //std::vector<column_view> slice(column_view const& input, std::vector<size_type> const& indices);

  //asdad  
  
}

static inline void AddColumnTransport(
	std::vector<blazingdb::transport::ColumnTransport> & columnTransports,
	std::vector<ral::memory::blazing_chunked_column_info> & chunkedColumnInfos,
	std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> &
		allocationChunks,
	const std::string & columnName,
	const std::size_t columnLength) {
	using typeid_traits = Traits<type_id>;
	using value_type = typename typeid_traits::value_type;
	const std::size_t columnSize = columnLength * typeid_traits::size;

	// column transport
	const std::int32_t dtype = static_cast<std::int32_t>(
		static_cast<std::underlying_type_t<cudf::type_id>>(type_id));
	blazingdb::transport::ColumnTransport::MetaData metadata{dtype, 0, 0, {0}};

	std::strncpy(metadata.col_name,
		columnName.c_str(),
		sizeof(blazingdb::transport::ColumnTransport::MetaData::col_name));

	columnTransports.emplace_back(blazingdb::transport::ColumnTransport{
		metadata, -1, -1, -1, -1, -1, 0, 0, columnSize});

	// chunked column info
	chunkedColumnInfos.emplace_back(ral::memory::blazing_chunked_column_info{
		{columnCounter++}, {0}, {columnSize}, columnSize});

	// allocation
	std::unique_ptr<value_type[]> payload =
		std::make_unique<value_type[]>(columnLength);

	std::generate(payload.get(), payload.get() + columnLength, []() {
		return typeid_traits::generate(generationCounter++);
	});
	char * data = reinterpret_cast<char *>(payload.get());

	static std::vector<std::unique_ptr<value_type[]>> payloads;
	payloads.emplace_back(std::move(payload));

	std::unique_ptr<ral::memory::base_allocator> allocator =
		std::make_unique<ral::memory::host_allocator>(false);

	std::unique_ptr<ral::memory::allocation_pool> pool =
		std::make_unique<ral::memory::allocation_pool>(
			std::move(allocator), 0, 0);

	std::unique_ptr<ral::memory::blazing_allocation> allocation =
		std::make_unique<ral::memory::blazing_allocation>();
	allocation->index = 0;
	allocation->pool = pool.get();

	static std::vector<std::unique_ptr<ral::memory::allocation_pool>> pools;
	pools.emplace_back(std::move(pool));

	allocationChunks.emplace_back(
		std::make_unique<ral::memory::blazing_allocation_chunk>(
			ral::memory::blazing_allocation_chunk{
				columnSize, data, allocation.get()}));

	static std::vector<std::unique_ptr<ral::memory::blazing_allocation>>
		allocations;
	allocations.emplace_back(std::move(allocation));
}

TEST(BlazingHostTable, ToArrowTable) {
	const std::size_t columnLength = 10;

	std::vector<blazingdb::transport::ColumnTransport> columnTransports;
	std::vector<ral::memory::blazing_chunked_column_info> chunkedColumnInfos;
	std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>>
		allocationChunks;

	AddColumn<cudf::type_id::INT8>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"int8-col",
		columnLength);

	AddColumn<cudf::type_id::INT16>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"int16-col",
		columnLength);

	AddColumn<cudf::type_id::INT32>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"int32-col",
		columnLength);

	AddColumn<cudf::type_id::INT64>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"int64-col",
		columnLength);

	AddColumn<cudf::type_id::FLOAT32>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"float32-col",
		columnLength);

	AddColumn<cudf::type_id::FLOAT64>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"float64-col",
		columnLength);

	std::unique_ptr<ral::frame::BlazingHostTable> blazingHostTable =
		std::make_unique<ral::frame::BlazingHostTable>(columnTransports,
			std::move(chunkedColumnInfos),
			std::move(allocationChunks));

	std::unique_ptr<ral::frame::BlazingArrowTable> blazingArrowTable =
		blazingHostTable->get_arrow_table();
	std::shared_ptr<arrow::Table> table =
		blazingArrowTable->to_table_view()->view();

	std::ostringstream sink;
	arrow::Status status = arrow::PrettyPrint(*table, {0}, &sink);
	ASSERT_EQ(status.ok(), true);
	status = arrow::PrettyPrint(*table, {0}, &std::cout);
	ASSERT_EQ(status.ok(), true);
	std::string result = sink.str();
	// static const char * expected = R"del(int32-col: int32
	//----
	// int32-col:
	//[
	//[
	// 1,
	// 2,
	// 3,
	// 4,
	// 5,
	// 6,
	// 7,
	// 8,
	// 9,
	// 10
	//]
	//]
	//)del";
	// ASSERT_EQ(std::string{expected}, result);
}
