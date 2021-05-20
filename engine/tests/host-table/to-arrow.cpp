#include <numeric>

#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include "blazing_table/BlazingHostTable.h"
#include "utilities/DebuggingUtils.h"

TEST(BlazingHostTable, ToArrowTable) {
	std::size_t columnLength = 10;
	std::size_t columnSize = columnLength * sizeof(std::int32_t);

	std::vector<blazingdb::transport::ColumnTransport> column_transports;
	column_transports.emplace_back(blazingdb::transport::ColumnTransport{
		{
			(std::int32_t) cudf::type_id::INT32,
			0,
			0,
			"test-column",
		},   // metadata
		-1,  // data
		-1,  // size
		-1,
		-1,
		-1,
		0,
		0,
		columnSize});

	std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos;
	chunked_column_infos.emplace_back(ral::memory::blazing_chunked_column_info{
		{0}, {0}, {columnSize}, columnSize});

	std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>>
		allocations;

	std::unique_ptr<std::int32_t[]> payload =
		std::make_unique<std::int32_t[]>(columnLength);

	std::iota(payload.get(), payload.get() + columnLength, 1);
	char * data = reinterpret_cast<char *>(payload.get());

	std::unique_ptr<ral::memory::base_allocator> allocator =
		std::make_unique<ral::memory::host_allocator>(false);

	std::unique_ptr<ral::memory::allocation_pool> pool =
		std::make_unique<ral::memory::allocation_pool>(
			std::move(allocator), 0, 0);

	ral::memory::blazing_allocation allocation;
	allocation.index = 0;
	allocation.pool = pool.get();

	allocations.emplace_back(
		std::make_unique<ral::memory::blazing_allocation_chunk>(
			ral::memory::blazing_allocation_chunk{
				columnSize, data, &allocation}));

	std::unique_ptr<ral::frame::BlazingHostTable> blazingHostTable =
		std::make_unique<ral::frame::BlazingHostTable>(column_transports,
			std::move(chunked_column_infos),
			std::move(allocations));

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
	static const char * expected = R"del(f0: int32
----
f0:
  [
    [
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10
    ]
  ]
)del";
	ASSERT_EQ(std::string{expected}, result);
}
