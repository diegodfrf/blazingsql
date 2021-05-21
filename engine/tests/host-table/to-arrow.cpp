#include <numeric>

#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include "blazing_table/BlazingHostTable.h"
#include "utilities/DebuggingUtils.h"

template <cudf::type_id>
class Traits {};

#define FACT_TRAIT(dtype, ptype)                                               \
	template <>                                                                \
	class Traits<cudf::type_id::dtype> {                                       \
	public:                                                                    \
		using value_type = ptype;                                              \
		static constexpr std::size_t size = sizeof(value_type);                \
	}

// FACT_TRAIT(INT8, std::int8_t);
FACT_TRAIT(INT16, std::int16_t);
FACT_TRAIT(INT32, std::int32_t);
// FACT_TRAIT(INT64, std::int64_t);
// FACT_TRAIT(FLOAT32, float);
// FACT_TRAIT(FLOAT64, double);
// FACT_TRAIT(UINT8, std::uint8_t);
// FACT_TRAIT(UINT16, std::uint16_t);
// FACT_TRAIT(UINT32, std::uint32_t);
// FACT_TRAIT(UINT64, std::uint64_t);

template <cudf::type_id type_id>
static inline void AddColumn(
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
		{0}, {0}, {columnSize}, columnSize});

	// allocation
	std::unique_ptr<value_type[]> payload =
		std::make_unique<value_type[]>(columnLength);

	//std::iota(payload.get(), payload.get() + columnLength, 1);
  for (std::size_t i = 0; i < columnLength; i++) {
    payload[i] = i + 1;
  }
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

	std::cout << "\033[33m>>> " << columnLength << "\033[0m" << std::endl;
	std::cout << "\033[33m>>> " << columnSize << "\033[0m" << std::endl;

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

	AddColumn<cudf::type_id::INT32>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"int32-col",
		columnLength);

	AddColumn<cudf::type_id::INT16>(columnTransports,
		chunkedColumnInfos,
		allocationChunks,
		"int16-col",
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
