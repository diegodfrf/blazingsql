#include <numeric>

#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include "blazing_table/BlazingHostTable.h"
#include "utilities/DebuggingUtils.h"

#include "utilities/CommonOperations.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>


void slice() {
  /*
  * input:   {10, 12, 14, 16, 18, 20, 22, 24, 26, 28}
  * indices: {1, 3, 5, 9, 2, 4, 8, 8}
  * output:  {{12, 14}, {20, 22, 24, 26}, {14, 16}, {}}
  */
  //std::vector<column_view> slice(column_view const& input, std::vector<size_type> const& indices);

  //asdad  

}

/**
 * @brief Computes the row indices that would produce `input` in a lexicographical sorted order.
 *
 * @param input The table to sort
 * @param column_order The desired sort order for each column. Size must be
 * equal to `input.num_columns()` or empty. If empty, all columns will be sorted
 * in ascending order.
 * @param null_precedence The desired order of null compared to other elements
 * for each column.  Size must be equal to `input.num_columns()` or empty.
 * If empty, all columns will be sorted in `null_order::BEFORE`.
 * @param mr Device memory resource used to allocate the returned column's device memory
 * @return A non-nullable column of `size_type` elements containing the permuted row indices of
 * `input` if it were sorted

std::unique_ptr<column> (table_view input,
                                     std::vector<order> const& column_order,
                                     std::vector<null_order> const& null_precedence,
                                     rmm::mr::device_memory_resource* mr)
 */
/*
void sorted_order_arrow(std::shared_ptr<arrow::Table> input,
                        std::vector<cudf::order> const& column_order,
                        std::vector<cudf::null_order> const& null_precedence)
{
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
  for (int c = 0; c < input->columns().size(); ++c) {
    auto col = input->column(c);
    std::shared_ptr<arrow::Array> values = arrow::Concatenate(col->chunks()).ValueOrDie();
    std::shared_ptr<arrow::Array> indices = arrow::compute::SortToIndices(*values).ValueOrDie();
    //std::shared_ptr<arrow::ChunkedArray> sorted = arrow::compute::Take(*col, *indices).ValueOrDie();
  }
  auto new_schema = ral::cpu::utilities::build_arrow_schema(columns, input->ColumnNames(), input->schema()->metadata());
  return   
}

TEST(BlazingHostTable, arrow_slice) {
 
}
*/
static inline void AddColumnTransport(
	std::vector<blazingdb::transport::ColumnTransport> & columnTransports,
	const cudf::type_id type_id,
	const std::string & columnName,
	const std::size_t columnSize) {
	const std::int32_t dtype = static_cast<std::int32_t>(
		static_cast<std::underlying_type_t<cudf::type_id>>(type_id));
	blazingdb::transport::ColumnTransport::MetaData metadata{dtype, 0, 0, {0}};

	std::strncpy(metadata.col_name,
		columnName.c_str(),
		sizeof(blazingdb::transport::ColumnTransport::MetaData::col_name));

	columnTransports.emplace_back(blazingdb::transport::ColumnTransport{
		metadata, -1, -1, -1, -1, -1, 0, 0, columnSize});
}

TEST(BlazingHostTable, ToArrowTable) {
	std::size_t columnLength = 10;
	std::size_t columnSize = columnLength * sizeof(std::int32_t);

	std::vector<blazingdb::transport::ColumnTransport> column_transports;
	AddColumnTransport(
		column_transports, cudf::type_id::INT32, "int32-col", columnSize);

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
	static const char * expected = R"del(int32-col: int32
----
int32-col:
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
