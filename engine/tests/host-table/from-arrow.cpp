#include <gtest/gtest.h>

#include "communication/messages/GPUComponentMessage.h"
#include <arrow/api.h>

#include <bmr/initializer.h>

#include <utilities/DebuggingUtils.h>


TEST(BlazingHostTable, FromArrowTable) {
	BlazingRMMInitialize("cuda_memory_resource");

	arrow::Int32Builder int32Builder;
	int32Builder.Append(1);
	int32Builder.Append(3);
	int32Builder.Append(5);
	int32Builder.Append(7);
	int32Builder.Append(9);
	std::shared_ptr<arrow::Array> int32Array;
	int32Builder.Finish(&int32Array);

	arrow::Int64Builder int64Builder;
	int64Builder.Append(0);
	int64Builder.Append(2);
	int64Builder.Append(4);
	int64Builder.Append(6);
	int64Builder.Append(8);
	std::shared_ptr<arrow::Array> int64Array;
	int64Builder.Finish(&int64Array);

	std::shared_ptr<arrow::Schema> schema =
		arrow::schema({arrow::field("col-32", arrow::int32()),
			arrow::field("col-64", arrow::int64())});

	std::shared_ptr<arrow::Table> table =
		arrow::Table::Make(schema, {int32Array, int64Array});

	std::shared_ptr<ral::frame::BlazingArrowTableView> blazingArrowTableView =
		std::make_shared<ral::frame::BlazingArrowTableView>(table);

	std::unique_ptr<ral::frame::BlazingHostTable> blazingHostTable =
		ral::communication::messages::serialize_arrow_message_to_host_table(
			blazingArrowTableView);

	std::cout << ">>> num rows = " << blazingHostTable->num_rows() << std::endl;
	std::cout << ">>> num columns = " << blazingHostTable->num_columns()
			  << std::endl;

	std::cout << ">>> column names: " << std::endl;
	for (const std::string & columnName : blazingHostTable->column_names()) {
		std::cout << "... " << columnName << std::endl;
	}

	std::unique_ptr<ral::frame::BlazingCudfTable> blazingCudfTable =
		blazingHostTable->get_cudf_table();

	ral::utilities::print_blazing_cudf_table_view(
		blazingCudfTable->to_table_view(), "table");

	BlazingRMMFinalize();
}
