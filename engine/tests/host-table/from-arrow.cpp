#include <gtest/gtest.h>

#include "communication/messages/GPUComponentMessage.h"
#include <arrow/api.h>

#include <bmr/initializer.h>

//#include <utilities/DebuggingUtils.h>

TEST(BlazingHostTable, FromArrowTable) {
	BlazingRMMInitialize("cuda_memory_resource");
	arrow::Int32Builder builder;

	builder.Append(1);
	builder.Append(3);
	builder.Append(5);
	builder.Append(7);
	builder.Append(9);

	std::shared_ptr<arrow::Array> array;
	builder.Finish(&array);

	std::shared_ptr<arrow::Schema> schema =
		arrow::schema({arrow::field("col-32", arrow::int32())});

	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});

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

	// std::unique_ptr<ral::frame::BlazingCudfTable> blazingCudfTable =
	// blazingHostTable->get_cudf_table();

	BlazingRMMFinalize();
}
