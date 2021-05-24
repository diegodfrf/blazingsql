#include <gtest/gtest.h>

#include "communication/messages/GPUComponentMessage.h"
#include <arrow/api.h>

TEST(BlazingHostTable, FromArrowTable) {
  arrow::Int32Builder builder;

  builder.Append(1);
  builder.Append(3);
  builder.Append(5);
  builder.Append(7);
  builder.Append(9);

  std::shared_ptr<arrow::Array> array;
  builder.Finish(&array);
}
