
#include <gtest/gtest.h>
#include "select/select.h"


using ral::execution::backend_id;
using ral::execution::execution_backend;

struct DispatcherTest : public ::testing::Test {
 
};

DEFINE_DISPATCH(select_stub);


TEST_F(DispatcherTest, arrow_only)
{
  auto backend = execution_backend(backend_id::ARROW);
  select_stub(backend, 0, 5, 8);
}
 


TEST_F(DispatcherTest, cudf_only)
{
  auto backend = execution_backend(backend_id::CUDF);
  select_stub(backend, 0, 5, 8);
}
 