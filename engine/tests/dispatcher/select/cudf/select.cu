
#include "compute/backend_register.h"
#include "../select.h"
 
void cuda_select_impl(int type, int64_t a, int64_t b) {
  std::cout << "SELECT: cuda : a+b " << a + b << std::endl;
}
REGISTER_DISPATCH(select_stub, &cuda_select_impl)

 