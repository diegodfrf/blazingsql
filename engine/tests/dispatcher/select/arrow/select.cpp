
#include "compute/backend_register.h"
#include "../select.h"
 
void cpu_select_impl(int type, int64_t a, int64_t b) {
  std::cout << "SELECT: cpu : a+b " << a + b << std::endl;
}

REGISTER_CPU_DISPATCH(select_stub, &cpu_select_impl)
 