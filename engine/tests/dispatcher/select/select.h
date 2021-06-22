#pragma once

#include "compute/backend_register.h"
#include <cstdint>
 
using select_fn = void (*)(int type, int64_t a, int64_t b);
DECLARE_DISPATCH(select_fn, select_stub);

 