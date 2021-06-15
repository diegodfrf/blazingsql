#pragma once

#ifdef CUDF_SUPPORT
#include "blazing_table/BlazingCudfTable.h"
#endif

#include "blazing_table/BlazingArrowTable.h"

namespace ral{
namespace execution {

template <typename Functor, typename... Ts>
CUDA_HOST_DEVICE_CALLABLE
constexpr decltype(auto) backend_dispatcher(execution_backend backend, Functor f,
                                            Ts&&... args)
{
  switch (backend.id()) {
    case backend_id::ARROW:
    return f.template operator()<ral::frame::BlazingArrowTable>(std::forward<Ts>(args)...);

#ifdef CUDF_SUPPORT
    case backend_id::CUDF:
    return f.template operator()<ral::frame::BlazingCudfTable>(std::forward<Ts>(args)...);
#endif
  }
}

} //namespace execution

} //namespace ral
