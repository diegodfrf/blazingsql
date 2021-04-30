#pragma once

#include "execution_kernels/LogicPrimitives.h"

namespace ral{
namespace execution {

template <typename Functor, typename... Ts>
CUDA_HOST_DEVICE_CALLABLE
constexpr decltype(auto) backend_dispatcher(execution::execution_backend backend,
                                            Functor f,
                                            Ts&&... args)
{
  switch (backend.id()) {
    case backend_id::ARROW:
    return f.template operator()<ral::frame::BlazingArrowTable>(std::forward<Ts>(args)...);
    case backend_id::CUDF:
    return f.template operator()<ral::frame::BlazingCudfTable>(std::forward<Ts>(args)...);
  }
}

} //namespace execution

} //namespace ral
