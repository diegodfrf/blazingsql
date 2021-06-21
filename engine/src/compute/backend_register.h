#pragma once

#include <iostream>
#include "backend.h"


template <typename FnPtr, typename T> struct DispatchStub;

template <typename rT, typename T, typename... Args>
struct DispatchStub<rT (*)(Args...), T> {
  using FnPtr = rT (*)(Args...);

  DispatchStub() = default;
  DispatchStub(const DispatchStub &) = delete;
  DispatchStub &operator=(const DispatchStub &) = delete;

public:
  template <typename... ArgTypes>
  rT operator()(ral::execution::execution_backend backend, ArgTypes &&...args) {
    FnPtr call_ptr = nullptr;
    if (backend.id() == ral::execution::backend_id::ARROW){
      call_ptr = CPU_BACKEND;
    } else {
#ifdef CUDF_SUPPORT
      call_ptr = CUDA_BACKEND;
#endif
    }
    assert(call_ptr != nullptr);
    return (*call_ptr)(std::forward<ArgTypes>(args)...);
  }

  static FnPtr CPU_BACKEND;
#ifdef CUDF_SUPPORT
  static FnPtr CUDA_BACKEND;
#endif 
};

#define DECLARE_DISPATCH(fn, name)                                             \
  struct name : DispatchStub<fn, name> {                                       \
    name() = default;                                                          \
    name(const name &) = delete;                                               \
    name &operator=(const name &) = delete;                                    \
  };                                                                           \
  extern struct name name

#define DEFINE_DISPATCH(name) struct name name

#define REGISTER_ARCH_DISPATCH(name, arch, fn)                                 \
  template <> decltype(fn) DispatchStub<decltype(fn), struct name>::arch = fn;

#define REGISTER_CUDF_DISPATCH(name, fn)                                            \
  REGISTER_ARCH_DISPATCH(name, CUDA_BACKEND, fn)
#define REGISTER_ARROW_DISPATCH(name, fn)                                            \
  REGISTER_ARCH_DISPATCH(name, CPU_BACKEND, fn)