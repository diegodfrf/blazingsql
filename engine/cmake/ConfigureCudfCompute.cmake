#=============================================================================
# Copyright 2021 VoltronData, Inc.
#     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
#=============================================================================

# TODO percy arrow improve our cmake organization for compute backends

set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

if(CMAKE_CUDA_COMPILER_VERSION)
  # Compute the version. from  CMAKE_CUDA_COMPILER_VERSION
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\1" CUDA_VERSION_MAJOR ${CMAKE_CUDA_COMPILER_VERSION})
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\2" CUDA_VERSION_MINOR ${CMAKE_CUDA_COMPILER_VERSION})
  set(CUDA_VERSION "${CUDA_VERSION_MAJOR}.${CUDA_VERSION_MINOR}" CACHE STRING "Version of CUDA as computed from nvcc.")
  mark_as_advanced(CUDA_VERSION)
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
    # Suppress parentheses warning which causes gmock to fail
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wno-parentheses")
endif(CMAKE_COMPILER_IS_GNUCXX)

message(STATUS "CUDA_VERSION_MAJOR: ${CUDA_VERSION_MAJOR}")
message(STATUS "CUDA_VERSION_MINOR: ${CUDA_VERSION_MINOR}")
message(STATUS "CUDA_VERSION: ${CUDA_VERSION}")

# Always set this convenience variable
set(CUDA_VERSION_STRING "${CUDA_VERSION}")

# Auto-detect available GPU compute architectures
set(GPU_ARCHS "ALL" CACHE STRING
  "List of GPU architectures (semicolon-separated) to be compiled for. Pass 'ALL' if you want to compile for all supported GPU architectures. Empty string means to auto-detect the GPUs on the current system")

if("${GPU_ARCHS}" STREQUAL "")
  include(cmake/EvalGpuArchs.cmake)
  evaluate_gpu_archs(GPU_ARCHS)
endif()

if("${GPU_ARCHS}" STREQUAL "ALL")
  set(GPU_ARCHS "60")
  if((CUDA_VERSION_MAJOR EQUAL 9) OR (CUDA_VERSION_MAJOR GREATER 9))
    set(GPU_ARCHS "${GPU_ARCHS};70")
  endif()
  if((CUDA_VERSION_MAJOR EQUAL 10) OR (CUDA_VERSION_MAJOR GREATER 10))
    set(GPU_ARCHS "${GPU_ARCHS};75")
  endif()
endif()
message("GPU_ARCHS = ${GPU_ARCHS}")

foreach(arch ${GPU_ARCHS})
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_${arch},code=sm_${arch}")
endforeach()

list(GET GPU_ARCHS -1 ptx)
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_${ptx},code=compute_${ptx}")

set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --expt-extended-lambda --expt-relaxed-constexpr")

# set warnings as errors
# TODO: remove `no-maybe-unitialized` used to suppress warnings in rmm::exec_policy
# NOTE felipe percy these flags are too strict for blazingsql: -Werror,
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Werror=cross-execution-space-call -Xcompiler -Wall,-Wno-error=deprecated-declarations")

if(DISABLE_DEPRECATION_WARNING)
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wno-deprecated-declarations")
endif(DISABLE_DEPRECATION_WARNING)

if(CMAKE_CUDA_LINEINFO)
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -lineinfo")
endif(CMAKE_CUDA_LINEINFO)

# Debug options
if(CMAKE_BUILD_TYPE MATCHES Debug)
    message(STATUS "Building with debugging flags")
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -G -g -Xcompiler -rdynamic")
endif(CMAKE_BUILD_TYPE MATCHES Debug)

###################################################################################################
# - cudart options --------------------------------------------------------------------------------
if(CUDA_STATIC_RUNTIME)
    message(STATUS "Enabling static linking of cudart")
    set(CUDART_LIBRARY "cudart_static")
else()
    set(CUDART_LIBRARY "cudart")
endif(CUDA_STATIC_RUNTIME)
###################################################################################################

# BEGIN UCX
set(UCX_INSTALL_DIR $ENV{CONDA_PREFIX})
set(UCX_INCLUDE_DIR $ENV{CONDA_PREFIX}/include)
find_package(UCX REQUIRED)
# END UCX

# BEGIN find arrow
find_library(ARROW_CUDA_LIB "arrow_cuda" NAMES libarrow_cuda HINTS "$ENV{ARROW_ROOT}/lib" "$ENV{ARROW_ROOT}/build")
message(STATUS "ARROW: ARROW_CUDA_LIB set to ${ARROW_CUDA_LIB}")
add_library(arrow_cuda SHARED IMPORTED ${ARROW_CUDA_LIB})

if(ARROW_INCLUDE_DIR AND ARROW_LIB AND ARROW_CUDA_LIB)
  set_target_properties(arrow_cuda PROPERTIES IMPORTED_LOCATION ${ARROW_CUDA_LIB})
endif(ARROW_INCLUDE_DIR AND ARROW_LIB AND ARROW_CUDA_LIB)
# END find arrow

# NOTE this is the main def for the RAL libblazingsql engine! percy arrow
add_definitions(-DCUDF_SUPPORT)

set(COMPUTE_CUDF_SRC_FILES
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/interops.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/aggregations.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/interpreter/interpreter_cpp.cu
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/transform.cu
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/filter.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/join.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/nulls.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/search.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/concatenate.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/types.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/scalars.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/cudf/detail/io.cpp

    ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingCudfTable.cpp
    ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingCudfTableView.cpp
    ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingColumnOwner.cpp

    ${PROJECT_SOURCE_DIR}/src/communication/messages/MessageUtil.cu
    ${PROJECT_SOURCE_DIR}/src/cache_machine/GPUCacheData.cpp
    ${PROJECT_SOURCE_DIR}/src/config/GPUManager.cu
    ${PROJECT_SOURCE_DIR}/src/io/data_provider/GDFDataProvider.cpp
    ${PROJECT_SOURCE_DIR}/src/io/data_parser/GDFParser.cpp
    ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/AbstractSQLParser.cpp # TODO percy arrow 4 make sql ds compute agnostic!
)

link_directories(${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES})

include_directories(
    ${BSQL_BLD_PREFIX}/include/libcudf/libcudacxx/
    ${PROJECT_SOURCE_DIR}/thirdparty/jitify
    ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
)

set(CUDF_COMPUTE_LIBRARIES
    cudf
    cudart
)

###################################################################################################
# - build options ---------------------------------------------------------------------------------
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --default-stream=per-thread")

option(USE_NVTX "Build with NVTX support" ON)
if(USE_NVTX)
    message(STATUS "Using Nvidia Tools Extension")
    # The `USE_NVTX` macro is deprecated
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_NVTX")
else()
    # Makes NVTX APIs no-ops
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDISABLE_NVTX")
endif(USE_NVTX)

option(HT_DEFAULT_ALLOCATOR "Use the default allocator for hash tables" ON)
if(HT_DEFAULT_ALLOCATOR)
    message(STATUS "Using default allocator for hash tables")
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -DHT_DEFAULT_ALLOCATOR")
endif(HT_DEFAULT_ALLOCATOR)

###################################################################################################

message(STATUS "cmake_cuda_flags = ${CMAKE_CUDA_FLAGS}")

target_link_libraries(blazingsql-engine
    cudftestutil
)
