#=============================================================================
# Copyright 2021 VoltronData, Inc.
#     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
#=============================================================================

add_definitions(-DCUDF_SUPPORT)

link_directories(${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES})

include_directories(
    ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
    ${BSQL_BLD_PREFIX}/include/libcudf/libcudacxx/
    )

set(CUDF_COMPUTE_LIBRARIES
    cudf
    cudart
)

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

    ${PROJECT_SOURCE_DIR}/src/cache_machine/cudf/detail/CacheDataLocalFile.cpp
    ${PROJECT_SOURCE_DIR}/src/cache_machine/cudf/CacheMachine.cpp

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

find_library(ARROW_CUDA_LIB "arrow_cuda" NAMES libarrow_cuda HINTS "$ENV{ARROW_ROOT}/lib" "$ENV{ARROW_ROOT}/build")
message(STATUS "ARROW: ARROW_CUDA_LIB set to ${ARROW_CUDA_LIB}")
add_library(arrow_cuda SHARED IMPORTED ${ARROW_CUDA_LIB})

if(ARROW_INCLUDE_DIR AND ARROW_LIB AND ARROW_CUDA_LIB)
  set_target_properties(arrow_cuda PROPERTIES IMPORTED_LOCATION ${ARROW_CUDA_LIB})
endif(ARROW_INCLUDE_DIR AND ARROW_LIB AND ARROW_CUDA_LIB)
