#=============================================================================
# Copyright (c) 2018-2020, NVIDIA CORPORATION.
# Copyright 2018-2019 BlazingDB, Inc.
#     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
#=============================================================================

cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

option(CUDF_SUPPORT "Enables support for cudf compute backend" OFF)

if(CUDF_SUPPORT)
    project(blazingdb-ral C CXX CUDA)
    message(STATUS "cudf compute backend support is enabled!")
else()
    project(blazingdb-ral C CXX)
    message(STATUS "cudf compute backend support is disabled!")
endif()

###################################################################################################
# - compiler options ------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_COMPILER $ENV{CC})
set(CMAKE_CXX_COMPILER $ENV{CXX})
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX)
    # NOTE felipe percy these flags are too strict for blazingsql
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wno-error=deprecated-declarations")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
endif(CMAKE_COMPILER_IS_GNUCXX)

# To apply RUNPATH to transitive dependencies (this is a temporary solution)
set(CMAKE_SHARED_LINKER_FLAGS "-Wl,--disable-new-dtags")
set(CMAKE_EXE_LINKER_FLAGS "-Wl,--disable-new-dtags")

# Build options
option(BUILD_SHARED_LIBS "Build shared libraries" ON)
option(BUILD_TESTS "Configure CMake to build tests" ON)
option(BUILD_BENCHMARKS "Configure CMake to build (google) benchmarks" OFF)

option(GCS_SUPPORT "Enables support for GCS fileSystem" ON)
option(S3_SUPPORT "Enables support for S3 fileSystem" ON)

option(MYSQL_SUPPORT "Enables support for MySQL database" ON)
option(SQLITE_SUPPORT "Enables support for SQLite database" ON)
option(POSTGRESQL_SUPPORT "Enables support for PostgreSQL database" ON)
option(SNOWFLAKE_SUPPORT "Enables support for SnowFlake database" ON)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/" ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

###################################################################################################
# - conda environment -----------------------------------------------------------------------------

set(BSQL_BLD_PREFIX "")
if ("$ENV{CONDA_BUILD}" STREQUAL "1")
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{BUILD_PREFIX};$ENV{PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda build detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
    set(BSQL_BLD_PREFIX "$ENV{BUILD_PREFIX}")

    set(AWS_SDK_CPP_INSTALL_DIR $ENV{BUILD_PREFIX})
    set(GOOGLE_CLOUD_CPP_INSTALL_DIR $ENV{BUILD_PREFIX})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{BUILD_PREFIX}/lib/cmake)
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{BUILD_PREFIX}/lib64/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{BUILD_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{BUILD_PREFIX}/lib/) # to link the tests correctly against libcurl.so
    link_directories($ENV{BUILD_PREFIX}/lib/)
    link_directories($ENV{BUILD_PREFIX}/lib64/)

elseif (DEFINED ENV{CONDA_PREFIX})
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{CONDA_PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda environment detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
    set(BSQL_BLD_PREFIX "$ENV{CONDA_PREFIX}")

    set(AWS_SDK_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(GOOGLE_CLOUD_CPP_INSTALL_DIR $ENV{CONDA_PREFIX})
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CONDA_PREFIX}/lib/cmake)
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} $ENV{CONDA_PREFIX}/lib64/cmake)
    set(ENV{PKG_CONFIG_PATH} $ENV{CONDA_PREFIX}/lib/pkgconfig/) # used by find libcurl and openssl
    set(ENV{LD_LIBRARY_PATH} $ENV{CONDA_PREFIX}/lib/) # to link the tests correctly against libcurl.so
    link_directories($ENV{CONDA_PREFIX}/lib/)
    link_directories($ENV{CONDA_PREFIX}/lib64/)

endif ()

if(CUDF_SUPPORT)
    include(InitCudaCompiler)
endif()

message(WARNING "BSQL_BLD_PREFIX set to: ${BSQL_BLD_PREFIX}")
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

#########################################################################3

# Include CMake modules
include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

find_package(CURL REQUIRED)

# this breaks powerpc custom compiler
#find_package(Threads)

find_package(PythonLibs)
# TODO jp mario c.gonzales we must use find here!
#find_package(spdlog REQUIRED)

find_package(GTest REQUIRED)

# TODO percy arrow felipe see we we will use ucx on cudf workflows only
if(CUDF_SUPPORT)
    include(ConfigureUcx)
endif()

# BEGIN find arrow
find_path(ARROW_INCLUDE_DIR "arrow" HINTS "$ENV{ARROW_ROOT}/include")
find_library(ARROW_LIB "arrow" NAMES libarrow HINTS "$ENV{ARROW_ROOT}/lib" "$ENV{ARROW_ROOT}/build")
message(STATUS "ARROW: ARROW_INCLUDE_DIR set to ${ARROW_INCLUDE_DIR}")
message(STATUS "ARROW: ARROW_LIB set to ${ARROW_LIB}")
add_library(arrow SHARED IMPORTED ${ARROW_LIB})

if(ARROW_INCLUDE_DIR AND ARROW_LIB)
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${ARROW_LIB})
endif(ARROW_INCLUDE_DIR AND ARROW_LIB)
# END find arrow

# BEGIN MAIN #

# TODO percy move this to tool-chain and add support for cuda def and more than 1 defs
add_definitions(${CXX_DEFINES})
get_directory_property(CXX_COMPILE_DEFINITIONS DIRECTORY ${PROJECT_SOURCE_DIR} COMPILE_DEFINITIONS)
message(STATUS "C++ compiler definitions: ${CXX_COMPILE_DEFINITIONS}")

include_directories(
    ${PROJECT_BINARY_DIR}
    ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR}
    ${BSQL_BLD_PREFIX}/include
    ${BSQL_BLD_PREFIX}/include/blazingdb/io
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/thirdparty/jitify
    ${PYTHON_INCLUDE_DIRS}
    "${ARROW_INCLUDE_DIR}"
    )

if(CUDF_SUPPORT)
    include(ConfigureCudfCompute)
endif()

if(MYSQL_SUPPORT AND CUDF_SUPPORT) # TODO percy arrow 4 c.gonzales cudf & sql providers
    add_definitions(-DMYSQL_SUPPORT)
    set(MYSQL_DATABASE_SRC_FILES
        # TODO percy in case we need custom mysql transformer
        #${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/MySQLTransformer.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/MySQLDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/MySQLParser.cpp)
    set(MYSQL_LIBRARY libmysqlcppconn-static.a)
    message(STATUS "MySQL support is enabled!")
else()
    message(STATUS "MySQL support is disabled!")
endif()

if(SQLITE_SUPPORT AND CUDF_SUPPORT) # TODO percy arrow 4 c.gonzales cudf & sql providers
    add_definitions(-DSQLITE_SUPPORT)
    set(SQLITE_DATABASE_SRC_FILES
        # TODO percy c.gonzales
        #${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/SQLiteTransformer.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/SQLiteDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/SQLiteParser.cpp)
    set(SQLITE_LIBRARY libsqlite3.so)
    message(STATUS "SQLite support is enabled!")
else()
    message(STATUS "SQLite support is disabled!")
endif()

if(POSTGRESQL_SUPPORT AND CUDF_SUPPORT) # TODO percy arrow 4 c.gonzales cudf & sql providers
    # TODO postgresql percy c.gonzales
    add_definitions(-DPOSTGRESQL_SUPPORT)
    set(POSTGRESQL_DATABASE_SRC_FILES
        # TODO percy c.gonzales
        #${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/PostgreSQLTransformer.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/PostgreSQLDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/PostgreSQLParser.cpp)
    set(POSTGRESQL_LIBRARY libpq.so)
    message(STATUS "PostgreSQL support is enabled!")
else()
    message(STATUS "MyPostgreSQL support is disabled!")
endif()

if(SNOWFLAKE_SUPPORT AND CUDF_SUPPORT) # TODO percy arrow 4 c.gonzales cudf & sql providers
  add_definitions(-DSNOWFLAKE_SUPPORT)
    set(SNOWFLAKE_DATABASE_SRC_FILES
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/SnowFlakeDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/sql/SnowFlakeParser.cpp)
      set(SNOWFLAKE_LIBRARY libodbc.so)
      message(STATUS "SnowFlake support is enabled!")
else()
  message(STATUS "SnowFlake support is disabled!")
endif()

set(COMPUTE_ARROW_SRC_FILES
    ${PROJECT_SOURCE_DIR}/src/compute/arrow/detail/aggregations.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/arrow/detail/types.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/arrow/detail/scalars.cpp
    ${PROJECT_SOURCE_DIR}/src/compute/arrow/detail/io.cpp
)

## Target source files
set(SRC_FILES
        # COMPUTE
        ${COMPUTE_CUDF_SRC_FILES}
        ${COMPUTE_ARROW_SRC_FILES}

        ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingHostTable.cpp
        ${PROJECT_SOURCE_DIR}/src/cache_machine/CacheData.cpp
        ${PROJECT_SOURCE_DIR}/src/cache_machine/CacheDataIO.cpp
        ${PROJECT_SOURCE_DIR}/src/cache_machine/CacheDataLocalFile.cpp
        ${PROJECT_SOURCE_DIR}/src/cache_machine/ConcatCacheData.cpp
        ${PROJECT_SOURCE_DIR}/src/cache_machine/CPUCacheData.cpp
        ${PROJECT_SOURCE_DIR}/src/cache_machine/ArrowCacheData.cpp
        ${PROJECT_SOURCE_DIR}/src/cache_machine/CacheMachine.cpp
        ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingArrowTable.cpp
        ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingArrowTableView.cpp
        ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingTable.cpp
        ${PROJECT_SOURCE_DIR}/src/blazing_table/BlazingTableView.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_graph/port.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/kernel.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_graph/executor.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/distributing_kernel.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/kernel_type.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_graph/Context.cpp
        ${PROJECT_SOURCE_DIR}/src/bmr/MemoryMonitor.cpp
        ${PROJECT_SOURCE_DIR}/src/bmr/BufferProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/bmr/BlazingMemoryResource.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_graph/graph.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchAggregationProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchJoinProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchOrderByProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchUnionProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchFilterProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchProjectionProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchScanProcessing.cpp
        # TODO percy arrow
        #${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchWindowFunctionProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_kernels/BatchProcessing.cpp
        ${PROJECT_SOURCE_DIR}/src/operators/OrderBy.cpp
        ${PROJECT_SOURCE_DIR}/src/operators/GroupBy.cpp
        ${PROJECT_SOURCE_DIR}/src/operators/LogicalFilter.cpp
        ${PROJECT_SOURCE_DIR}/src/operators/LogicalProject.cpp
        ${PROJECT_SOURCE_DIR}/src/operators/Concatenate.cpp
        ${PROJECT_SOURCE_DIR}/src/operators/Distribution.cpp
        ${PROJECT_SOURCE_DIR}/src/operators/Types.cpp

        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/compatibility/SQLTranspiler.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/sql/AbstractSQLDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/UriDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_provider/ArrowDataProvider.cpp
        ${PROJECT_SOURCE_DIR}/src/io/Schema.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/ParquetParser.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/CSVParser.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/JSONParser.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/OrcParser.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/ArrowParser.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/ArgsUtil.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/metadata/parquet_metadata.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/metadata/orc_metadata.cpp
        ${PROJECT_SOURCE_DIR}/src/io/data_parser/metadata/common_metadata.cpp

        ${PROJECT_SOURCE_DIR}/src/utilities/DebuggingUtils.cpp
        ${PROJECT_SOURCE_DIR}/src/parser/CalciteExpressionParsing.cpp
        ${PROJECT_SOURCE_DIR}/src/io/DataLoader.cpp
        ${PROJECT_SOURCE_DIR}/src/execution_graph/manager.cpp
        ${PROJECT_SOURCE_DIR}/src/parser/expression_utils.cpp
        ${PROJECT_SOURCE_DIR}/src/parser/expression_tree.cpp

        ${PROJECT_SOURCE_DIR}/src/parser/filter_parser_utils.cpp
        ${PROJECT_SOURCE_DIR}/src/parser/groupby_parser_utils.cpp
        ${PROJECT_SOURCE_DIR}/src/parser/project_parser_utils.cpp
        ${PROJECT_SOURCE_DIR}/src/parser/orderby_parser_utils.cpp
        ${PROJECT_SOURCE_DIR}/src/parser/types_parser_utils.cpp

        # TODO percy arrow
        #${PROJECT_SOURCE_DIR}/src/skip_data/SkipDataProcessor.cpp
        #${PROJECT_SOURCE_DIR}/src/skip_data/utils.cpp
        
        ${PROJECT_SOURCE_DIR}/src/cython/static.cpp
        ${PROJECT_SOURCE_DIR}/src/cython/initialize.cpp
        ${PROJECT_SOURCE_DIR}/src/cython/io.cpp
        ${PROJECT_SOURCE_DIR}/src/cython/errors.cpp
        ${PROJECT_SOURCE_DIR}/src/cython/engine.cpp

        ${PROJECT_SOURCE_DIR}/src/communication/factory/MessageFactory.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationData.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/messages/GPUComponentMessage.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/serializer.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/node.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/messageReceiver.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/messageListener.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/bufferTransport.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/protocols.cpp
        ${PROJECT_SOURCE_DIR}/src/communication/CommunicationInterface/messageSender.cpp

        ${PROJECT_SOURCE_DIR}/src/transport/Node.cpp
        
        ${PROJECT_SOURCE_DIR}/src/compute/api.h

        # SQL
        ${MYSQL_DATABASE_SRC_FILES}
        ${SQLITE_DATABASE_SRC_FILES}
        ${POSTGRESQL_DATABASE_SRC_FILES}
        ${SNOWFLAKE_DATABASE_SRC_FILES}
    )

link_directories(${BSQL_BLD_PREFIX}/lib)

# These new var are for custom build process (powerpc)
link_directories($ENV{SNAPPY_INSTALL_DIR})
link_directories($ENV{LZ4_INSTALL_DIR})

add_library(blazingsql-engine SHARED
    ${SRC_FILES}
    ${MYSQL_DATABASE_SRC_FILES}
    ${SQLITE_DATABASE_SRC_FILES}
    ${POSTGRESQL_DATABASE_SRC_FILES}
    ${SNOWFLAKE_DATABASE_SRC_FILES}
)

if(GCS_SUPPORT)
    find_package(storage_client REQUIRED)
    set(GCS_LIBRARY storage_client google_cloud_cpp_common crc32c)
endif()

if(S3_SUPPORT)
    set(S3_LIBRARY aws-cpp-sdk-s3 aws-cpp-sdk-s3-encryption)
endif()

set_target_properties(blazingsql-engine PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(blazingsql-engine
    blazingdb-io
    #Threads::Threads
    pthread

    ${CUDF_COMPUTE_LIBRARIES}

    zmq

    ${UCX_LIBRARIES}

    parquet
    arrow
    snappy

    zstd
    lz4

    ${S3_LIBRARY}

    ${GCS_LIBRARY}

    libboost_filesystem.so
    libboost_system.so
    libboost_regex.so

    # TODO jp c.gonzales make use of standard cmake find_package here (see above)
    #spdlog::spdlog
    libspdlog.so

    # Library needed for blazing communication repository (debug)
    gcov

    # TODO percy use cmake::find
    ${MYSQL_LIBRARY}
    ${SQLITE_LIBRARY}
    ${POSTGRESQL_LIBRARY}
    ${SNOWFLAKE_LIBRARY}
)

if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo" OR "$ENV{CONDA_BUILD}" STREQUAL "1")
    message(STATUS "This is a release build so the final runtime binaries will not depend on gtest")
else()
    message(WARNING "This is not a release build so the final runtime binaries will depend on gtest (BSQLDBGUTILS precompiler definition is set)")
    add_definitions(-DBSQLDBGUTILS) # see DebuggingUtils.cpp
    target_link_libraries(blazingsql-engine
        gtest
    )
    
    if(CUDF_SUPPORT)
        target_link_libraries(blazingsql-engine
            cudftestutil
        )
    endif()
endif()

#install(TARGETS testing-libgdf_lib DESTINATION lib RENAME libblazingsql-engine.a)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libblazingsql-engine.so DESTINATION lib RENAME libblazingsql-engine.so)

# Tests
if(BUILD_TESTING)
    if(GTEST_FOUND AND CUDF_SUPPORT) # TODO percy arrow unit tests
        add_subdirectory(tests)
    else()
        message(AUTHOR_WARNING "Google C++ Testing Framework (Google Test) not found: automated tests are disabled.")
    endif()
endif()

#Benchmarks
if(GBENCHMARK_FOUND AND CUDF_SUPPORT) # TODO percy arrow benchmarks
    add_subdirectory(benchmarks)
else()
    message(AUTHOR_WARNING "Google C++ Benchmarking Framework (Google Benchmark) not found")
endif()


include(ConfigureProductVersion)

# Print the project summary
feature_summary(WHAT ALL INCLUDE_QUIET_PACKAGES FATAL_ON_MISSING_REQUIRED_PACKAGES)

#END MAIN #
