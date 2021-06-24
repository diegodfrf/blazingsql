# set(kernel_projection_test_sources
#         kernel_projection_test.cpp
# )
# configure_test(kernel_projection_test "${kernel_projection_test_sources}")

# set(window_overlap_kernel_test_sources
#         window_overlap_kernel_test.cpp
# )
# configure_test(window_overlap_kernel_test "${window_overlap_kernel_test_sources}")


set(DATASET_PATH $ENV{CONDA_PREFIX}/blazingsql-testing-files/data/tpch)
message("TPCH Dataset Path {DATASET_PATH}")

set(arrow_compute_test_sources
    arrow/generators/data_generator.cpp
    arrow/compute_test.cpp
    )
configure_test(arrow_compute_test "${arrow_compute_test_sources}")
target_compile_definitions(arrow_compute_test PUBLIC -DDATASET_PATH="${DATASET_PATH}")


if(CUDF_SUPPORT)
    set(DATASET_PATH $ENV{CONDA_PREFIX}/blazingsql-testing-files/data/tpch)
    message("TPCH Dataset Path {DATASET_PATH}")

    set(compute_test_sources
        generators/data_generator.cpp
        compute_test.cpp
        )
    configure_test(compute_test "${compute_test_sources}")
    target_compile_definitions(compute_test PUBLIC -DDATASET_PATH="${DATASET_PATH}")
endif()