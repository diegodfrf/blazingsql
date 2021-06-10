set(kernel_projection_test_sources
        kernel_projection_test.cpp
)
configure_test(kernel_projection_test "${kernel_projection_test_sources}")

# set(window_overlap_kernel_test_sources
#         window_overlap_kernel_test.cpp
# )
# configure_test(window_overlap_kernel_test "${window_overlap_kernel_test_sources}")


message("CONDA_PREFIX=${CONDA_PREFIX}")
if( CONDA_PREFIX )
    message("${CONDA_PREFIX} evaluates to True")
    set(DATASET_PATH ${CONDA_PREFIX}/blazingsql-testing-files/data/tpch)
    set(compute_test_sources
        generators/data_generator.cpp
        compute_test.cpp
        )
    configure_test(compute_test "${compute_test_sources}")
endif()

