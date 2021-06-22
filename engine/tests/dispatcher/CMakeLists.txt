set(dispatcher_test_sources
        select/arrow/select.cpp
        select/cudf/select.cu
        dispatcher_test.cpp
)

configure_test(dispatcher_test "${dispatcher_test_sources}")