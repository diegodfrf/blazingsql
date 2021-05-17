set(provider_sources
    provider_test.cpp
)

configure_test(provider_test "${provider_sources}")

set(sql_transpiler_sources
    sql_transpiler_test.cpp
)

configure_test(sql_transpiler_test "${sql_transpiler_sources}")

# TODO percy c.gonzales add separated tests and add parser tests too
if(MYSQL_SUPPORT)
    set(sql_provider_sources
        sql_provider_test.cpp
    )
    configure_test(sql_provider_test "${sql_provider_sources}")
endif()

if(SNOWFLAKE_SUPPORT)
    set(snowflake_provider_sources snowflake_provider_test.cpp)
    configure_test(snowflake_provider_test "${snowflake_provider_sources}")
endif()

if(SQLITE_SUPPORT)
    set(sqlite_provider_sources sqlite_provider_test.cpp)
    configure_test(sqlite_provider_test "${sqlite_provider_sources}")
endif()

if(POSTGRESQL_SUPPORT)
    set(postgresql_provider_sources postgresql_provider_test.cpp)
    configure_test(postgresql_provider_test "${postgresql_provider_sources}")
endif()
