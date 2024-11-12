add_executable(kuzu-wasm
    main.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/connection.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/database.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/data_type.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/helpers.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/flat_tuple.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/prepared_statement.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/query_result.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/query_summary.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/value.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/version.cpp
)
target_link_libraries(kuzu-wasm PRIVATE kuzu)
