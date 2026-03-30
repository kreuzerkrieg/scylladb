# test-lib-core: minimal test infrastructure for lightweight (BOOST) tests.
# Provides the boost test tree lister, logging, and basic test utilities
# without pulling in Scylla's full codebase.
add_library(test-lib-core STATIC)
target_sources(test-lib-core
  PRIVATE
    boost_test_tree_lister.cc
    boost_tree_lister_injector.cc
    exception_utils.cc
    log.cc)
target_include_directories(test-lib-core
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(test-lib-core
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    Boost::regex
    Boost::unit_test_framework)

# test-lib: full test infrastructure for SEASTAR tests that need the
# complete Scylla environment (cql_test_env, mutation helpers, etc.).
add_library(test-lib STATIC)
target_sources(test-lib
  PRIVATE
    cql_assertions.cc
    dummy_sharder.cc
    test_utils.cc
    tmpdir.cc
    cql_test_env.cc
    expr_test_utils.cc
    test_services.cc
    key_utils.cc
    mutation_assertions.cc
    mutation_source_test.cc
    random_schema.cc
    result_set_assertions.cc
    sstable_run_based_compaction_strategy_for_tests.cc
    sstable_utils.cc
    data_model.cc
    eventually.cc
    proc_utils.cc
    gcs_fixture.cc
    aws_kms_fixture.cc
    azure_kms_fixture.cc
    limiting_data_source.cc
    )
target_include_directories(test-lib
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(test-lib
  PUBLIC
    test-lib-core
    scylla-main
    replica
  PRIVATE
    audit
    auth
    cdc
    compaction
    dht   # used by cql3
    gms
    types
    idl
    index # used by cql3
    locator
    message
    schema
    scylla_version
    service
    sstables
    vector_search # used by cql3
    utils
    Boost::regex
    Boost::unit_test_framework)
