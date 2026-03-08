add_library(test-lib STATIC)
target_sources(test-lib
  PUBLIC
    boost_test_tree_lister.cc
    boost_tree_lister_injector.cc
  PRIVATE
    cql_assertions.cc
    dummy_sharder.cc
    exception_utils.cc
    log.cc
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
# gcs_fixture.cc: "using namespace utils::gcp;" brings gcp::storage into scope,
# causing 'storage' ambiguity with other definitions.
# boost_test_tree_lister.cc: PUBLIC source propagated to dependent targets (e.g.
# test-perf) where 'using namespace seastar;' causes 'internal' ambiguity.
# aws_kms_fixture.cc, azure_kms_fixture.cc: reference encryption:: symbols not
# linked by all consumers; bundling them into a unity TU forces the linker to
# pull in unresolved symbols that would otherwise be dead-stripped.
set_source_files_properties(gcs_fixture.cc boost_test_tree_lister.cc
    aws_kms_fixture.cc azure_kms_fixture.cc
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
target_include_directories(test-lib
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(test-lib
  PUBLIC
    scylla-main
    replica
    Seastar::seastar
    xxHash::xxhash
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
