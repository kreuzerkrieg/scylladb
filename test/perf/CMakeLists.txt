function(add_perf_test name)
  cmake_parse_arguments(parsed_args
    ""
    ""
    "LIBRARIES"
    ${ARGN})
  set(src "${name}.cc")
  add_executable(${name} ${src})
  target_include_directories(${name}
    PRIVATE
      ${CMAKE_SOURCE_DIR})
  target_link_libraries(${name}
    PRIVATE
      perf-lib
      test-lib
      Seastar::seastar_perf_testing
      Seastar::seastar
      xxHash::xxhash)
  if(parsed_args_LIBRARIES)
    target_link_libraries(${name}
      PRIVATE
        ${parsed_args_LIBRARIES})
  endif()
endfunction()

add_library(test-perf STATIC)
target_sources(test-perf
  PRIVATE
    perf_alternator.cc
    perf_fast_forward.cc
    perf_row_cache_update.cc
    perf_simple_query.cc
    perf_cql_raw.cc
    perf_sstable.cc
    perf_tablets.cc
    tablet_load_balancing.cc
    perf.cc)
target_include_directories(test-perf
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(test-perf
  PRIVATE
    idl
    test-lib
    Seastar::seastar_perf_testing
    JsonCpp::JsonCpp
    xxHash::xxhash)

# scylla-perf: perf infrastructure linked into the scylla binary.
# Mirrors configure.py's scylla_perfs list — includes only the specific
# test/lib sources that perf code needs, NOT the full test-lib (which would
# pull in 16 unnecessary test-only files like boost_test_tree_lister,
# mutation_assertions, KMS fixtures, etc.).
add_library(scylla-perf STATIC)
target_sources(scylla-perf
  PRIVATE
    perf_alternator.cc
    perf_fast_forward.cc
    perf_row_cache_update.cc
    perf_simple_query.cc
    perf_cql_raw.cc
    perf_sstable.cc
    perf_tablets.cc
    tablet_load_balancing.cc
    perf.cc
    ${CMAKE_SOURCE_DIR}/test/lib/cql_test_env.cc
    ${CMAKE_SOURCE_DIR}/test/lib/log.cc
    ${CMAKE_SOURCE_DIR}/test/lib/test_services.cc
    ${CMAKE_SOURCE_DIR}/test/lib/test_utils.cc
    ${CMAKE_SOURCE_DIR}/test/lib/tmpdir.cc
    ${CMAKE_SOURCE_DIR}/test/lib/key_utils.cc
    ${CMAKE_SOURCE_DIR}/test/lib/random_schema.cc
    ${CMAKE_SOURCE_DIR}/test/lib/data_model.cc
    ${CMAKE_SOURCE_DIR}/test/lib/eventually.cc
    ${CMAKE_SOURCE_DIR}/seastar/tests/perf/linux_perf_event.cc)
target_include_directories(scylla-perf
  PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}/gen)
target_link_libraries(scylla-perf
  PRIVATE
    scylla-main
    idl
    absl::headers
    Seastar::seastar_testing
    JsonCpp::JsonCpp
    xxHash::xxhash)

add_library(perf-lib OBJECT perf.cc)
target_include_directories(perf-lib
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(perf-lib
  PUBLIC
    Seastar::seastar
  PRIVATE
    xxHash::xxhash)

add_perf_test(logalloc)
add_perf_test(memory_footprint_test)
add_perf_test(perf_big_decimal
  LIBRARIES
    mutation
    schema)
add_perf_test(perf_cache_eviction)
add_perf_test(perf_checksum)
add_perf_test(perf_commitlog
  LIBRARIES
    JsonCpp::JsonCpp)
add_perf_test(perf_collection)
add_perf_test(perf_cql_parser
  LIBRARIES
    cql3)
add_perf_test(perf_hash)
add_perf_test(perf_idl
  LIBRARIES
    idl)
add_perf_test(perf_mutation)
add_perf_test(perf_mutation_readers
  LIBRARIES
    auth
    cql3
    dht
    mutation
    readers
    replica
    schema
    service
    types
    utils)
add_perf_test(perf_mutation_fragment)
add_perf_test(perf_vint)
add_perf_test(perf_row_cache_reads)
add_perf_test(perf_generic_server)
add_perf_test(perf_s3_client
  LIBRARIES
    JsonCpp::JsonCpp)
add_perf_test(perf_sort_by_proximity)
add_perf_test(perf_bti_key_translation
  LIBRARIES
    dht
    sstables)
