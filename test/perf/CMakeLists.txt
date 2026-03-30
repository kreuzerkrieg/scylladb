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
      test-perf
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
# Links test-perf and test-lib instead of duplicating their sources.
# The linker only pulls object files that resolve pending symbols from
# static libraries, so test-only objects (boost_test_tree_lister,
# mutation_assertions, etc.) won't be included in the scylla binary.
#
# CAVEAT: because scylla-perf is INTERFACE, test-lib and test-perf
# appear in the scylla binary's link graph.  Any translation unit in
# those libraries that acquires a global constructor or static
# initializer will be silently pulled into the production binary.
# Keep test-lib and test-perf free of such side effects, or move the
# offending TU out of the library.
add_library(scylla-perf INTERFACE)
target_include_directories(scylla-perf
  INTERFACE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}/gen)
target_link_libraries(scylla-perf
  INTERFACE
    test-perf
    test-lib
    scylla-main
    idl
    absl::headers
    Seastar::seastar_testing
    JsonCpp::JsonCpp
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
