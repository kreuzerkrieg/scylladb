add_library(sstables STATIC)
target_sources(sstables
  PRIVATE
    compress.cc
    compressor.cc
    checksummed_data_source.cc
    integrity_checked_file_impl.cc
    kl/reader.cc
    metadata_collector.cc
    m_format_read_helpers.cc
    mx/partition_reversing_data_source.cc
    mx/reader.cc
    mx/writer.cc
    object_storage_client.cc
    prepended_input_stream.cc
    random_access_reader.cc
    sstable_directory.cc
    sstable_mutation_reader.cc
    sstables.cc
    sstable_set.cc
    sstables_manager.cc
    sstable_version.cc
    storage.cc
    trie/bti_key_translation.cc
    trie/bti_index_reader.cc
    trie/bti_node_reader.cc
    trie/bti_node_sink.cc
    trie/bti_partition_index_writer.cc
    trie/bti_row_index_writer.cc
    trie/trie_writer.cc
    writer.cc)
# object_storage_client.cc: "using namespace seastar; using namespace sstables;
# using namespace utils;" pollutes scope, causing input_stream and coroutine ambiguity.
# sstables.cc: redefines static check_buf_size() also present in m_format_read_helpers.cc.
set_source_files_properties(object_storage_client.cc sstables.cc
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
target_include_directories(sstables
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(sstables
  PUBLIC
    idl
    wasmtime_bindings
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    readers
    tracing
    absl::headers
    libdeflate::libdeflate
    ZLIB::ZLIB)
if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(sstables REUSE_FROM scylla-precompiled-header)
endif()

check_headers(check-headers sstables
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
