add_library(repair STATIC)
target_sources(repair
  PRIVATE
    repair.cc
    row_level.cc
    incremental.cc)
# repair.cc includes row_level.hh which has fields only used by row_level.cc;
# combining them in a unity TU triggers -Wunused-private-field.
set_source_files_properties(repair.cc
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
target_include_directories(repair
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(repair
  PUBLIC
    idl
    absl::headers
    Seastar::seastar
    xxHash::xxhash)
if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(repair REUSE_FROM scylla-precompiled-header)
endif()

check_headers(check-headers repair
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
