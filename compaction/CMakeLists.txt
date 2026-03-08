add_library(compaction STATIC)
target_sources(compaction
  PRIVATE
    compaction.cc
    compaction_manager.cc
    compaction_strategy.cc
    incremental_backlog_tracker.cc
    incremental_compaction_strategy.cc
    leveled_compaction_strategy.cc
    size_tiered_compaction_strategy.cc
    task_manager_module.cc
    time_window_compaction_strategy.cc)
# compaction_manager.cc: compaction::compaction_state resolves incorrectly because
# 'compaction' matches both the namespace and the compaction class/function.
# compaction.cc defines 'class compaction' inside namespace compaction, causing the
# same compaction::X qualified lookup issue for other files in the unity batch.
# incremental_compaction_strategy.cc: redefines static validate_bucket_low/high()
# also defined in size_tiered_compaction_strategy.cc.
set_source_files_properties(compaction_manager.cc compaction.cc
    incremental_compaction_strategy.cc
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
target_include_directories(compaction
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(compaction
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    mutation_writer
    replica)

if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(compaction REUSE_FROM scylla-precompiled-header)
endif()
check_headers(check-headers compaction
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
