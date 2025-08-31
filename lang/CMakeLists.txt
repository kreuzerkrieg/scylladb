add_library(lang STATIC)
target_sources(lang
  PRIVATE
    manager.cc
    lua.cc
    wasm.cc
    wasm_alien_thread_runner.cc
    wasm_instance_cache.cc)
target_include_directories(lang
  PUBLIC
    ${CMAKE_SOURCE_DIR}
  PRIVATE
    ${LUA_INCLUDE_DIR})
target_link_libraries(lang
  PUBLIC
    wasmtime_bindings
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    absl::headers
    ${LUA_LIBRARIES}
    scylla-precompiled-header)

check_headers(check-headers lang
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)

set_source_files_properties(wasm.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
set_source_files_properties(wasm_instance_cache.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
#
#set_target_properties()
#get_property(all_targets DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY BUILDSYSTEM_TARGETS)
#
#foreach(tgt IN LISTS all_targets)
#    message(STATUS "Target: ${tgt}")
#    get_target_property(srcs ${tgt} SOURCES)
#    if(srcs)
#        foreach(src IN LISTS srcs)
#            message(STATUS "  Source: ${src}")
#        endforeach()
#    else()
#        message(STATUS "  No sources found.")
#    endif()
#endforeach()