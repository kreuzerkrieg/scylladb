message(STATUS "KABOOM!!!")
get_property(all_targets DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY BUILDSYSTEM_TARGETS)

foreach(tgt IN LISTS all_targets)
    message(STATUS "Target: ${tgt}")
    get_target_property(srcs ${tgt} SOURCES)
    if(srcs)
        foreach(src IN LISTS srcs)
            message(STATUS "  Source: ${src}")
        endforeach()
    else()
        message(STATUS "  No sources found.")
    endif()
endforeach()


set_source_files_properties(wasm.cc TARGET_DIRECTORY lang PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
set_source_files_properties(wasm_instance_cache.cc TARGET_DIRECTORY lang PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
set_source_files_properties(abseil/absl/time/time.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
set_source_files_properties(abseil/absl/time/format.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
set_source_files_properties(abseil/absl/time/internal/cctz/src/time_zone_posix.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
set_source_files_properties(abseil/absl/time/internal/cctz/src/time_zone_fixed.cc PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)

set_target_properties(absl_time PROPERTIES UNITY_BUILD OFF)
set_target_properties(absl_time_zone PROPERTIES UNITY_BUILD OFF)

