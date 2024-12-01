/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "utils/lister.hh"
#include "utils/s3/client.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "db/snapshot-ctl.hh"
#include "db/snapshot/backup_task.hh"
#include "schema/schema_fwd.hh"
#include "sstables/sstables.hh"
#include "utils/error_injection.hh"

extern logging::logger snap_log;

namespace db::snapshot {

backup_task_impl::backup_task_impl(tasks::task_manager::module_ptr module,
                                   snapshot_ctl& ctl,
                                   client_config cfg,
                                   sstring bucket,
                                   sstring prefix,
                                   sstring ks,
                                   std::filesystem::path snapshot_dir,
                                   bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _cfg(std::move(cfg))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir))
    , _remove_on_uploaded(move_files) {
    _status.progress_units = "bytes ('total' may grow along the way)";
}

std::string backup_task_impl::type() const {
    return "backup";
}

tasks::is_internal backup_task_impl::is_internal() const noexcept {
    return tasks::is_internal::no;
}

tasks::is_abortable backup_task_impl::is_abortable() const noexcept {
    return tasks::is_abortable::yes;
}

future<tasks::task_manager::task::progress> backup_task_impl::get_progress() const {
    struct adder {
        tasks::task_manager::task::progress result;
        future<> operator()(s3::upload_progress p) {
            result.completed += p.uploaded;
            result.total += p.total;
            return make_ready_future<>();
        }
        tasks::task_manager::task::progress get() const { return result; }
    };
    auto p = co_await _snap_ctl.container().map_reduce(adder{}, [this](auto&) -> s3::upload_progress {
        return _progress_per_shard[this_shard_id()];
    });
    co_return tasks::task_manager::task::progress{
        .completed = p.completed,
        .total = p.total,
    };
}

tasks::is_user_task backup_task_impl::is_user_task() const noexcept {
    return tasks::is_user_task::yes;
}

future<> backup_task_impl::upload_component(shared_ptr<s3::client>& client, abort_source& as, s3::upload_progress& progress, sstring name) {
    auto component_name = _snapshot_dir / name;
    auto destination = fmt::format("/{}/{}/{}", _bucket, _prefix, name);
    snap_log.trace("Upload {} to {}", component_name.native(), destination);

    // Start uploading in the background. The caller waits for these fibers
    // with the uploads gate.
    // Parallelism is implicitly controlled in two ways:
    //  - s3::client::claim_memory semaphore
    //  - http::client::max_connections limitation
    try {
        co_await client->upload_file(component_name, destination, progress, &as);
    } catch (...) {
        snap_log.error("Error uploading {}: {}", component_name.native(), std::current_exception());
        throw;
    }

    if (!_remove_on_uploaded) {
        co_return;
    }

    // Delete the uploaded component to:
    // 1. Free up disk space immediately
    // 2. Avoid costly S3 existence checks on future backup attempts
    try {
        co_await remove_file(component_name.native());
    } catch (...) {
        // If deletion of an uploaded file fails, the backup process will continue.
        // While this doesn't halt the backup, it may indicate filesystem permissions
        // issues or system constraints that should be investigated.
        snap_log.warn("Failed to remove {}: {}", component_name, std::current_exception());
    }
}

namespace {
[[maybe_unused]] std::vector<std::tuple<size_t, std::vector<sstring>>> greedy_partitioning(std::vector<std::tuple<size_t, sstring>>&& files,
                                                                                           size_t partitions) {
    std::vector<std::tuple<size_t, std::vector<sstring>>> result(partitions);
    std::ranges::sort(files, [](const auto& lhs, const auto& rhs) { return std::get<0>(lhs) > std::get<0>(rhs); });
    for (auto&& [size, file] : files) {
        auto& smallest = *std::ranges::min_element(result, [](const auto& lhs, const auto& rhs) { return std::get<0>(lhs) < std::get<0>(rhs); });
        std::get<1>(smallest).emplace_back(std::move(file));
        std::get<0>(smallest) += size;
    }
    return result;
}

[[maybe_unused]] std::vector<std::tuple<size_t, std::vector<sstring>>> round_robin_partitioning(std::vector<std::tuple<size_t, sstring>>&& files,
                                                                                                size_t partitions) {
    std::vector<std::tuple<size_t, std::vector<sstring>>> result(partitions);
    std::ranges::sort(files, [](const auto& lhs, const auto& rhs) { return std::get<0>(lhs) > std::get<0>(rhs); });
    size_t pos = 0;
    for (auto&& [size, file] : files) {
        auto& elem = result[pos++ % partitions];
        std::get<1>(elem).emplace_back(std::move(file));
        std::get<0>(elem) += size;
    }
    return result;
}

sstring print_results(const std::vector<std::tuple<size_t, std::vector<sstring>>>& files) {
    sstring sizes_table;
    sizes_table += "\n+-------+----------------------------------------------+\n";
    sizes_table += "|  Shard  |    Accumulated size    |  Number of files  |\n";
    sizes_table += "+---------+--------------------------------------------+\n";
    for (size_t i = 0; i < files.size(); ++i) {
        sizes_table += std::format("|{:^9}|{:>17} bytes |{:>19}|\n", i, std::get<0>(files[i]), std::get<1>(files[i]).size());
    }
    sizes_table += "+---------+------------------------+-------------------+\n";
    return sizes_table;
}

future<std::vector<std::tuple<size_t, std::vector<sstring>>>> get_backup_files(const std::filesystem::path& snapshot_dir) {
    std::exception_ptr ex;
    auto snapshot_dir_lister = directory_lister(snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
    std::vector<std::tuple<size_t, sstring>> backup_files;
    std::optional<directory_entry> component_ent;

    do {
        try {
            component_ent = co_await snapshot_dir_lister.get();
            if (component_ent) {
                auto component_name = snapshot_dir / component_ent->name;
                auto size = co_await file_size(component_name.native());
                backup_files.emplace_back(size, std::move(component_ent->name));
            }
        } catch (...) {
            ex = std::current_exception();
            break;
        }
    } while (component_ent);
    co_await snapshot_dir_lister.close();
    if (ex)
        co_await coroutine::return_exception_ptr(std::move(ex));

    co_return greedy_partitioning(std::move(backup_files), smp::count);
}
} // namespace

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    auto backup_shards = co_await get_backup_files(_snapshot_dir);
    // TODO: Set the print to trace level
    if (snap_log.is_enabled(log_level::info))
        snap_log.info("{}", print_results(backup_shards));

    std::vector<abort_source> as_per_shard(smp::count);
    auto s = _as.subscribe([&]() noexcept {
            try {
                std::ignore=smp::invoke_on_all([&as_per_shard, ex = _as.abort_requested_exception_ptr()] {
                    as_per_shard[this_shard_id()].request_abort_ex(ex);
                });
            } catch (...) {
            }
        });

    auto [_1, memory, _2] = _cfg();
    auto shard_mem = memory.current() / smp::count;

    co_await smp::invoke_on_all([this, &backup_shards, &shard_mem, &as_per_shard] -> future<> {
        const auto& shard_files = std::get<1>(backup_shards[this_shard_id()]);
        auto shard_semaphore = semaphore(shard_mem);
        auto& shard_as = as_per_shard[this_shard_id()];
        auto& shard_progress = _progress_per_shard[this_shard_id()];

        auto [endpoint, _1, cfg] = _cfg();
        auto cln = s3::client::make(endpoint, make_lw_shared<s3::endpoint_config>(std::move(cfg)), shard_semaphore);

        gate uploads;

        for (const auto& name : shard_files) {
            // Pre-upload break point. For testing abort in actual s3 client usage.
            co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

            // Start uploading in the background. The caller waits for these fibers
            // with the uploads gate.
            // Parallelism is implicitly controlled in two ways:
            //  - s3::client::claim_memory semaphore
            //  - http::client::max_connections limitation
            auto gh = uploads.hold();

            std::ignore = upload_component(cln, shard_as, shard_progress, name).finally([gh = std::move(gh)] {});
            co_await coroutine::maybe_yield();
            co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
            if (shard_as.abort_requested()) {
                co_await coroutine::return_exception_ptr(shard_as.abort_requested_exception_ptr());
            }
        }
        co_await uploads.close();
        co_await cln->close();
    });
}

future<> backup_task_impl::run() {
    // do_backup() removes a file once it is fully uploaded, so we are actually
    // mutating snapshots.
    co_await _snap_ctl.run_snapshot_modify_operation([this] {
        return do_backup();
    });
    snap_log.info("Finished backup");
}

} // db::snapshot namespace
