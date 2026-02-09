/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "tablet_aware_download_task.hh"

#include "sstables_tablet_aware_loader.hh"
#include "tablet_aware_loader.hh"

future<> tablet_aware_download_task_impl::run() {
    _as.check();
    named_gate g("tablet_aware_download_task_impl");
    auto s = _as.subscribe([&]() noexcept {
        try {
            auto h = g.hold();
            // Can safely ignore the future since we hold the gate.
            std::ignore = smp::invoke_on_all([this, ex = _as.abort_requested_exception_ptr()] {
                              _as_per_shard[this_shard_id()].request_abort_ex(ex);
                          }).finally([h = std::move(h)] {});
        } catch (...) {
        }
    });
    sstables_tablet_aware_loader loader(_tablet_aware_loader, _snapshot, _data_center, _rack, _keyspace, _table, _endpoint, _bucket, _progress_per_shard, _as_per_shard);
    co_await loader.load_snapshot_sstables();
    co_await g.close();
    co_return;
}
tablet_aware_download_task_impl::tablet_aware_download_task_impl(tasks::task_manager::module_ptr module,
                                                                 tablet_aware_loader& tablet_aware_loader,
                                                                 std::string snapshot,
                                                                 std::string data_center,
                                                                 std::string rack,
                                                                 std::string keyspace,
                                                                 std::string table,
                                                                 std::string endpoint,
                                                                 std::string bucket) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", "", "", "", tasks::task_id::create_null_id())
    , _snapshot(std::move(snapshot))
    , _data_center(std::move(data_center))
    , _rack(std::move(rack))
    , _keyspace(std::move(keyspace))
    , _table(std::move(table))
    , _endpoint(std::move(endpoint))
    , _bucket(std::move(bucket))
    , _tablet_aware_loader(tablet_aware_loader) {
    _status.progress_units = "batches";
}

future<tasks::task_manager::task::progress> tablet_aware_download_task_impl::get_progress() const {
    co_return std::accumulate(_progress_per_shard.cbegin(),
                              _progress_per_shard.cend(),
                              tasks::task_manager::task::progress{},
                              [](auto init, const tasks::task_manager::task::progress& p) {
                                  init.total += p.total;
                                  init.completed += p.completed;
                                  return init;
                              });
}
