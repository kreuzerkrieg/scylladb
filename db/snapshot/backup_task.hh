/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include "utils/s3/client_fwd.hh"
#include "tasks/task_manager.hh"

namespace s3{struct endpoint_config;}
namespace db {
class snapshot_ctl;

namespace snapshot {

using client_config = std::function<std::tuple<sstring, semaphore&, s3::endpoint_config>()>;
class backup_task_impl : public tasks::task_manager::task::impl {
    snapshot_ctl& _snap_ctl;
    client_config _cfg;
    sstring _bucket;
    sstring _prefix;
    std::filesystem::path _snapshot_dir;
    std::vector<s3::upload_progress> _progress_per_shard{smp::count};
    bool _remove_on_uploaded;

    future<> do_backup();
    future<> upload_component(shared_ptr<s3::client>& client, abort_source& as, s3::upload_progress& progress, sstring name);

protected:
    virtual future<> run() override;

public:
    backup_task_impl(tasks::task_manager::module_ptr module,
                     snapshot_ctl& ctl,
                     client_config cfg,
                     sstring bucket,
                     sstring prefix,
                     sstring ks,
                     std::filesystem::path snapshot_dir,
                     bool move_files) noexcept;

    virtual std::string type() const override;
    virtual tasks::is_internal is_internal() const noexcept override;
    virtual tasks::is_abortable is_abortable() const noexcept override;
    virtual future<tasks::task_manager::task::progress> get_progress() const override;
    virtual tasks::is_user_task is_user_task() const noexcept override;
};

} // snapshot namespace
} // db namespace
