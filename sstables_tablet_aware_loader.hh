/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "locator/abstract_replication_strategy.hh"
#include "sstables/generation_type.hh"
#include "sstables/shared_sstable.hh"
#include <seastar/core/sharded.hh>

namespace sstables {
class sstables_manager;
}
namespace db {
struct sstable_info;
class system_distributed_keyspace;
} // namespace db

struct minimal_sst_info {
    shard_id shard;
    sstables::generation_type generation;
    sstables::sstable_version_types version;
    sstables::sstable_format_types format;
};

class sstables_tablet_aware_loader : public seastar::peering_sharded_service<sstables_tablet_aware_loader> {
    std::string _snapshot;
    std::string _data_center;
    std::string _rack;
    std::string _keyspace;
    std::string _table;
    seastar::sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    locator::effective_replication_map_ptr _erm;
    sharded<replica::database>& _db;
    sstables::sstables_manager& _manager;

    seastar::future<minimal_sst_info> download_sstable(const sstables::shared_sstable& sstable);
    future<utils::chunked_vector<db::sstable_info>> get_owned_sstables(utils::chunked_vector<db::sstable_info> sst_infos);
    seastar::future<std::vector<sstables::shared_sstable>> get_snapshot_sstables();

public:
    sstables_tablet_aware_loader(
        const std::string& snapshot, const std::string& data_center, const std::string& rack, const std::string& ks_name, const std::string& cf_name);
    seastar::future<> load_snapshot_sstables();
};
