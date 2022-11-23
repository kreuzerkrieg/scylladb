/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_set>
#include <unordered_map>
#include <exception>
#include <absl/container/btree_set.h>

#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

#include "locator/abstract_replication_strategy.hh"
#include "replica/database_fwd.hh"
#include "frozen_mutation.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "streaming/stream_reason.hh"
#include "locator/token_metadata.hh"
#include "repair/hash.hh"
#include "repair/sync_boundary.hh"
#include "tasks/types.hh"

namespace tasks {
class repair_module;
}

namespace replica {
class database;
}

class repair_service;
namespace db {
    namespace view {
        class view_update_generator;
    }
    class system_distributed_keyspace;
}
namespace netw { class messaging_service; }
namespace service {
class migration_manager;
}
namespace gms { class gossiper; }

class repair_exception : public std::exception {
private:
    sstring _what;
public:
    repair_exception(sstring msg) : _what(std::move(msg)) { }
    virtual const char* what() const noexcept override { return _what.c_str(); }
};

class repair_stopped_exception : public repair_exception {
public:
    repair_stopped_exception() : repair_exception("Repair stopped") { }
};

struct repair_uniq_id {
    // The integer ID used to identify a repair job. It is currently used by nodetool and http API.
    int id;
    // Task info containing a UUID to identifiy a repair job, and a shard of the job.
    // We will transit to use UUID over the integer ID.
    tasks::task_info task_info;

    tasks::task_id uuid() const noexcept {
        return task_info.id;
    }

    unsigned shard() const noexcept {
        return task_info.shard;
    }
};
std::ostream& operator<<(std::ostream& os, const repair_uniq_id& x);

class node_ops_info {
public:
    utils::UUID ops_uuid;
    shared_ptr<abort_source> as;
    std::list<gms::inet_address> ignore_nodes;

private:
    optimized_optional<abort_source::subscription> _abort_subscription;
    sharded<abort_source> _sas;
    future<> _abort_done = make_ready_future<>();

public:
    node_ops_info(utils::UUID ops_uuid_, shared_ptr<abort_source> as_, std::list<gms::inet_address>&& ignore_nodes_) noexcept;
    node_ops_info(const node_ops_info&) = delete;
    node_ops_info(node_ops_info&&) = delete;

    future<> start();
    future<> stop() noexcept;

    void check_abort();

    abort_source* local_abort_source();
};

// NOTE: repair_start() can be run on any node, but starts a node-global
// operation.
// repair_start() starts the requested repair on this node. It returns an
// integer id which can be used to query the repair's status with
// repair_get_status(). The returned future<int> becomes available quickly,
// as soon as repair_get_status() can be used - it doesn't wait for the
// repair to complete.
future<int> repair_start(seastar::sharded<repair_service>& repair,
        sstring keyspace, std::unordered_map<sstring, sstring> options);

// TODO: Have repair_progress contains a percentage progress estimator
// instead of just "RUNNING".
enum class repair_status { RUNNING, SUCCESSFUL, FAILED };

enum class repair_checksum {
    legacy = 0,
    streamed = 1,
};

class repair_stats {
public:
    uint64_t round_nr = 0;
    uint64_t round_nr_fast_path_already_synced = 0;
    uint64_t round_nr_fast_path_same_combined_hashes= 0;
    uint64_t round_nr_slow_path = 0;

    uint64_t rpc_call_nr = 0;

    uint64_t tx_hashes_nr = 0;
    uint64_t rx_hashes_nr = 0;

    uint64_t tx_row_nr = 0;
    uint64_t rx_row_nr = 0;

    uint64_t tx_row_bytes = 0;
    uint64_t rx_row_bytes = 0;

    std::map<gms::inet_address, uint64_t> row_from_disk_bytes;
    std::map<gms::inet_address, uint64_t> row_from_disk_nr;

    std::map<gms::inet_address, uint64_t> tx_row_nr_peer;
    std::map<gms::inet_address, uint64_t> rx_row_nr_peer;

    lowres_clock::time_point start_time = lowres_clock::now();

public:
    void add(const repair_stats& o);
    sstring get_stats();
};

class repair_neighbors {
public:
    std::vector<gms::inet_address> all;
    std::vector<gms::inet_address> mandatory;
    repair_neighbors() = default;
    explicit repair_neighbors(std::vector<gms::inet_address> a)
        : all(std::move(a)) {
    }
    repair_neighbors(std::vector<gms::inet_address> a, std::vector<gms::inet_address> m)
        : all(std::move(a))
        , mandatory(std::move(m)) {
    }
};

class repair_info {
public:
    repair_service& rs;
    seastar::sharded<replica::database>& db;
    seastar::sharded<netw::messaging_service>& messaging;
    sharded<db::system_distributed_keyspace>& sys_dist_ks;
    sharded<db::view::view_update_generator>& view_update_generator;
    service::migration_manager& mm;
    gms::gossiper& gossiper;
    const dht::sharder& sharder;
    sstring keyspace;
    locator::effective_replication_map_ptr erm;
    dht::token_range_vector ranges;
    std::vector<sstring> cfs;
    std::vector<table_id> table_ids;
    repair_uniq_id id;
    std::vector<sstring> data_centers;
    std::vector<sstring> hosts;
    std::unordered_set<gms::inet_address> ignore_nodes;
    streaming::stream_reason reason;
    std::unordered_map<dht::token_range, repair_neighbors> neighbors;
    size_t total_rf;
    uint64_t nr_ranges_finished = 0;
    uint64_t nr_ranges_total;
    size_t nr_failed_ranges = 0;
    bool aborted = false;
    int ranges_index = 0;
    repair_stats _stats;
    std::unordered_set<sstring> dropped_tables;
    optimized_optional<abort_source::subscription> _abort_subscription;
    bool _hints_batchlog_flushed = false;
public:
    repair_info(repair_service& repair,
            const sstring& keyspace_,
            locator::effective_replication_map_ptr erm_,
            const dht::token_range_vector& ranges_,
            std::vector<table_id> table_ids_,
            repair_uniq_id id_,
            const std::vector<sstring>& data_centers_,
            const std::vector<sstring>& hosts_,
            const std::unordered_set<gms::inet_address>& ingore_nodes_,
            streaming::stream_reason reason_,
            abort_source* as,
            bool hints_batchlog_flushed);
    void check_failed_ranges();
    void abort() noexcept;
    void check_in_abort();
    void check_in_shutdown();
    repair_neighbors get_repair_neighbors(const dht::token_range& range);
    void update_statistics(const repair_stats& stats) {
        _stats.add(stats);
    }
    const std::vector<sstring>& table_names() {
        return cfs;
    }

    bool hints_batchlog_flushed() const {
        return _hints_batchlog_flushed;
    }

    future<> repair_range(const dht::token_range& range, table_id);

    size_t ranges_size();
};

future<uint64_t> estimate_partitions(seastar::sharded<replica::database>& db, const sstring& keyspace,
        const sstring& cf, const dht::token_range& range);


enum class repair_row_level_start_status: uint8_t {
    ok,
    no_such_column_family,
};

struct repair_row_level_start_response {
    repair_row_level_start_status status;
};

// Return value of the REPAIR_GET_SYNC_BOUNDARY RPC verb
struct get_sync_boundary_response {
    std::optional<repair_sync_boundary> boundary;
    repair_hash row_buf_combined_csum;
    // The current size of the row buf
    uint64_t row_buf_size;
    // The number of bytes this verb read from disk
    uint64_t new_rows_size;
    // The number of rows this verb read from disk
    uint64_t new_rows_nr;
};

// Return value of the REPAIR_GET_COMBINED_ROW_HASH RPC verb
using get_combined_row_hash_response = repair_hash;

struct node_repair_meta_id {
    gms::inet_address ip;
    uint32_t repair_meta_id;
    bool operator==(const node_repair_meta_id& x) const {
        return x.ip == ip && x.repair_meta_id == repair_meta_id;
    }
};

// Represent a partition_key and frozen_mutation_fragments within the partition_key.
class partition_key_and_mutation_fragments {
    partition_key _key;
    std::list<frozen_mutation_fragment> _mfs;
public:
    partition_key_and_mutation_fragments()
        : _key(std::vector<bytes>() ) {
    }
    partition_key_and_mutation_fragments(partition_key key, std::list<frozen_mutation_fragment> mfs)
        : _key(std::move(key))
        , _mfs(std::move(mfs)) {
    }
    const partition_key& get_key() const { return _key; }
    const std::list<frozen_mutation_fragment>& get_mutation_fragments() const { return _mfs; }
    partition_key& get_key() { return _key; }
    std::list<frozen_mutation_fragment>& get_mutation_fragments() { return _mfs; }
    void push_mutation_fragment(frozen_mutation_fragment mf) { _mfs.push_back(std::move(mf)); }
};

using repair_row_on_wire = partition_key_and_mutation_fragments;
using repair_rows_on_wire = std::list<partition_key_and_mutation_fragments>;

enum class repair_stream_cmd : uint8_t {
    error,
    hash_data,
    row_data,
    end_of_current_hash_set,
    needs_all_rows,
    end_of_current_rows,
    get_full_row_hashes,
    put_rows_done,
};

struct repair_hash_with_cmd {
    repair_stream_cmd cmd;
    repair_hash hash;
};

struct repair_row_on_wire_with_cmd {
    repair_stream_cmd cmd;
    repair_row_on_wire row;
};

enum class row_level_diff_detect_algorithm : uint8_t {
    send_full_set,
    send_full_set_rpc_stream,
};

std::ostream& operator<<(std::ostream& out, row_level_diff_detect_algorithm algo);

enum class node_ops_cmd : uint32_t {
     removenode_prepare,
     removenode_heartbeat,
     removenode_sync_data,
     removenode_abort,
     removenode_done,
     replace_prepare,
     replace_prepare_mark_alive,
     replace_prepare_pending_ranges,
     replace_heartbeat,
     replace_abort,
     replace_done,
     decommission_prepare,
     decommission_heartbeat,
     decommission_abort,
     decommission_done,
     bootstrap_prepare,
     bootstrap_heartbeat,
     bootstrap_abort,
     bootstrap_done,
     query_pending_ops,
     repair_updater,
};

std::ostream& operator<<(std::ostream& out, node_ops_cmd cmd);

// The cmd and ops_uuid are mandatory for each request.
// The ignore_nodes and leaving_node are optional.
struct node_ops_cmd_request {
    // Mandatory field, set by all cmds
    node_ops_cmd cmd;
    // Mandatory field, set by all cmds
    utils::UUID ops_uuid;
    // Optional field, list nodes to ignore, set by all cmds
    std::list<gms::inet_address> ignore_nodes;
    // Optional field, list leaving nodes, set by decommission and removenode cmd
    std::list<gms::inet_address> leaving_nodes;
    // Optional field, map existing nodes to replacing nodes, set by replace cmd
    std::unordered_map<gms::inet_address, gms::inet_address> replace_nodes;
    // Optional field, map bootstrapping nodes to bootstrap tokens, set by bootstrap cmd
    std::unordered_map<gms::inet_address, std::list<dht::token>> bootstrap_nodes;
    // Optional field, list uuids of tables being repaired, set by repair cmd
    std::list<table_id> repair_tables;
    node_ops_cmd_request(node_ops_cmd command,
            utils::UUID uuid,
            std::list<gms::inet_address> ignore = {},
            std::list<gms::inet_address> leaving = {},
            std::unordered_map<gms::inet_address, gms::inet_address> replace = {},
            std::unordered_map<gms::inet_address, std::list<dht::token>> bootstrap = {},
            std::list<table_id> tables = {})
        : cmd(command)
        , ops_uuid(std::move(uuid))
        , ignore_nodes(std::move(ignore))
        , leaving_nodes(std::move(leaving))
        , replace_nodes(std::move(replace))
        , bootstrap_nodes(std::move(bootstrap))
        , repair_tables(std::move(tables)) {
    }
};

struct node_ops_cmd_response {
    // Mandatory field, set by all cmds
    bool ok;
    // Optional field, set by query_pending_ops cmd
    std::list<utils::UUID> pending_ops;
    node_ops_cmd_response(bool o, std::list<utils::UUID> pending = {})
        : ok(o)
        , pending_ops(std::move(pending)) {
    }
};


struct repair_update_system_table_request {
    tasks::task_id repair_uuid;
    table_id table_uuid;
    sstring keyspace_name;
    sstring table_name;
    dht::token_range range;
    gc_clock::time_point repair_time;
};

struct repair_update_system_table_response {
};

struct repair_flush_hints_batchlog_request {
    tasks::task_id repair_uuid;
    std::list<gms::inet_address> target_nodes;
    std::chrono::seconds hints_timeout;
    std::chrono::seconds batchlog_timeout;
};

struct repair_flush_hints_batchlog_response {
};

namespace std {

template<>
struct hash<repair_hash> {
    size_t operator()(repair_hash h) const { return h.hash; }
};

template<>
struct hash<node_repair_meta_id> {
    size_t operator()(node_repair_meta_id id) const { return utils::tuple_hash()(id.ip, id.repair_meta_id); }
};

}