/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sstables_tablet_aware_loader.hh"
#include "db/system_distributed_keyspace.hh"
#include "replica/database.hh"
#include "replica/global_table_ptr.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/units.hh>

future<minimal_sst_info> sstables_tablet_aware_loader::download_sstable(const sstables::shared_sstable& sstable) {
    // auto sstable = sstables::make
    // shared_sstable sst = _manager.make_sstable(_schema, storage_opts, desc.generation, _state, desc.version, desc.format, db_clock::now(),
    // _error_handler_gen); data_dictionary::storage_options storage_opts; make_lw_shared<sstable>(std::move(schema), storage_opts, desc.generation,
    // state,desc.version, desc.format, get_large_data_handler(), get_corrupt_data_handler(), *this, now, std::move(error_handler_gen), buffer_size);

    auto& db = _db.local();
    auto& table = db.find_column_family(_keyspace, _table);
    auto& sst_manager = table.get_sstables_manager();
    auto sst = sst_manager.make_sstable(
        table.schema(), table.get_storage_options(), min_info._generation, sstables::sstable_state::normal, min_info._version, min_info._format);
    auto components = sstable->all_components();

    // Move the TOC to the front to be processed first since `sstables::create_stream_sink` takes care
    // of creating behind the scene TemporaryTOC instead of usual one. This assures that in case of failure
    // this partially created SSTable will be cleaned up properly at some point.
    auto toc_it = std::ranges::find_if(components, [](const auto& component) { return component.first == component_type::TOC; });
    if (toc_it != components.begin()) {
        swap(*toc_it, components.front());
    }

    // Ensure the Scylla component is processed second.
    //
    // The sstable_sink->output() call for each component may invoke load_metadata()
    // and save_metadata(), but these functions only operate correctly if the Scylla
    // component file already exists on disk. If the Scylla component is written first,
    // load_metadata()/save_metadata() become no-ops, leaving the original Scylla
    // component (with outdated metadata) untouched.
    //
    // By placing the Scylla component second, we guarantee that:
    //   1) The first component (TOC) is written and the Scylla component file already
    //      exists on disk when subsequent output() calls happen.
    //   2) Later output() calls will overwrite the Scylla component with the correct,
    //      updated metadata.
    //
    // In short: Scylla must be written second so that all following output() calls
    // can properly update its metadata instead of silently skipping it.
    auto scylla_it = std::ranges::find_if(components, [](const auto& component) { return component.first == component_type::Scylla; });
    if (scylla_it != std::next(components.begin())) {
        swap(*scylla_it, *std::next(components.begin()));
    }

    auto gen = table.get_sstable_generation_generator()();
    auto files = co_await sstable->readable_file_for_all_components();
    for (auto it = components.cbegin(); it != components.cend(); ++it) {
        try {
            auto descriptor = sstable->get_descriptor(it->first);
            auto sstable_sink =
                sstables::create_stream_sink(table.schema(),
                                             table.get_sstables_manager(),
                                             table.get_storage_options(),
                                             sstables::sstable_state::normal,
                                             sstables::sstable::component_basename(
                                                 table.schema()->ks_name(), table.schema()->cf_name(), descriptor.version, gen, descriptor.format, it->first),
                                             sstables::sstable_stream_sink_cfg{.last_component = std::next(it) == components.cend()});
            auto out = co_await sstable_sink->output(foptions, stream_options);

            input_stream src(co_await [this, &it, sstable, f = files.at(it->first), &table]() -> future<input_stream<char>> {
                const auto fis_options = file_input_stream_options{.buffer_size = 128_KiB, .read_ahead = 2};

                if (it->first != sstables::component_type::Data) {
                    co_return input_stream<char>(
                        co_await sstable->get_storage().make_source(*sstable, it->first, f, 0, std::numeric_limits<size_t>::max(), fis_options));
                }
                auto permit = co_await _db.local().obtain_reader_permit(table, "download_fully_contained_sstables", db::no_timeout, {});
                co_return co_await (
                    sstable->get_compression()
                        ? sstable->data_stream(0, sstable->ondisk_data_size(), std::move(permit), nullptr, nullptr, sstables::sstable::raw_stream::yes)
                        : sstable->data_stream(0, sstable->data_size(), std::move(permit), nullptr, nullptr, sstables::sstable::raw_stream::no));
            }());

            std::exception_ptr eptr;
            try {
                co_await seastar::copy(src, out);
            } catch (...) {
                eptr = std::current_exception();
                // llog.info("Error downloading SSTable component {}. Reason: {}", it->first, eptr);
            }
            co_await src.close();
            co_await out.close();
            if (eptr) {
                co_await sstable_sink->abort();
                std::rethrow_exception(eptr);
            }
            if (auto sst = co_await sstable_sink->close()) {
                const auto& shards = sstable->get_shards_for_this_sstable();
                if (shards.size() != 1) {
                    on_internal_error(llog, "Fully-contained sstable must belong to one shard only");
                }
                // llog.debug("SSTable shards {}", fmt::join(shards, ", "));
                co_return {shards.front(), gen, descriptor.version, descriptor.format};
            }
        } catch (...) {
            // llog.info("Error downloading SSTable component {}. Reason: {}", it->first, std::current_exception());
            throw;
        }
    }
}

future<utils::chunked_vector<db::sstable_info>> sstables_tablet_aware_loader::get_owned_sstables(utils::chunked_vector<db::sstable_info> sst_infos) {
    const auto global_table = replica::get_table_on_all_shards(_db, _keyspace, _table).get();
    const auto table_id = global_table->schema()->id();
    const auto& tablet_map = _erm->get_token_metadata().tablets().get_tablet_map(table_id);
    auto tablet_in_node_scope = [&tablet_map, this](locator::tablet_id tid) {
        const auto& topo = _erm->get_topology();
        return std::ranges::any_of(tablet_map.get_tablet_info(tid).replicas, [&topo](const auto& r) { return topo.is_me(r.host); });
    };
    auto tablets_in_scope = tablet_map.tablet_ids() | std::views::filter([&](auto tid) { return tablet_in_node_scope(tid); }) |
                            std::views::transform([&tablet_map](auto tid) { return tablet_map.get_token_range(tid); });

    utils::chunked_vector<db::sstable_info> ret_val;
    for (const auto& tablet_range : tablets_in_scope) {
        for (const auto& sst : sst_infos) {

            // SSTable entirely after tablet -> no further SSTables (larger keys) can overlap
            if (tablet_range.after(sst.first_token, dht::token_comparator{})) {
                break;
            }
            // SSTable entirely before tablet -> skip and continue scanning later (larger keys)
            if (tablet_range.before(sst.last_token, dht::token_comparator{})) {
                continue;
            }

            if (tablet_range.contains(dht::token_range{sst.first_token, sst.last_token}, dht::token_comparator{})) {
                ret_val.push_back(sst);
            } else {
                throw std::logic_error("sstables_partially_contained");
            }
            co_await coroutine::maybe_yield();
        }
    }
    co_return std::move(ret_val);
}

future<std::vector<sstables::shared_sstable>> sstables_tablet_aware_loader::get_snapshot_sstables() {
    auto sst_infos = co_await _sys_dist_ks.local().get_snapshot_sstables(_snapshot, _keyspace, _table, _data_center, _rack);
    auto owned_sstables = co_await get_owned_sstables(sst_infos);
    std::vector<sstables::shared_sstable> ret_val;
    seastar::parallel_for_each(owned_sstables, [this, &ret_val](const auto& sstable) { ret_val.emplace_back(co_await download_sstable(sstable)); });
    co_return ret_val;
}

sstables_tablet_aware_loader::sstables_tablet_aware_loader(
    const std::string& snapshot, const std::string& data_center, const std::string& rack, const std::string& ks_name, const std::string& cf_name)
    : _snapshot(snapshot), _data_center(data_center), _rack(rack), _keyspace(ks_name), _table(cf_name) {
}


seastar::future<> sstables_tablet_aware_loader::load_snapshot_sstables() {
    auto sstables = co_await get_snapshot_sstables();
    auto downloaded_sstables = co_await download_new_sstables(sstables);
    for (auto shard_id : std::ranges::iota_view(0, seastar::smp::count)) {
        co_await container().invoke_on(shard_id,
                                       [shard_sstables = std::move(downloaded_sstables[shard_id])]() { return loader.load_sstables(downloaded_sstables); });
    }
}
