/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Adaptive S3 download stress-tester.
//
// Downloads objects the same way Scylla does in production: via
// make_chunked_download_source() over the full object range, with the stock
// aws::default_aws_retry_strategy retry budget and the stock
// connections-per-shard pool size.  Every knob therefore defaults to the value
// a running Scylla node would use, so a baseline round reflects real behaviour.
//
// Starts with --initial_connections per shard and doubles the connection count
// each round until the S3 server begins pushing back (SlowDown / 429 /
// ECONNRESET).  Each round downloads every listed object once, discarding
// the data.  A per-round timeout (--round_timeout minutes) prevents a round
// from running indefinitely when the file list is very large.
//
// By default, runs on all CPU cores and uses ~93% of available memory (Seastar
// reserves max(1.5GB, 7%) for the OS).  Use -c and -m to override if needed.
//
// Required environment variables:
//   S3_SERVER_ADDRESS_FOR_TEST   – S3-compatible endpoint (host[:port])
//   S3_BUCKET_FOR_TEST           – bucket to list and read from
//   AWS_DEFAULT_REGION           – AWS region string
//   AWS_ACCESS_KEY_ID            – AWS access key
//   AWS_SECRET_ACCESS_KEY        – AWS secret key
//   AWS_SESSION_TOKEN            – AWS session token (for temporary credentials)

#include <algorithm>
#include <array>
#include <optional>
#include <chrono>
#include <filesystem>
#include <random>
#include <unordered_set>
#include <ranges>
#include <string>

#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>

#include "test/lib/test_utils.hh"
#include "utils/estimated_histogram.hh"
#include "utils/exceptions.hh"
#include "utils/lister.hh"
#include "utils/s3/aws_error.hh"
#include "utils/s3/aws_throttling_controller.hh"
#include "utils/s3/client.hh"
#include "utils/s3/default_aws_retry_strategy.hh"
#include "utils/UUID_gen.hh"

using namespace std::chrono_literals;
using namespace std::string_view_literals;

seastar::logger plog("perf");

// ─── counting_retry_strategy ─────────────────────────────────────────────────
//
// Wraps the standard AWS retry strategy, counting the specific error classes
// that indicate the S3 server is actively throttling or dropping connections.

class counting_retry_strategy : public aws::default_aws_retry_strategy {
    // mutable: should_retry() is const in the interface, but we count errors.
    mutable unsigned _slowdown_errors = 0;
    mutable unsigned _network_errors = 0;
    // Requests that ran out of retries while the endpoint was still refusing
    // them, i.e. a failure to ride out throttling. Every object lost in this
    // series was one of these, so it is the number the mitigation has to move.
    // Counted here rather than grepped from the "Retries exhausted" log line,
    // which a run at the wrong log level reports as zero.
    mutable unsigned _throttle_exhaustions = 0;

public:
    // The controller has to be handed in: client::make() injects one only into a
    // strategy it creates itself, so a strategy supplied by a caller would keep the
    // no-op controller and report its throttles nowhere -- measuring the unmitigated
    // behaviour while looking like a test of the mitigated one.
    counting_retry_strategy(unsigned max_retries, s3::throttling_controller& controller)
        : aws::default_aws_retry_strategy(max_retries, controller) {}

    future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;

    unsigned slowdown_errors() const noexcept { return _slowdown_errors; }
    unsigned network_errors() const noexcept { return _network_errors; }
    unsigned throttle_exhaustions() const noexcept { return _throttle_exhaustions; }
    void reset() noexcept { _slowdown_errors = _network_errors = _throttle_exhaustions = 0; }
};

future<bool> counting_retry_strategy::should_retry(std::exception_ptr error, unsigned attempted_retries) const {
    try {
        std::rethrow_exception(error);
    } catch (const aws::aws_exception& e) {
        const auto type = e.error().get_error_type();
        if (type == aws::aws_error_type::SLOW_DOWN || type == aws::aws_error_type::HTTP_TOO_MANY_REQUESTS || type == aws::aws_error_type::SERVICE_UNAVAILABLE ||
            type == aws::aws_error_type::HTTP_SERVICE_UNAVAILABLE) {
            ++_slowdown_errors;
            // The base class refuses at this point, so this is the last word on
            // the request: it died still being throttled.
            if (attempted_retries >= _max_retries) {
                ++_throttle_exhaustions;
            }
        } else if (type == aws::aws_error_type::NETWORK_CONNECTION) {
            ++_network_errors;
        }
    } catch (const std::system_error& e) {
        if (e.code().value() == ECONNRESET) {
            ++_network_errors;
        }
    } catch (...) {
    }

    co_return co_await default_aws_retry_strategy::should_retry(error, attempted_retries);
}

// Attributing a lost object to throttling was previously done by correlating logs
// after the fact. Classify the terminal exception instead. Two forms accounted for
// every loss observed so far (run 11: 55 and 15 of 70):
//
//   storage_io_error  "... Code: 19. Reason: Please reduce your request rate."
//   std::runtime_error "Failed to parse ETag list. Aborting multipart upload."
//
// The second used to be a throttle whose cause upload_part had swallowed. It now
// propagates the real exception, so a parse error reaching here means no part
// recorded a cause at all -- an anomaly worth looking at, not a throttle in
// disguise. It stays a separate bucket so that distinction remains visible.
//
// Matching on message text rather than type because storage_io_error derives from
// std::exception and carries the reason only in its what() string.
enum class failure_kind { throttled, masked, other };

static failure_kind classify_failure(std::exception_ptr e) {
    const auto text = fmt::format("{}", e);
    if (text.find("Failed to parse ETag list") != std::string::npos) {
        return failure_kind::masked;
    }
    if (text.find("reduce your request rate") != std::string::npos) {
        return failure_kind::throttled;
    }
    return failure_kind::other;
}

// ─── round_stats ─────────────────────────────────────────────────────────────

struct round_stats {
    unsigned slowdown_errors = 0;
    unsigned network_errors = 0;
    unsigned download_errors = 0; // retry exhausted — unrecoverable; stop when > 0
    // download_errors split by attributed cause; the remainder is "other"
    unsigned failed_throttled = 0; // terminal error said "reduce your request rate"
    unsigned failed_masked = 0;    // "Failed to parse ETag list" — cause swallowed
    // Throttling recovery. The first is the failure the mitigation exists to
    // prevent; the second says the retry budget, not the endpoint, did the
    // refusing. The third proves the brake was actually in the loop -- a run
    // reporting throttles but no freezes never engaged it.
    unsigned throttle_exhaustions = 0;
    uint64_t retry_quota_denials = 0;
    uint64_t freezes = 0;          // times sending was held back after a throttle
    // GETs actually put on the wire, counted by the HTTP client rather than by
    // this test: chunked_download_source issues one request per chunk plus one
    // per internal retry, none of which are visible from here.
    uint64_t read_requests = 0;
    uint64_t read_bytes = 0;
    uint64_t completed = 0; // whole objects downloaded
    uint64_t sstables = 0;  // whole sstables completed

    round_stats& operator+=(const round_stats& o) noexcept {
        slowdown_errors += o.slowdown_errors;
        network_errors += o.network_errors;
        download_errors += o.download_errors;
        failed_throttled += o.failed_throttled;
        failed_masked += o.failed_masked;
        throttle_exhaustions += o.throttle_exhaustions;
        retry_quota_denials += o.retry_quota_denials;
        freezes += o.freezes;
        read_requests += o.read_requests;
        read_bytes += o.read_bytes;
        completed += o.completed;
        sstables += o.sstables;
        return *this;
    }

    // Rates between two cumulative samples taken dt apart.
    struct rates {
        double requests_per_sec;
        double objects_per_sec;
        double sstables_per_sec;
        double mbytes_per_sec;
    };

    rates since(const round_stats& prev, double dt) const noexcept {
        if (dt <= 0) {
            return {};
        }
        return {
            .requests_per_sec = static_cast<double>(read_requests - prev.read_requests) / dt,
            .objects_per_sec = static_cast<double>(completed - prev.completed) / dt,
            .sstables_per_sec = static_cast<double>(sstables - prev.sstables) / dt,
            .mbytes_per_sec = static_cast<double>((read_bytes - prev.read_bytes) >> 20) / dt,
        };
    }
};

// ─── endpoint ────────────────────────────────────────────────────────────────
//
// Where and how to reach the object store.  Resolved once in main() and passed
// to every shard, so all clients agree on the endpoint.

struct endpoint_params {
    std::string host;
    std::string region;
    unsigned port = 0;
    bool use_https = false;

    // Follows the convention used by test/boost/s3_test.cc: the port comes from
    // the environment and TLS is on whenever a real AWS region is configured,
    // which keeps a plain-HTTP MinIO usable for smoke runs.
    static endpoint_params from_environment() {
        const char* region = ::getenv("AWS_DEFAULT_REGION");
        const char* port = ::getenv("S3_SERVER_PORT_FOR_TEST");
        return {
            .host = tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"),
            .region = region ?: "local",
            .port = static_cast<unsigned>(port ? std::stoul(port) : (region ? 443 : 80)),
            .use_https = region != nullptr,
        };
    }

    s3::endpoint_config_ptr make_config(unsigned connections_per_shard) const {
        s3::endpoint_config cfg;
        cfg.port = port;
        cfg.use_https = use_https;
        cfg.region = region;
        cfg.connections_per_shard = connections_per_shard;
        return make_lw_shared<s3::endpoint_config>(std::move(cfg));
    }
};

// Wires a client the way production does: one controller, owned by the client,
// with the retry strategy referring to that same instance, so retries are paced
// by the same converged rate as fresh requests and the throttles the strategy
// sees reach the rate the client applies.
//
// The client destroys its retry strategy before its controller, so the reference
// held here never dangles.
struct wired_client {
    shared_ptr<s3::client> client;
    counting_retry_strategy* retry;    // observer; the client owns it
    s3::throttling_controller* limiter; // ditto
};

static wired_client make_wired_client(const endpoint_params& ep, unsigned connections, unsigned max_retries) {
    auto controller = std::make_unique<s3::aws_throttling_controller>();
    auto* limiter = controller.get();
    auto rs = std::make_unique<counting_retry_strategy>(max_retries, *limiter);
    auto* retry = rs.get();
    auto client = s3::client::make(ep.host, ep.make_config(connections), std::move(rs), std::move(controller));
    return {.client = std::move(client), .retry = retry, .limiter = limiter};
}

// ─── sampling ────────────────────────────────────────────────────────────────

// Samples are taken by each shard on its own timer and kept locally, then
// collected once the round is over. Polling the shards from outside during the
// round does not work: the cross-shard calls queue behind the download work and
// get starved for the whole active phase, so every sample lands after the round
// has already finished.
struct sample {
    double at_sec; // since round start
    round_stats stats;
};

// Emits a sample as it is taken instead of only contributing to the series that
// is assembled once the round is over. Without this a long round is opaque: the
// throttling counters reach the operator only in the final RESULT line, so a
// fleet cannot be stopped early when S3 starts pushing back -- a 20-minute round
// accumulated ~25k SlowDown retries entirely invisibly before this existed.
//
// Escalates to warn when anything went wrong in the interval, so the line shows
// up even for a run that only enables warn.
static void log_sample(std::string_view verb, double at_sec, const round_stats& now, const round_stats& prev, double prev_at) {
    const auto r = now.since(prev, at_sec - prev_at);
    const auto slowdown = now.slowdown_errors - prev.slowdown_errors;
    const auto netreset = now.network_errors - prev.network_errors;
    const auto failed = now.download_errors - prev.download_errors;
    if (slowdown || netreset || failed) {
        plog.warn("shard {:2d} t={:6.1f}s {}/s={:7.0f} MB/s={:6.0f} slowdown+={} netreset+={} failed+={}",
                  this_shard_id(),
                  at_sec,
                  verb,
                  r.requests_per_sec,
                  r.mbytes_per_sec,
                  slowdown,
                  netreset,
                  failed);
    } else {
        plog.info("shard {:2d} t={:6.1f}s {}/s={:7.0f} MB/s={:6.0f}", this_shard_id(), at_sec, verb, r.requests_per_sec, r.mbytes_per_sec);
    }
}

// Mints ids the same way the sstable code does: sstable_identifier and
// sstable_generation_generator both call UUID_gen::get_time_UUID()
// (sstables/types.hh, sstables/generation_type.hh), so the key distribution here
// matches what a real node writes.
//
// One guard is needed that production does not need. clock_seq_and_node is a
// `static thread_local const` whose dynamic initialiser can run late; read before
// then it is zero-initialised, and get_time_UUID() then returns byte-identical
// UUIDs for every call on that shard. The assert covering this in UUID_gen is
// compiled out in release builds, so it fails silently -- observed here as
// hundreds of sstables colliding on one corpus directory. Detect the degenerate
// result and fall back to a generator that randomises the low half.
static utils::UUID fresh_sstable_id() {
    auto id = utils::UUID_gen::get_time_UUID();
    if (id.get_least_significant_bits() == 0) [[unlikely]] {
        const auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
        id = utils::UUID_gen::get_random_time_UUID_from_micros(now);
    }
    return id;
}

// ─── sstable grouping ────────────────────────────────────────────────────────
//
// Restore fetches whole sstables, not loose objects: per shard it runs
// max_concurrent_for_each over sstables with a fixed concurrency, and within one
// sstable it streams the components one after another, TOC first and Scylla
// second (see download_sstable() in sstables_loader_helpers.cc). The request
// pattern S3 sees depends on that shape, so the test reproduces it rather than
// fetching every object independently.

struct sstable_group {
    sstring id;                      // key prefix shared by every component
    std::vector<sstring> components; // TOC first, Scylla second, then the rest
};

// Component names Scylla writes. An explicit set is needed because the two
// layouts this test has to read cannot be told apart by shape:
//
//   backup:         <prefix>/<host_id>/me-<gen>-big-Data.db   components share a dir
//   object storage: <prefix>/<sstable_id>/Data.db             one dir per sstable
//
// In the first the component is the trailing '-' field and the sstable identity is
// the filename stem; in the second the basename *is* the component and the
// identity is the directory. Deciding on "does the basename contain a dash" gets
// the second case wrong -- every key looks like a non-component and the listing
// groups to zero sstables -- and it also has to special-case manifest.json and
// schema.cql, which sit alongside the sstables.
static bool is_component_name(std::string_view name) {
    static constexpr std::array known = {
        "CRC.db"sv, "CompressionInfo.db"sv, "Data.db"sv, "Digest.crc32"sv, "Filter.db"sv,
        "Index.db"sv, "Scylla.db"sv, "Statistics.db"sv, "Summary.db"sv, "TOC.txt"sv,
    };
    return std::ranges::find(known, name) != known.end();
}

struct key_parts {
    std::string_view sstable;   // identifier shared by every component of one sstable
    std::string_view component; // e.g. "Data.db"
};

// nullopt for anything that is not an sstable component, which is how
// manifest.json and schema.cql are excluded rather than by guessing from shape.
static std::optional<key_parts> split_key(std::string_view key) {
    const auto slash = key.rfind('/');
    const auto base = slash == std::string_view::npos ? key : key.substr(slash + 1);
    if (is_component_name(base)) {
        return key_parts{.sstable = slash == std::string_view::npos ? key : key.substr(0, slash), .component = base};
    }
    const auto dash = base.rfind('-');
    if (dash != std::string_view::npos && is_component_name(base.substr(dash + 1))) {
        return key_parts{.sstable = key.substr(0, key.size() - (base.size() - dash)), .component = base.substr(dash + 1)};
    }
    return std::nullopt;
}

static std::vector<sstable_group> group_by_sstable(const std::vector<sstring>& keys) {
    std::unordered_map<std::string_view, std::vector<sstring>> by_sstable;
    for (const auto& k : keys) {
        if (const auto parts = split_key(k)) {
            by_sstable[parts->sstable].push_back(k);
        }
    }

    std::vector<sstable_group> groups;
    groups.reserve(by_sstable.size());
    for (auto& [id, components] : by_sstable) {
        // Same ordering restore imposes, for the same reason: the TOC has to be
        // first, and the Scylla component second.
        const auto rank = [](const sstring& k) {
            const auto parts = split_key(k);
            const auto c = parts ? parts->component : std::string_view{};
            return c == "TOC.txt" ? 0 : c == "Scylla.db" ? 1 : 2;
        };
        std::stable_sort(components.begin(), components.end(), [&rank](const sstring& a, const sstring& b) { return rank(a) < rank(b); });
        groups.push_back(sstable_group{.id = sstring(id), .components = std::move(components)});
    }
    // Iteration order of an unordered_map is not part of its contract, and the
    // fleet split assigns work by position in this vector, so the order has to
    // come from the keys themselves: every instance must derive the same order
    // from the same listing without coordinating with the others.
    std::ranges::sort(groups, std::less<>{}, &sstable_group::id);
    return groups;
}

// Splits the dataset across a fleet: an instance takes the sstables whose
// position in the globally ordered listing is congruent to its own index. S3
// lists keys lexicographically, so each instance reaches the same assignment
// from the same bucket with no coordinator and no manifest to distribute, and
// the slices are exactly balanced for any fleet size.
//
// Deriving the assignment from the sstable_id instead is a trap. A v1 time UUID
// keeps its only fast-varying field, time_low, in the high half of the MSB: the
// low 48 bits of the LSB are the node id, fixed per producing shard, and the low
// 16 bits of the MSB are version|time_hi, which advances once per ~7.8 hours.
// Measured over a real 5735-sstable corpus, the ids carried 64 distinct LSBs
// that all shared their low 32 bits, so truncate-and-modulus lands every sstable
// on one instance for any fleet size dividing 256 — 16, 32 and 64 included.
// Folding the halves together first, as std::hash<UUID> does, restores the
// entropy but not the independence: PR 30846 derives the object prefix from a
// hash of the same id, so sharing a hash with the prefix layout would correlate
// each instance's key set with the variable under test. Position correlates with
// neither.
static std::vector<sstable_group> select_fleet_slice(std::vector<sstable_group> groups, unsigned fleet_size, unsigned fleet_index) {
    if (fleet_size <= 1) {
        return groups;
    }
    std::vector<sstable_group> mine;
    mine.reserve(groups.size() / fleet_size + 1);
    for (size_t i = fleet_index; i < groups.size(); i += fleet_size) {
        mine.push_back(std::move(groups[i]));
    }
    return mine;
}

// ─── downloader ──────────────────────────────────────────────────────────────
//
// One instance per shard.  Each instance holds its own S3 client.  The file
// list is identical on all shards but shuffled with a per-shard seed so that
// concurrent shards access objects in a different order, spreading the load.

class downloader {
    counting_retry_strategy* _retry = nullptr;     // raw observer; client owns the unique_ptr
    s3::throttling_controller* _limiter = nullptr; // ditto
    shared_ptr<s3::client> _client;
    std::vector<sstable_group> _sstables; // shuffled per shard
    unsigned _connections;                // connection pool size
    unsigned _sstable_concurrency;        // concurrent sstables per shard, as in restore
    bool _run_to_completion;              // walk the list once instead of cycling
    std::chrono::seconds _sample_interval;
    std::filesystem::path _corpus_dir; // empty when persistence is disabled
    uint64_t _total_bytes = 0;
    uint64_t _sstables_done = 0;
    unsigned _download_errors = 0;
    unsigned _download_errors_throttled = 0;
    unsigned _download_errors_masked = 0;
    std::vector<sample> _samples;
    utils::estimated_histogram _latencies;

    std::chrono::steady_clock::time_point _now() const noexcept { return std::chrono::steady_clock::now(); }

    // Components are persisted under a freshly minted sstable_id, mirroring the
    // in-bucket layout <sstable_id>/<component>. The id is minted per sstable per
    // shard, so two shards saving the same source sstable cannot collide, and the
    // corpus is already in the shape the upload phase needs.
    std::filesystem::path corpus_path(const utils::UUID& sid, std::string_view component) const {
        return _corpus_dir / fmt::to_string(sid) / std::string(component);
    }

    static std::filesystem::path partial_path(const std::filesystem::path& final_path) { return final_path.native() + ".partial"; }

    future<std::optional<output_stream<char>>> open_corpus_sink(const std::filesystem::path& path) {
        if (_corpus_dir.empty()) {
            co_return std::nullopt;
        }
        co_await recursive_touch_directory(path.parent_path().native());
        auto f = co_await open_file_dma(partial_path(path).native(), open_flags::wo | open_flags::create | open_flags::truncate);
        co_return co_await make_file_output_stream(std::move(f));
    }

    future<> download_sstable(const sstable_group& sst, abort_source& as) {
        // One id for the whole sstable: every component of it has to land under
        // the same directory, exactly as it would in the bucket.
        const auto sid = fresh_sstable_id();
        for (const auto& component : sst.components) {
            if (as.abort_requested()) {
                co_return;
            }
            co_await download_one(component, sid, as);
        }
        ++_sstables_done;
    }

    future<> download_one(const sstring& file, const utils::UUID& sid, abort_source& as) {
        const auto t0 = _now();
        // Declared outside the try so the failure path can still close it: an
        // output_stream asserts in its destructor if it was never closed, which
        // aborts the whole process rather than failing the one download.
        // sink holds a value only while the stream is still open, so the failure
        // path can tell whether it has to close it; claimed tracks the
        // _corpus_busy entry, which outlives the stream.
        std::optional<output_stream<char>> sink;
        std::exception_ptr failure;
        const auto parts = split_key(file);
        const auto path = _corpus_dir.empty() ? std::filesystem::path{} : corpus_path(sid, parts ? parts->component : std::string_view(file));
        try {
            sink = co_await open_corpus_sink(path);
            // Same source Scylla uses to read a whole object — see
            // object_storage_client::make_download_source().
            auto src = _client->make_chunked_download_source(file, s3::full_range, &as);
            uint64_t sz = 0;
            co_await with_closeable(input_stream<char>(std::move(src)), [&sz, &sink](input_stream<char>& in) -> future<> {
                co_await in.consume([&sz, &sink](auto buf) -> future<consumption_result<char>> {
                    if (buf.empty()) {
                        co_return stop_consuming(std::move(buf));
                    }
                    sz += buf.size();
                    if (sink) {
                        co_await sink->write(buf.get(), buf.size());
                    }
                    co_return continue_consuming();
                });
            });
            if (sink) {
                co_await sink->close();
                sink.reset(); // closed; the failure path must not close it again
                co_await rename_file(partial_path(path).native(), path.native());
            }
            _total_bytes += sz;
            _latencies.add(std::chrono::duration_cast<std::chrono::milliseconds>(_now() - t0).count());
        } catch (...) {
            failure = std::current_exception();
        }
        if (!failure) {
            co_return;
        }
        // Cleanup lives outside the handler: co_await is not allowed inside one.
        if (sink) {
            // Close before the destructor gets a chance to assert, and drop the
            // partial file so the object is retried rather than being mistaken
            // for a complete one on a later pass.
            try {
                co_await sink->close();
            } catch (...) {
            }
            try {
                co_await remove_file(partial_path(path).native());
            } catch (...) {
            }
        }
        // The round timeout aborts in-flight downloads on purpose; that is the
        // end of the round, not a failure of the endpoint.
        if (as.abort_requested()) {
            co_return;
        }
        plog.info("shard {}: error downloading {}: {}", this_shard_id(), file, failure);
        ++_download_errors;
        switch (classify_failure(failure)) {
        case failure_kind::throttled:
            ++_download_errors_throttled;
            break;
        case failure_kind::masked:
            ++_download_errors_masked;
            break;
        case failure_kind::other:
            break;
        }
    }

public:
    downloader(endpoint_params ep,
               unsigned connections,
               unsigned sstable_concurrency,
               unsigned max_retries,
               std::chrono::seconds sample_interval,
               std::vector<sstable_group> sstables,
               std::filesystem::path corpus_dir,
               bool run_to_completion)
        : _sstables(std::move(sstables))
        , _connections(connections)
        , _sstable_concurrency(sstable_concurrency)
        , _run_to_completion(run_to_completion)
        , _sample_interval(sample_interval)
        , _corpus_dir(std::move(corpus_dir)) {
        if (_run_to_completion) {
            // One pass over the data: shards take disjoint slices so each sstable
            // is fetched once and the corpus holds one copy, rather than every
            // shard walking the whole list.
            std::vector<sstable_group> mine;
            for (size_t i = this_shard_id(); i < _sstables.size(); i += this_smp_shard_count()) {
                mine.push_back(std::move(_sstables[i]));
            }
            _sstables = std::move(mine);
        } else {
            // Load generation: every shard cycles the whole list, in its own order
            // so concurrent shards do not hammer the same sstables together.
            std::shuffle(_sstables.begin(), _sstables.end(), std::default_random_engine(this_shard_id()));
        }

        auto wired = make_wired_client(ep, _connections, max_retries);
        _retry = wired.retry;
        _limiter = wired.limiter;
        _client = std::move(wired.client);
    }

    future<> run(std::chrono::minutes round_timeout) {
        _total_bytes = 0;
        _download_errors = 0;
        _download_errors_throttled = 0;
        _download_errors_masked = 0;
        _latencies = utils::estimated_histogram{};
        _samples.clear();
        _sstables_done = 0;
        _retry->reset();

        abort_source as;
        const auto round_start = _now();
        timer<lowres_clock> sample_timer;
        sample_timer.set_callback([this, round_start] {
            const auto at_sec = std::chrono::duration_cast<std::chrono::duration<double>>(_now() - round_start).count();
            auto stats = collect_stats();
            const round_stats prev = _samples.empty() ? round_stats{} : _samples.back().stats;
            const double prev_at = _samples.empty() ? 0.0 : _samples.back().at_sec;
            _samples.push_back(sample{.at_sec = at_sec, .stats = stats});
            log_sample("GET", at_sec, stats, prev, prev_at);
        });
        sample_timer.arm_periodic(_sample_interval);
        timer<lowres_clock> timeout_timer;
        timeout_timer.set_callback([&as] {
            if (!as.abort_requested()) {
                plog.info("shard {}: round timeout reached, stopping downloads", this_shard_id());
                as.request_abort();
            }
        });
        if (!_run_to_completion) {
            timeout_timer.arm(round_timeout);
        }

        try {
            // One worker per concurrently-restored sstable, matching the
            // max_concurrent_for_each concurrency restore uses per shard. The
            // worker set is what is bounded, not the input range: cycling an
            // unbounded range through max_concurrent_for_each becomes a busy loop
            // the moment the abort makes every item return immediately.
            // With a round timeout the workers cycle the list to keep load up for
            // the whole round. Without one the point is to get through the data
            // once, so each sstable is taken exactly once and the run ends when
            // the list is exhausted.
            size_t next = 0;
            const bool once = _run_to_completion;
            co_await coroutine::parallel_for_each(std::views::iota(0u, _sstable_concurrency), [this, &as, &next, once](unsigned) -> future<> {
                while (!as.abort_requested()) {
                    const size_t i = next++;
                    if (once && i >= _sstables.size()) {
                        co_return;
                    }
                    co_await download_sstable(_sstables[once ? i : i % _sstables.size()], as);
                }
            });
        } catch (const abort_requested_exception&) {
            // parallel_for_each itself was interrupted by the timeout
        }

        timeout_timer.cancel();
        sample_timer.cancel();
    }

    // Called by sharded<downloader>::stop().
    future<> stop() { co_await _client->close(); }

    round_stats collect_stats() const noexcept {
        const auto counters = _client->get_request_counters();
        return {
            .slowdown_errors = _retry ? _retry->slowdown_errors() : 0u,
            .network_errors = _retry ? _retry->network_errors() : 0u,
            .download_errors = _download_errors,
            .failed_throttled = _download_errors_throttled,
            .failed_masked = _download_errors_masked,
            .throttle_exhaustions = _retry ? _retry->throttle_exhaustions() : 0u,
            .retry_quota_denials = _limiter ? _limiter->retry_quota_denials() : 0u,
            .freezes = _limiter ? _limiter->freezes() : 0u,
            .read_requests = counters.read_ops + counters.read_retries,
            .read_bytes = counters.read_bytes,
            .completed = _latencies._count,
            .sstables = _sstables_done,
        };
    }

    std::vector<sample> collect_samples() const { return _samples; }

    future<> log_stats(double elapsed_sec) const {
        if (_latencies._count == 0) {
            plog.info("  shard {:2d}: no files completed", this_shard_id());
            co_return;
        }
        const double speed = elapsed_sec > 0 ? static_cast<double>(_total_bytes >> 20) / elapsed_sec : 0.0;
        plog.info("  shard {:2d}: files={:5}  bytes={:7}MB  speed={:.0f}MB/s  "
                  "dl-errors={}  lat min/p50/p99/max = {}/{}/{}/{} ms",
                  this_shard_id(),
                  _latencies._count,
                  _total_bytes >> 20,
                  speed,
                  _download_errors,
                  _latencies.percentile(0.0),
                  _latencies.percentile(0.5),
                  _latencies.percentile(0.99),
                  _latencies.percentile(1.0));
    }
};

// ─── uploader ────────────────────────────────────────────────────────────────
//
// Mirrors db/snapshot/backup_task.cc: components are uploaded individually via
// client::upload_file(), with the per-shard in-flight count bounded by what
// backup uses -- sstables_manager::dir_semaphore(), whose size is
// initial_sstable_loading_concurrency (default 4). upload_file() itself
// parallelises the parts of one file internally, bounded by the client.
//
// Keys follow the layout object_name(bucket, prefix, sstable_id, component)
// produces: /<bucket>/<prefix>/<sstable_id>/<component>. A fresh sstable_id is
// minted per sstable per run, so repeated runs accumulate rather than overwrite
// each other -- the same reason a real sstable gets a new generation.

struct upload_item {
    std::filesystem::path local;
    sstring key;
};

class uploader {
    counting_retry_strategy* _retry = nullptr;
    s3::throttling_controller* _limiter = nullptr; // raw observer; client owns it
    shared_ptr<s3::client> _client;
    std::vector<upload_item> _items;
    unsigned _connections;
    unsigned _file_concurrency;
    std::chrono::seconds _sample_interval;
    uint64_t _uploaded_bytes = 0;
    uint64_t _uploaded_files = 0;
    unsigned _upload_errors = 0;
    unsigned _upload_errors_throttled = 0;
    unsigned _upload_errors_masked = 0;
    utils::estimated_histogram _latencies;

    std::chrono::steady_clock::time_point _now() const noexcept { return std::chrono::steady_clock::now(); }

public:
    uploader(endpoint_params ep, unsigned connections, unsigned file_concurrency, unsigned max_retries, std::chrono::seconds sample_interval,
             std::vector<upload_item> items)
        : _items(std::move(items)), _connections(connections), _file_concurrency(file_concurrency), _sample_interval(sample_interval) {
        auto wired = make_wired_client(ep, _connections, max_retries);
        _retry = wired.retry;
        _limiter = wired.limiter;
        _client = std::move(wired.client);
    }

    // The slice is handed over after start(): sharded<> copies its constructor
    // arguments to every shard, which would give each one the whole list.
    void adopt(std::vector<upload_item> items) { _items = std::move(items); }

    // The abort_source has to be created here, per shard. abort_source::subscription
    // is an intrusive list hook and the list is not thread safe, so sharing one
    // across shards -- as passing it into invoke_on_all does -- corrupts the list
    // as soon as several shards register with it, and http::client::make_request
    // then segfaults walking a null node.
    future<> run() {
        abort_source as;
        size_t next = 0;

        // Upload had no in-flight reporting at all: throttling surfaced only in
        // the closing RESULT line. That is how a fleet run accumulated ~25k
        // SlowDown retries and 75 aborted multipart uploads before anyone could
        // see it. Sample on the same interval the download side uses.
        const auto round_start = _now();
        round_stats prev;
        double prev_at = 0;
        timer<lowres_clock> sample_timer;
        sample_timer.set_callback([this, round_start, &prev, &prev_at] {
            const auto at_sec = std::chrono::duration_cast<std::chrono::duration<double>>(_now() - round_start).count();
            auto stats = collect_stats();
            log_sample("PUT", at_sec, stats, prev, prev_at);
            prev = stats;
            prev_at = at_sec;
        });
        sample_timer.arm_periodic(_sample_interval);

        co_await coroutine::parallel_for_each(std::views::iota(0u, _file_concurrency), [this, &as, &next](unsigned) -> future<> {
            while (!as.abort_requested()) {
                const size_t i = next++;
                if (i >= _items.size()) {
                    co_return;
                }
                co_await upload_one(_items[i], as);
            }
        });
        sample_timer.cancel();
    }

    future<> upload_one(const upload_item& item, abort_source& as) {
        const auto t0 = _now();
        std::exception_ptr failure;
        try {
            const auto size = co_await file_size(item.local.native());
            co_await _client->upload_file(item.local, item.key, std::nullopt, std::nullopt, &as);
            _uploaded_bytes += size;
            ++_uploaded_files;
            _latencies.add(std::chrono::duration_cast<std::chrono::milliseconds>(_now() - t0).count());
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure && !as.abort_requested()) {
            // upload_part now propagates the exception a part died with, so a
            // throttled upload reports the 503 rather than an ETag parse error.
            plog.info("shard {}: error uploading {}: {}", this_shard_id(), item.key, failure);
            ++_upload_errors;
            switch (classify_failure(failure)) {
            case failure_kind::throttled:
                ++_upload_errors_throttled;
                break;
            case failure_kind::masked:
                ++_upload_errors_masked;
                break;
            case failure_kind::other:
                break;
            }
        }
    }

    future<> stop() { co_await _client->close(); }

    round_stats collect_stats() const noexcept {
        const auto c = _client->get_request_counters();
        return {
            .slowdown_errors = _retry ? _retry->slowdown_errors() : 0u,
            .network_errors = _retry ? _retry->network_errors() : 0u,
            .download_errors = _upload_errors,
            .failed_throttled = _upload_errors_throttled,
            .failed_masked = _upload_errors_masked,
            .throttle_exhaustions = _retry ? _retry->throttle_exhaustions() : 0u,
            .retry_quota_denials = _limiter ? _limiter->retry_quota_denials() : 0u,
            .freezes = _limiter ? _limiter->freezes() : 0u,
            .read_requests = c.write_ops + c.write_retries,
            .read_bytes = c.write_bytes,
            .completed = _uploaded_files,
            .sstables = 0,
        };
    }

    future<> log_stats(double elapsed_sec) const {
        if (_latencies._count == 0) {
            plog.info("  shard {:2d}: no files uploaded", this_shard_id());
            co_return;
        }
        plog.info("  shard {:2d}: files={:6}  bytes={:8}MB  speed={:.0f}MB/s  up-errors={}  lat min/p50/p99/max = {}/{}/{}/{} ms",
                  this_shard_id(),
                  _latencies._count,
                  _uploaded_bytes >> 20,
                  elapsed_sec > 0 ? static_cast<double>(_uploaded_bytes >> 20) / elapsed_sec : 0.0,
                  _upload_errors,
                  _latencies.percentile(0.0),
                  _latencies.percentile(0.5),
                  _latencies.percentile(0.99),
                  _latencies.percentile(1.0));
    }
};

// The corpus is laid out as <sstable_id>/<component>, so one directory is one
// sstable. A *new* id is minted for the upload key rather than reusing the
// directory name, so repeated upload runs accumulate objects instead of
// overwriting each other. A time UUID is used because that is what
// scylla_metadata::set_sstable_identifier() defaults to, giving the same key
// distribution a real node produces.
// Random key-root prefix. S3 partitions on the leading bytes of a key, so a prefix
// only spreads load when it comes *before* everything shared -- the hash element of
// the X2 layout sits after the static prefix, which leaves every key of a run with
// identical leading bytes. This one goes directly under the bucket.
namespace random_prefix {
constexpr std::array prefix_chars{
    '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
    'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_', 'a',
    'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

static std::string get_random_prefix(size_t length) {
  thread_local std::mt19937 gen = [] {
    std::random_device rd;
    std::seed_seq seq{rd(), rd(), rd(), rd(), rd(), rd(), rd(), rd()};
    return std::mt19937{seq};
  }();
  thread_local std::uniform_int_distribution<uint64_t> rand_dist(
      0, prefix_chars.size() - 1);
  std::string ret_val(length, '\0');
  std::ranges::generate(ret_val,
                        [&]() { return prefix_chars[rand_dist(gen)]; });
  return ret_val;
}
} // namespace random_prefix

static std::vector<upload_item> plan_uploads(const std::filesystem::path& corpus_dir, const sstring& bucket, const sstring& prefix,
                                            size_t random_prefix_len) {
    std::vector<upload_item> items;
    for (const auto& dir : std::filesystem::directory_iterator(corpus_dir)) {
        if (!dir.is_directory()) {
            continue;
        }
        const auto sid = fresh_sstable_id();
        // Drawn once per sstable, not per component: a backup writes an sstable's
        // components together, and splitting them across prefixes would measure a
        // key distribution no real workload produces.
        const auto root = random_prefix_len ? random_prefix::get_random_prefix(random_prefix_len) : std::string{};
        for (const auto& comp : std::filesystem::directory_iterator(dir.path())) {
            if (!comp.is_regular_file()) {
                continue;
            }
            const auto name = comp.path().filename().string();
            if (name.ends_with(".partial")) {
                continue; // interrupted download, not a complete component
            }
            // Join only the elements that are set, so an empty --upload_prefix drops
            // out of the key instead of leaving a double slash.
            std::vector<sstring> parts;
            if (!root.empty()) {
                parts.emplace_back(root);
            }
            if (!prefix.empty()) {
                parts.push_back(prefix);
            }
            parts.emplace_back(fmt::to_string(sid));
            parts.emplace_back(name);
            items.push_back(upload_item{
                .local = comp.path(),
                .key = fmt::format("/{}/{}", bucket, fmt::join(parts, "/")),
            });
        }
    }
    return items;
}

// ─── bucket listing ──────────────────────────────────────────────────────────

static future<std::vector<sstring>> list_bucket(const endpoint_params& ep, const sstring& bucket, const sstring& prefix, size_t max_objects) {
    auto client = s3::client::make(ep.host, ep.make_config(s3::endpoint_config::default_connections_per_shard));
    std::vector<sstring> files;

    auto bl = abstract_lister::make<s3::client::bucket_lister>(client, bucket, prefix);
    bool truncated = false;
    while (auto entry = co_await bl.get()) {
        // bucket_lister strips exactly prefix.size() characters off the key, so
        // the full key is prefix and name concatenated. Joining them as paths
        // would insert a separator that is not in the key whenever the prefix
        // does not happen to end on a component boundary.
        files.emplace_back(fmt::format("/{}/{}{}", bucket, prefix, entry->name));
        // Stop paging as soon as we have enough: listing a full node prefix is
        // ~50k keys and dominates the runtime of a short run.
        if (max_objects != 0 && files.size() >= max_objects) {
            truncated = true;
            break;
        }
    }
    // Draining to the end closes the lister implicitly; stopping early leaves
    // its fibre parked on a queue push, so it has to be closed explicitly.
    if (truncated) {
        co_await bl.close();
    }

    co_await client->close();
    plog.info("Listed {} files in s3://{}/{}", files.size(), bucket, prefix);
    co_return files;
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    // Defaults match what a Scylla node uses for its object-storage S3 client.
    constexpr unsigned default_connections = s3::endpoint_config::default_connections_per_shard;
    constexpr unsigned default_retries = aws::default_aws_retry_strategy::default_max_retries;
    // What restore passes to max_concurrent_for_each per shard, see
    // sstables_loader.cc:download_tablet_sstables().
    constexpr unsigned default_sstable_concurrency = 16;
    // What backup bounds itself by: sstables_manager::dir_semaphore(), sized from
    // initial_sstable_loading_concurrency (db/config.cc default 4).
    constexpr unsigned default_file_concurrency = 4;
    app.add_options()("bucket", bpo::value<sstring>()->default_value(""), "S3 bucket name (default: $S3_BUCKET_FOR_TEST)")(
        "prefix", bpo::value<sstring>()->default_value(""), "object key prefix to filter listed files")(
        "initial_connections", bpo::value<unsigned>()->default_value(default_connections), "connections per shard for the first round (doubles each round)")(
        "max_retries", bpo::value<unsigned>()->default_value(default_retries), "max retries per request inside the counting retry strategy")(
        "round_timeout", bpo::value<unsigned>()->default_value(15), "per-round timeout in minutes (0: run until the whole list is done)")(
        "corpus_dir", bpo::value<sstring>()->default_value(""), "save each downloaded object once under this directory (empty: do not save)")(
        "max_objects", bpo::value<size_t>()->default_value(0), "use at most this many listed objects (0: all)")(
        "fleet_size", bpo::value<unsigned>()->default_value(1), "number of instances sharing the dataset (1: this instance takes all of it)")(
        "fleet_index", bpo::value<unsigned>()->default_value(0), "0-based index of this instance within the fleet")(
        "max_rounds", bpo::value<unsigned>()->default_value(0), "stop after this many rounds even if nothing failed (0: unlimited)")(
        "sample_interval", bpo::value<unsigned>()->default_value(10), "seconds between in-round rate samples")(
        "sstable_concurrency",
        bpo::value<unsigned>()->default_value(default_sstable_concurrency),
        "sstables downloaded concurrently per shard (restore uses 16)")("mode", bpo::value<sstring>()->default_value("download"), "download or upload")(
        "upload_bucket", bpo::value<sstring>()->default_value("manager-backup-tests-us-east-1"), "bucket to upload the corpus into")(
        "upload_prefix", bpo::value<sstring>()->default_value("sstables_ewz"), "key prefix for uploads")(
        "upload_random_prefix", bpo::value<unsigned>()->default_value(0),
        "prepend N random base64url characters directly under the bucket, ahead of --upload_prefix (0: off)")(
        "upload_no_prefix", bpo::bool_switch(),
        "omit --upload_prefix from the key entirely (boost rejects an empty --upload_prefix value)")(
        "file_concurrency",
        bpo::value<unsigned>()->default_value(default_file_concurrency),
        "components uploaded concurrently per shard (backup uses initial_sstable_loading_concurrency, 4)");

    return app.run(argc, argv, [&app]() -> future<> {
        const sstring bucket = app.configuration()["bucket"].as<sstring>().empty() ? sstring(tests::getenv_safe("S3_BUCKET_FOR_TEST"))
                                                                                   : app.configuration()["bucket"].as<sstring>();
        const sstring prefix = app.configuration()["prefix"].as<sstring>();
        const auto endpoint = endpoint_params::from_environment();
        const unsigned initial_connections = app.configuration()["initial_connections"].as<unsigned>();
        const unsigned max_retries = app.configuration()["max_retries"].as<unsigned>();
        const auto round_timeout = std::chrono::minutes(app.configuration()["round_timeout"].as<unsigned>());
        const std::filesystem::path corpus_dir{app.configuration()["corpus_dir"].as<sstring>().c_str()};
        const size_t max_objects = app.configuration()["max_objects"].as<size_t>();
        const unsigned fleet_size = app.configuration()["fleet_size"].as<unsigned>();
        const unsigned fleet_index = app.configuration()["fleet_index"].as<unsigned>();
        unsigned max_rounds = app.configuration()["max_rounds"].as<unsigned>();
        // Walking the list once is a single round by definition; without this the
        // connection-doubling loop would start over and re-fetch everything.
        if (round_timeout.count() == 0 && max_rounds == 0) {
            max_rounds = 1;
        }
        const auto sample_interval = std::chrono::seconds(app.configuration()["sample_interval"].as<unsigned>());
        const unsigned sstable_concurrency = app.configuration()["sstable_concurrency"].as<unsigned>();
        const sstring mode = app.configuration()["mode"].as<sstring>();
        const sstring upload_bucket = app.configuration()["upload_bucket"].as<sstring>();
        const sstring upload_prefix = app.configuration()["upload_prefix"].as<sstring>();
        const unsigned file_concurrency = app.configuration()["file_concurrency"].as<unsigned>();
        const auto upload_random_prefix = app.configuration()["upload_random_prefix"].as<unsigned>();
        const bool upload_no_prefix = app.configuration()["upload_no_prefix"].as<bool>();

        if (mode != "download" && mode != "upload") {
            throw std::invalid_argument(format("unknown mode '{}', expected download or upload", mode));
        }

        if (fleet_size < 1) {
            throw std::invalid_argument("fleet_size must be >= 1");
        }
        if (fleet_index >= fleet_size) {
            throw std::invalid_argument(format("fleet_index {} is out of range for fleet_size {}", fleet_index, fleet_size));
        }

        if (mode == "upload") {
            if (corpus_dir.empty()) {
                throw std::invalid_argument("upload mode needs --corpus_dir");
            }
            auto items = plan_uploads(corpus_dir, upload_bucket, upload_no_prefix ? sstring{} : upload_prefix, upload_random_prefix);
            if (items.empty()) {
                plog.error("no sstable components found under {}", corpus_dir.native());
                co_return;
            }
            plog.info("Uploading {} components to s3://{}/{}/ ({} concurrent per shard x {} shards)",
                      items.size(),
                      upload_bucket,
                      upload_prefix,
                      file_concurrency,
                      this_smp_shard_count());

            // Each shard takes a disjoint slice, so a component is uploaded once.
            sharded<uploader> uploaders;
            std::vector<std::vector<upload_item>> per_shard(this_smp_shard_count());
            for (size_t i = 0; i < items.size(); ++i) {
                per_shard[i % this_smp_shard_count()].push_back(std::move(items[i]));
            }
            co_await uploaders.start(endpoint, initial_connections, file_concurrency, max_retries, sample_interval, std::vector<upload_item>{});
            // hand each shard its slice
            for (unsigned sh = 0; sh < this_smp_shard_count(); ++sh) {
                co_await uploaders.invoke_on(sh, [slice = std::move(per_shard[sh])](uploader& u) mutable -> future<> {
                    u.adopt(std::move(slice));
                    co_return;
                });
            }

            const auto t0 = std::chrono::steady_clock::now();
            co_await uploaders.invoke_on_all([](uploader& u) { return u.run(); });
            const double elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - t0).count();

            co_await uploaders.invoke_on_all([elapsed](uploader& u) { return u.log_stats(elapsed); });
            round_stats total;
            for (unsigned sh = 0; sh < this_smp_shard_count(); ++sh) {
                total += co_await uploaders.invoke_on(sh, &uploader::collect_stats);
            }
            co_await uploaders.stop();

            const double puts = elapsed > 0 ? static_cast<double>(total.read_requests) / elapsed : 0.0;
            const double mbs = elapsed > 0 ? static_cast<double>(total.read_bytes >> 20) / elapsed : 0.0;
            plog.warn("RESULT {{\"mode\":\"upload\",\"shards\":{},\"elapsed_sec\":{:.1f},"
                      "\"requests\":{},\"requests_per_sec\":{:.0f},\"files\":{},\"mbytes_per_sec\":{:.0f},"
                      "\"slowdown\":{},\"net_reset\":{},\"failed\":{},\"failed_throttled\":{},\"failed_masked\":{},"
                      "\"throttle_exhaustions\":{},\"retry_quota_denials\":{},\"freezes\":{}}}",
                      this_smp_shard_count(),
                      elapsed,
                      total.read_requests,
                      puts,
                      total.completed,
                      mbs,
                      total.slowdown_errors,
                      total.network_errors,
                      total.download_errors,
                      total.failed_throttled,
                      total.failed_masked,
                      total.throttle_exhaustions,
                      total.retry_quota_denials,
                      total.freezes);
            co_return;
        }

        if (initial_connections < 1) {
            throw std::invalid_argument("initial_connections must be >= 1");
        }

        plog.info("Endpoint {}://{}:{} region={}", endpoint.use_https ? "https" : "http", endpoint.host, endpoint.port, endpoint.region);

        // List files once on shard 0; the full vector is broadcast to every
        // shard via sharded<downloader>::start().
        auto files = co_await list_bucket(endpoint, bucket, prefix, max_objects);
        if (files.empty()) {
            plog.error("No files found in s3://{}/{} — nothing to do", bucket, prefix);
            co_return;
        }
        auto sstables = group_by_sstable(files);
        const auto fleet_total = sstables.size();
        sstables = select_fleet_slice(std::move(sstables), fleet_size, fleet_index);
        if (sstables.empty()) {
            plog.error("Fleet slice {}/{} is empty: only {} sstables under s3://{}/{}", fleet_index, fleet_size, fleet_total, bucket, prefix);
            co_return;
        }
        plog.info("Going to run on {} of {} sstables ({} objects listed) under s3://{}/{}, fleet slice {}/{}",
                  sstables.size(),
                  fleet_total,
                  files.size(),
                  bucket,
                  prefix,
                  fleet_index,
                  fleet_size);

        // Keep doubling connections each round until the retry strategy is
        // exhausted and downloads start failing outright.
        for (unsigned conns = initial_connections, round = 1;; conns *= 2, ++round) {
            plog.info("=== Round: {} connections/shard × {} shards = {} total connections ===", conns, this_smp_shard_count(), conns * this_smp_shard_count());

            sharded<downloader> downloaders;
            co_await downloaders.start(endpoint, conns, sstable_concurrency, max_retries, sample_interval, sstables, corpus_dir, round_timeout.count() == 0);

            const auto t0 = std::chrono::steady_clock::now();
            try {
                co_await downloaders.invoke_on_all([round_timeout](downloader& d) { return d.run(round_timeout); });
            } catch (...) {
                plog.error("Unexpected error during round: {}", std::current_exception());
            }

            const double elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - t0).count();

            co_await downloaders.invoke_on_all([elapsed](downloader& d) { return d.log_stats(elapsed); });

            // Aggregate the per-shard series: all shards sample on the same
            // interval from the same round start, so equal indices line up.
            std::vector<std::vector<sample>> per_shard;
            for (unsigned sh = 0; sh < this_smp_shard_count(); ++sh) {
                per_shard.push_back(co_await downloaders.invoke_on(sh, &downloader::collect_samples));
            }
            size_t n = per_shard.empty() ? 0 : per_shard.front().size();
            for (const auto& series : per_shard) {
                n = std::min(n, series.size());
            }
            round_stats prev;
            double prev_at = 0;
            for (size_t i = 0; i < n; ++i) {
                round_stats at_i;
                double at_sec = 0;
                for (const auto& series : per_shard) {
                    at_i += series[i].stats;
                    at_sec = std::max(at_sec, series[i].at_sec);
                }
                const auto r = at_i.since(prev, at_sec - prev_at);
                plog.info("  t={:6.1f}s  GET/s={:7.0f}  sst/s={:6.1f}  obj/s={:7.1f}  MB/s={:6.0f}  slowdown+={}  netreset+={}  failed+={}",
                          at_sec,
                          r.requests_per_sec,
                          r.sstables_per_sec,
                          r.objects_per_sec,
                          r.mbytes_per_sec,
                          at_i.slowdown_errors - prev.slowdown_errors,
                          at_i.network_errors - prev.network_errors,
                          at_i.download_errors - prev.download_errors);
                prev = at_i;
                prev_at = at_sec;
            }

            round_stats total;
            for (unsigned s = 0; s < this_smp_shard_count(); ++s) {
                total += co_await downloaders.invoke_on(s, &downloader::collect_stats);
            }

            co_await downloaders.stop();

            // GET/s is the axis S3 throttles on, so report it rather than
            // comparing against any assumed per-prefix limit.
            const double gets_per_sec = elapsed > 0 ? static_cast<double>(total.read_requests) / elapsed : 0.0;
            const double mbytes_per_sec = elapsed > 0 ? static_cast<double>(total.read_bytes >> 20) / elapsed : 0.0;
            plog.warn("--- conns/shard={:4}  GET/s={:.0f}  MB/s={:.0f}  slowdown={}  net-reset={}  dl-failed={}  elapsed={:.1f}s",
                      conns,
                      gets_per_sec,
                      mbytes_per_sec,
                      total.slowdown_errors,
                      total.network_errors,
                      total.download_errors,
                      elapsed);

            // One line per round, parseable, so runs from different builds can be
            // compared without scraping the human-readable log.
            plog.warn("RESULT {{\"conns_per_shard\":{},\"shards\":{},\"elapsed_sec\":{:.1f},"
                      "\"requests\":{},\"requests_per_sec\":{:.0f},\"objects\":{},\"objects_per_sec\":{:.1f},"
                      "\"sstables\":{},\"mbytes_per_sec\":{:.0f},\"slowdown\":{},\"net_reset\":{},\"failed\":{},"
                      "\"failed_throttled\":{},\"failed_masked\":{},"
                      "\"throttle_exhaustions\":{},\"retry_quota_denials\":{},\"freezes\":{}}}",
                      conns,
                      this_smp_shard_count(),
                      elapsed,
                      total.read_requests,
                      gets_per_sec,
                      total.completed,
                      elapsed > 0 ? total.completed / elapsed : 0.0,
                      total.sstables,
                      mbytes_per_sec,
                      total.slowdown_errors,
                      total.network_errors,
                      total.download_errors,
                      total.failed_throttled,
                      total.failed_masked,
                      total.throttle_exhaustions,
                      total.retry_quota_denials,
                      total.freezes);

            if (total.download_errors > 0) {
                plog.warn(">>> {} file(s) failed after all retries at {} connections/shard — "
                          "connection limit reached",
                          total.download_errors,
                          conns);
                break;
            }
            if (max_rounds != 0 && round >= max_rounds) {
                plog.info(">>> Reached the {}-round limit without a failure", max_rounds);
                break;
            }
        }
    });
}
