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
#include <chrono>
#include <filesystem>
#include <random>
#include <unordered_set>
#include <ranges>

#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
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
#include "utils/s3/client.hh"
#include "utils/s3/default_aws_retry_strategy.hh"

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

public:
    explicit counting_retry_strategy(unsigned max_retries) : aws::default_aws_retry_strategy(max_retries) {}

    future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;

    unsigned slowdown_errors() const noexcept { return _slowdown_errors; }
    unsigned network_errors() const noexcept { return _network_errors; }
    void reset() noexcept { _slowdown_errors = _network_errors = 0; }
};

future<bool> counting_retry_strategy::should_retry(std::exception_ptr error, unsigned attempted_retries) const {
    try {
        std::rethrow_exception(error);
    } catch (const aws::aws_exception& e) {
        const auto type = e.error().get_error_type();
        if (type == aws::aws_error_type::SLOW_DOWN || type == aws::aws_error_type::HTTP_TOO_MANY_REQUESTS || type == aws::aws_error_type::SERVICE_UNAVAILABLE ||
            type == aws::aws_error_type::HTTP_SERVICE_UNAVAILABLE) {
            ++_slowdown_errors;
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

// ─── round_stats ─────────────────────────────────────────────────────────────

struct round_stats {
    unsigned slowdown_errors = 0;
    unsigned network_errors = 0;
    unsigned download_errors = 0; // retry exhausted — unrecoverable; stop when > 0
    // GETs actually put on the wire, counted by the HTTP client rather than by
    // this test: chunked_download_source issues one request per chunk plus one
    // per internal retry, none of which are visible from here.
    uint64_t read_requests = 0;
    uint64_t read_bytes = 0;

    round_stats& operator+=(const round_stats& o) noexcept {
        slowdown_errors += o.slowdown_errors;
        network_errors += o.network_errors;
        download_errors += o.download_errors;
        read_requests += o.read_requests;
        read_bytes += o.read_bytes;
        return *this;
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

// ─── sstable grouping ────────────────────────────────────────────────────────
//
// Restore fetches whole sstables, not loose objects: per shard it runs
// max_concurrent_for_each over sstables with a fixed concurrency, and within one
// sstable it streams the components one after another, TOC first and Scylla
// second (see download_sstable() in sstables_loader_helpers.cc). The request
// pattern S3 sees depends on that shape, so the test reproduces it rather than
// fetching every object independently.

struct sstable_group {
    std::vector<sstring> components; // TOC first, Scylla second, then the rest
};

// Component is the last '-' separated field of the basename. Dropping it leaves
// an identifier shared by every component of one sstable, for both the modern
// "<ver>-<gen>-<fmt>-<comp>" and the older "<ks>-<cf>-<ver>-<gen>-<comp>" forms.
static std::string_view component_of(std::string_view key) {
    const auto base = key.substr(key.rfind('/') + 1);
    const auto dash = base.rfind('-');
    return dash == std::string_view::npos ? base : base.substr(dash + 1);
}

static std::string_view sstable_of(std::string_view key) {
    const auto dash = key.rfind('-');
    return dash == std::string_view::npos ? key : key.substr(0, dash);
}

static std::vector<sstable_group> group_by_sstable(const std::vector<sstring>& keys) {
    std::unordered_map<std::string_view, std::vector<sstring>> by_sstable;
    for (const auto& k : keys) {
        by_sstable[sstable_of(k)].push_back(k);
    }

    std::vector<sstable_group> groups;
    groups.reserve(by_sstable.size());
    for (auto& [_, components] : by_sstable) {
        // Same ordering restore imposes, for the same reason: the TOC has to be
        // first, and the Scylla component second.
        const auto rank = [](const sstring& k) {
            const auto c = component_of(k);
            return c == "TOC.txt" ? 0 : c == "Scylla.db" ? 1 : 2;
        };
        std::stable_sort(components.begin(), components.end(), [&rank](const sstring& a, const sstring& b) { return rank(a) < rank(b); });
        groups.push_back(sstable_group{.components = std::move(components)});
    }
    return groups;
}

// ─── downloader ──────────────────────────────────────────────────────────────
//
// One instance per shard.  Each instance holds its own S3 client.  The file
// list is identical on all shards but shuffled with a per-shard seed so that
// concurrent shards access objects in a different order, spreading the load.

class downloader {
    counting_retry_strategy* _retry = nullptr; // raw observer; client owns the unique_ptr
    shared_ptr<s3::client> _client;
    std::vector<sstable_group> _sstables;              // shuffled per shard
    unsigned _connections;                             // connection pool size
    unsigned _sstable_concurrency;                     // concurrent sstables per shard, as in restore
    std::filesystem::path _corpus_dir;                 // empty when persistence is disabled
    std::unordered_set<std::string_view> _corpus_busy; // objects currently being saved
    uint64_t _total_bytes = 0;
    uint64_t _sstables_done = 0;
    unsigned _download_errors = 0;
    utils::estimated_histogram _latencies;

    std::chrono::steady_clock::time_point _now() const noexcept { return std::chrono::steady_clock::now(); }

    // Local path an object is persisted to.  The leading "/<bucket>" is dropped
    // so that the directory tree mirrors the in-bucket key layout, which is what
    // the upload phase needs in order to reproduce it.
    std::filesystem::path corpus_path(const sstring& object_key) const {
        std::filesystem::path key{object_key.c_str()};
        auto it = key.begin();
        ++it; // leading "/"
        ++it; // bucket name
        std::filesystem::path relative;
        for (; it != key.end(); ++it) {
            relative /= *it;
        }
        return _corpus_dir / relative;
    }

    // Objects are saved at most once: the first pass populates the corpus and
    // later rounds run without touching the disk, so local I/O never becomes the
    // bottleneck during the rounds that matter.
    // Objects are written to a ".partial" sibling and renamed once the download
    // has completed, so an interrupted or failed transfer can never leave a
    // truncated file that the existence check above would then skip forever.
    // Several workers cycle over the same file list concurrently, so the same
    // object is routinely downloaded more than once at a time. Only one of those
    // downloads may write to disk: _corpus_busy holds the objects already being
    // saved. It is claimed before the first co_await, otherwise two workers both
    // pass the check while suspended and then race on the rename.
    future<std::optional<output_stream<char>>> open_corpus_sink(const sstring& object_key) {
        // Only one shard saves. _corpus_busy is per shard and the existence check
        // below only helps once a rename has landed, so letting every shard save
        // means each object gets written up to smp::count times over.
        if (_corpus_dir.empty() || this_shard_id() != 0 || _corpus_busy.contains(object_key)) {
            co_return std::nullopt;
        }
        _corpus_busy.emplace(object_key);
        auto path = corpus_path(object_key);
        if (co_await file_exists(path.native())) {
            _corpus_busy.erase(object_key);
            co_return std::nullopt;
        }
        co_await recursive_touch_directory(path.parent_path().native());
        auto f = co_await open_file_dma(partial_path(path).native(), open_flags::wo | open_flags::create | open_flags::truncate);
        co_return co_await make_file_output_stream(std::move(f));
    }

    // Shards share the corpus directory but keep separate _corpus_busy sets, so
    // the partial name has to carry the shard id to keep them from colliding.
    static std::filesystem::path partial_path(const std::filesystem::path& final_path) {
        return fmt::format("{}.partial.{}", final_path.native(), this_shard_id());
    }

    // Components go one at a time, in order, exactly as restore streams them.
    future<> download_sstable(const sstable_group& sst, abort_source& as) {
        for (const auto& component : sst.components) {
            if (as.abort_requested()) {
                co_return;
            }
            co_await download_one(component, as);
        }
        ++_sstables_done;
    }

    future<> download_one(const sstring& file, abort_source& as) {
        const auto t0 = _now();
        // sink is declared out here so the failure path can still close it: an
        // output_stream asserts in its destructor when it was never closed, which
        // takes down the process instead of failing the one download. It holds a
        // value only while the stream is open; claimed tracks the _corpus_busy
        // entry, which outlives the stream.
        std::optional<output_stream<char>> sink;
        std::exception_ptr failure;
        bool claimed = false;
        auto release = defer([this, &file, &claimed]() noexcept {
            if (claimed) {
                _corpus_busy.erase(file);
            }
        });
        try {
            sink = co_await open_corpus_sink(file);
            claimed = sink.has_value();
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
                auto path = corpus_path(file);
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
            try {
                co_await sink->close();
            } catch (...) {
            }
            // Drop the partial so the object is retried rather than being taken
            // for a complete one on a later pass.
            try {
                co_await remove_file(partial_path(corpus_path(file)).native());
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
    }

public:
    downloader(endpoint_params ep,
               unsigned connections,
               unsigned sstable_concurrency,
               unsigned max_retries,
               std::vector<sstable_group> sstables,
               std::filesystem::path corpus_dir)
        : _sstables(std::move(sstables)), _connections(connections), _sstable_concurrency(sstable_concurrency), _corpus_dir(std::move(corpus_dir)) {
        // Give each shard a distinct permutation so concurrent shards do not
        // hammer the same sstables in the same order.
        std::shuffle(_sstables.begin(), _sstables.end(), std::default_random_engine(this_shard_id()));

        auto rs = std::make_unique<counting_retry_strategy>(max_retries);
        _retry = rs.get();
        _client = s3::client::make(ep.host, ep.make_config(_connections), std::move(rs));
    }

    future<> run(std::chrono::minutes round_timeout) {
        _total_bytes = 0;
        _download_errors = 0;
        _latencies = utils::estimated_histogram{};
        _sstables_done = 0;
        _retry->reset();

        abort_source as;
        timer<lowres_clock> timeout_timer;
        timeout_timer.set_callback([&as] {
            if (!as.abort_requested()) {
                plog.info("shard {}: round timeout reached, stopping downloads", this_shard_id());
                as.request_abort();
            }
        });
        timeout_timer.arm(round_timeout);

        try {
            // One worker per concurrently-restored sstable, matching the
            // max_concurrent_for_each concurrency restore uses per shard. The
            // worker set is what is bounded, not the input range: cycling an
            // unbounded range through max_concurrent_for_each becomes a busy loop
            // the moment the abort makes every item return immediately.
            size_t next = 0;
            co_await coroutine::parallel_for_each(std::views::iota(0u, _sstable_concurrency), [this, &as, &next](unsigned) -> future<> {
                while (!as.abort_requested()) {
                    co_await download_sstable(_sstables[next++ % _sstables.size()], as);
                }
            });
        } catch (const abort_requested_exception&) {
            // parallel_for_each itself was interrupted by the timeout
        }

        timeout_timer.cancel();
    }

    // Called by sharded<downloader>::stop().
    future<> stop() { co_await _client->close(); }

    round_stats collect_stats() const noexcept {
        const auto counters = _client->get_request_counters();
        return {
            .slowdown_errors = _retry ? _retry->slowdown_errors() : 0u,
            .network_errors = _retry ? _retry->network_errors() : 0u,
            .download_errors = _download_errors,
            .read_requests = counters.read_ops + counters.read_retries,
            .read_bytes = counters.read_bytes,
        };
    }

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
    // Draining to the end closes the lister implicitly; stopping early leaves its
    // fibre parked on a queue push, so it has to be closed explicitly.
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
    app.add_options()("bucket", bpo::value<sstring>()->default_value(""), "S3 bucket name (default: $S3_BUCKET_FOR_TEST)")(
        "prefix", bpo::value<sstring>()->default_value(""), "object key prefix to filter listed files")(
        "initial_connections", bpo::value<unsigned>()->default_value(default_connections), "connections per shard for the first round (doubles each round)")(
        "max_retries", bpo::value<unsigned>()->default_value(default_retries), "max retries per request inside the counting retry strategy")(
        "round_timeout", bpo::value<unsigned>()->default_value(15), "per-round timeout in minutes")(
        "corpus_dir", bpo::value<sstring>()->default_value(""), "save each downloaded object once under this directory (empty: do not save)")(
        "max_objects", bpo::value<size_t>()->default_value(0), "use at most this many listed objects (0: all)")(
        "max_rounds", bpo::value<unsigned>()->default_value(0), "stop after this many rounds even if nothing failed (0: unlimited)")(
        "sstable_concurrency",
        bpo::value<unsigned>()->default_value(default_sstable_concurrency),
        "sstables downloaded concurrently per shard (restore uses 16)");

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
        const unsigned max_rounds = app.configuration()["max_rounds"].as<unsigned>();
        const unsigned sstable_concurrency = app.configuration()["sstable_concurrency"].as<unsigned>();

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
        plog.info("Going to run on {} objects in {} sstables under s3://{}/{}", files.size(), sstables.size(), bucket, prefix);

        // Keep doubling connections each round until the retry strategy is
        // exhausted and downloads start failing outright.
        for (unsigned conns = initial_connections, round = 1;; conns *= 2, ++round) {
            plog.info("=== Round: {} connections/shard × {} shards = {} total connections ===", conns, this_smp_shard_count(), conns * this_smp_shard_count());

            sharded<downloader> downloaders;
            co_await downloaders.start(endpoint, conns, sstable_concurrency, max_retries, sstables, corpus_dir);

            const auto t0 = std::chrono::steady_clock::now();
            try {
                co_await downloaders.invoke_on_all([round_timeout](downloader& d) { return d.run(round_timeout); });
            } catch (...) {
                plog.error("Unexpected error during round: {}", std::current_exception());
            }
            const double elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - t0).count();

            co_await downloaders.invoke_on_all([elapsed](downloader& d) { return d.log_stats(elapsed); });

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
