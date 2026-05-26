/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Adaptive S3 download stress-tester.
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
#include <random>
#include <ranges>

#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/timer.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/closeable.hh>

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

    round_stats& operator+=(const round_stats& o) noexcept {
        slowdown_errors += o.slowdown_errors;
        network_errors += o.network_errors;
        download_errors += o.download_errors;
        return *this;
    }
};

// ─── downloader ──────────────────────────────────────────────────────────────
//
// One instance per shard.  Each instance holds its own S3 client.  The file
// list is identical on all shards but shuffled with a per-shard seed so that
// concurrent shards access objects in a different order, spreading the load.

class downloader {
    semaphore _mem;
    counting_retry_strategy* _retry = nullptr; // raw observer; client owns the unique_ptr
    shared_ptr<s3::client> _client;
    std::vector<sstring> _files; // shuffled per shard
    unsigned _connections;       // concurrency limit = connection pool size
    uint64_t _total_bytes = 0;
    unsigned _download_errors = 0;
    utils::estimated_histogram _latencies;

    static s3::endpoint_config_ptr make_config(const std::string& region, unsigned connections) {
        s3::endpoint_config cfg;
        cfg.port = 443;
        cfg.use_https = true;
        cfg.region = region;
        cfg.connections_per_shard = connections;
        return make_lw_shared<s3::endpoint_config>(std::move(cfg));
    }

    std::chrono::steady_clock::time_point _now() const noexcept { return std::chrono::steady_clock::now(); }

public:
    downloader(std::string endpoint, std::string region, unsigned connections, unsigned max_retries, std::vector<sstring> files)
        : _mem(memory::stats().total_memory()), _files(std::move(files)), _connections(connections) {
        // Give each shard a distinct permutation so concurrent shards do not
        // hammer the same objects in the same order.
        std::shuffle(_files.begin(), _files.end(), std::default_random_engine(this_shard_id()));

        auto rs = std::make_unique<counting_retry_strategy>(max_retries);
        _retry = rs.get();
        _client = s3::client::make(std::move(endpoint), make_config(region, _connections), _mem, std::move(rs));
    }

    future<> run(std::chrono::minutes round_timeout) {
        _total_bytes = 0;
        _download_errors = 0;
        _latencies = utils::estimated_histogram{};
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
            // Cycle through files indefinitely until the round timeout fires.
            // This keeps all connections saturated for the entire round duration.
            // iota without an upper bound produces an unbounded sequence; the
            // abort_source (fired by the timeout timer) stops the iteration.
            auto cycling_files = std::views::iota(size_t{0}) | std::views::transform([this](size_t i) -> const sstring& { return _files[i % _files.size()]; });

            co_await max_concurrent_for_each(cycling_files, _connections * 3, [this, &as](const sstring& file) -> future<> {
                if (as.abort_requested()) {
                    co_return;
                }

                const auto t0 = _now();
                try {
                    auto f = _client->make_readable_file(file, &as);
                    uint64_t sz = 0;
                    co_await with_closeable(make_file_input_stream(std::move(f)), [&sz](input_stream<char>& in) -> future<> {
                        co_await in.consume([&sz](auto buf) {
                            if (buf.empty()) {
                                return make_ready_future<consumption_result<char>>(stop_consuming(std::move(buf)));
                            }
                            sz += buf.size();
                            return make_ready_future<consumption_result<char>>(continue_consuming());
                        });
                    });
                    _total_bytes += sz;
                    _latencies.add(std::chrono::duration_cast<std::chrono::milliseconds>(_now() - t0).count());
                } catch (const storage_io_error& ex) {
                    // round timed out — stop silently
                    if (ex.what() != "S3 error (seastar::abort_requested_exception (abort requested))"sv) {
                        throw;
                    }
                } catch (...) {
                    plog.info("shard {}: error downloading {}: {}", this_shard_id(), file, std::current_exception());
                    ++_download_errors;
                }
            });
        } catch (const abort_requested_exception&) {
            // parallel_for_each itself was interrupted by timeout
        }

        timeout_timer.cancel();
    }

    // Called by sharded<downloader>::stop().
    future<> stop() { co_await _client->close(); }

    round_stats collect_stats() const noexcept {
        return {
            .slowdown_errors = _retry ? _retry->slowdown_errors() : 0u,
            .network_errors = _retry ? _retry->network_errors() : 0u,
            .download_errors = _download_errors,
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

static future<std::vector<sstring>> list_bucket(const std::string& endpoint, const std::string& region, const sstring& bucket, const sstring& prefix) {
    semaphore mem(memory::stats().total_memory());
    auto cfg = make_lw_shared<s3::endpoint_config>();
    cfg->port = 443;
    cfg->use_https = true;
    cfg->region = region;
    cfg->connections_per_shard = 4;

    auto client = s3::client::make(endpoint, cfg, mem);
    std::vector<sstring> files;

    auto bl = abstract_lister::make<s3::client::bucket_lister>(client, bucket, prefix);
    while (auto entry = co_await bl.get()) {
        // std::cout << fmt::format("/{}", (std::filesystem::path(bucket) / prefix / entry->name).c_str()) << std::endl;
        files.emplace_back(fmt::format("/{}", (std::filesystem::path(bucket) / prefix / entry->name).c_str()));
    }

    co_await client->close();
    plog.info("Listed {} files in s3://{}/{}", files.size(), bucket, prefix);
    co_return files;
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()("bucket", bpo::value<sstring>()->default_value(""), "S3 bucket name (default: $S3_BUCKET_FOR_TEST)")(
        "prefix", bpo::value<sstring>()->default_value(""), "object key prefix to filter listed files")(
        "initial_connections", bpo::value<unsigned>()->default_value(8), "connections per shard for the first round (doubles each round)")(
        "max_retries", bpo::value<unsigned>()->default_value(3), "max retries per request inside the counting retry strategy")(
        "round_timeout", bpo::value<unsigned>()->default_value(15), "per-round timeout in minutes");

    return app.run(argc, argv, [&app]() -> future<> {
        const sstring bucket = app.configuration()["bucket"].as<sstring>().empty() ? sstring(tests::getenv_safe("S3_BUCKET_FOR_TEST"))
                                                                                   : app.configuration()["bucket"].as<sstring>();
        const sstring prefix = app.configuration()["prefix"].as<sstring>();
        const std::string endpoint = tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST");
        const std::string region = tests::getenv_safe("AWS_DEFAULT_REGION");
        const unsigned initial_connections = app.configuration()["initial_connections"].as<unsigned>();
        const unsigned max_retries = app.configuration()["max_retries"].as<unsigned>();
        const auto round_timeout = std::chrono::minutes(app.configuration()["round_timeout"].as<unsigned>());

        if (initial_connections < 1) {
            throw std::invalid_argument("initial_connections must be >= 1");
        }

        // List files once on shard 0; the full vector is broadcast to every
        // shard via sharded<downloader>::start().
        auto files = co_await list_bucket(endpoint, region, bucket, prefix);
        if (files.empty()) {
            plog.error("No files found in s3://{}/{} — nothing to do", bucket, prefix);
            co_return;
        }
        plog.info("Going to run on {} objects under s3://{}/{}", files.size(), bucket, prefix);

        // Keep doubling connections each round until the retry strategy is
        // exhausted and downloads start failing outright.
        for (unsigned conns = initial_connections;; conns *= 2) {
            plog.info("=== Round: {} connections/shard × {} shards = {} total connections ===", conns, this_smp_shard_count(), conns * this_smp_shard_count());

            sharded<downloader> downloaders;
            co_await downloaders.start(endpoint, region, conns, max_retries, files);

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

            plog.warn("--- conns/shard={:4}  slowdown={}  net-reset={}  dl-failed={}  elapsed={:.1f}s",
                      conns,
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
        }
    });
}
