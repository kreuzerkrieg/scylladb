/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/units.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"
#include "utils/s3/client.hh"
#include "utils/estimated_histogram.hh"

seastar::logger plog("perf");

class tester {
    std::chrono::seconds _duration;
    std::string _object_name;
    size_t _object_size;
    semaphore _mem;
    shared_ptr<s3::client> _client;
    utils::estimated_histogram _latencies;
    unsigned _errors = 0;
    unsigned _part_size_mb;
    bool _remove_file;

    static s3::endpoint_config_ptr make_config(unsigned sockets) {
        s3::endpoint_config cfg;
        cfg.port = 443;
        cfg.use_https = true;
        cfg.region = tests::getenv_safe("AWS_DEFAULT_REGION");
        cfg.max_connections = sockets;

        return make_lw_shared<s3::endpoint_config>(std::move(cfg));
    }

    static constexpr unsigned chunk_size = 1000;

    std::chrono::steady_clock::time_point now() const { return std::chrono::steady_clock::now(); }

public:
    tester(std::chrono::seconds dur, unsigned sockets, unsigned part_size, sstring object_name, size_t obj_size)
            : _duration(dur)
            , _object_name(std::move(object_name))
            , _object_size(obj_size)
            , _mem(memory::stats().total_memory())
            , _client(s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_config(sockets), _mem))
            , _part_size_mb(part_size)
            , _remove_file(false)
    {}

private:
    future<> make_temporary_file() {
        _object_name = fmt::format("/{}/perfobject-{}-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid(), this_shard_id());
        _remove_file = true;
        plog.debug("Creating {} of {} bytes", _object_name, _object_size);

        auto out = output_stream<char>(_client->make_upload_sink(_object_name));
        std::exception_ptr ex;
        try {
            auto rnd = tests::random::get_bytes(chunk_size);
            uint64_t written = 0;
            do {
                co_await out.write(reinterpret_cast<char*>(rnd.begin()), std::min(_object_size - written, rnd.size()));
                written += rnd.size();
            } while (written < _object_size);
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
            plog.error("Cannot write object: {}", ex);
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    }

public:
    future<> start() {
        if (_object_name.empty()) {
            co_await make_temporary_file();
        }
    }

    future<> run_download() {
        plog.info("Downloading");
        auto until = now() + _duration;
        uint64_t off = 0;
        do {
            auto start = now();
            try {
                co_await _client->get_object_contiguous(_object_name, s3::range{off, chunk_size});
                off = (off + chunk_size) % (_object_size - chunk_size);
                _latencies.add(std::chrono::duration_cast<std::chrono::milliseconds>(now() - start).count());
            } catch (...) {
                _errors++;
            }
        } while (now() < until);
    }

    future<> run_upload(std::atomic<double>& throughput) {
        plog.info("Uploading");
        auto file_name = fs::path(_object_name);
        auto sz = co_await seastar::file_size(file_name.native());
        _object_name = fmt::format("/{}/{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), file_name.filename().native());
        _remove_file = true;
        auto start = now();
        co_await _client->upload_file(file_name, _object_name, {}, _part_size_mb << 20);
        auto time = std::chrono::duration_cast<std::chrono::duration<double>>(now() - start);
        plog.info("Uploaded {}MB in {}s, speed {}MB/s", sz >> 20, time.count(), (sz >> 20) / time.count());
        throughput += static_cast<double>(sz) / time.count();
    }

    future<> stop() {
        if (_remove_file) {
            plog.debug("Removing {}", _object_name);
            co_await _client->delete_object(_object_name);
        }
        co_await _client->close();

        auto print_percentiles = [] (const utils::estimated_histogram& hist) {
            return format("min: {:-6d}, 50%: {:-6d}, 90%: {:-6d}, 99%: {:-6d}, 99.9%: {:-6d}, max: {:-6d} [ms]",
                hist.percentile(0),
                hist.percentile(0.5),
                hist.percentile(0.9),
                hist.percentile(0.99),
                hist.percentile(0.999),
                hist.percentile(1.0)
            );
        };
        plog.info("requests total: {:5}, errors: {:5}; latencies: {}", _latencies._count, _errors, print_percentiles(_latencies));
    }
};

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("upload", "test file upload")
        ("duration", bpo::value<unsigned>()->default_value(10), "seconds to run")
        ("sockets", bpo::value<unsigned>()->default_value(1), "maximum number of socket for http client")
        ("part_size_mb", bpo::value<unsigned>()->default_value(5), "part size")
        ("object_name", bpo::value<sstring>()->default_value(""), "use given object/file name")
        ("object_size", bpo::value<size_t>()->default_value(1 << 20), "size of test object")
    ;

    return app.run(argc, argv, [&app] () -> future<> {
        auto dur = std::chrono::seconds(app.configuration()["duration"].as<unsigned>());
        auto sks = app.configuration()["sockets"].as<unsigned>();
        // auto part_size = app.configuration()["part_size_mb"].as<unsigned>();
        auto oname = app.configuration()["object_name"].as<sstring>();
        auto osz = app.configuration()["object_size"].as<size_t>();
        // auto upload = app.configuration().contains("upload");
        /*sharded<tester> test;
        plog.info("Creating");
        co_await test.start(dur, sks, part_size, oname, osz);*/
        /*plog.info("Starting");
        co_await test.invoke_on_all(&tester::start);*/

        auto format_size = [](double bytes) {
            const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
            unsigned unitIndex = 0;
            while (bytes >= 1024 && unitIndex < sizeof(units) / sizeof(units[0]) - 1) {
                bytes /= 1024;
                ++unitIndex;
            }
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(2) << bytes << " " << units[unitIndex];
            return oss.str();
        };
        try {
            plog.info("Running");
            size_t file_size = 1;
            std::vector<std::tuple<size_t, std::string>> files_to_test;
            for (; file_size < 10_GiB; file_size <<= 4) {
                std::string filename = fmt::format("{}_testfile.bin", file_size);
                try {
                    auto actual_size=co_await seastar::file_size(fs::path(filename).native());
                    if (actual_size != file_size) {
                        throw std::runtime_error(fmt::format("File {} has wrong size: {} instead of {}", filename, actual_size, file_size));
                    }
                } catch (...) {
                    std::cout << "Looks like the file is missing or has a wrong size." << std::endl
                              << "It is expected that the script will be executed prior to running the test." << std::endl
                              <<
                        R"(#!/bin/bash

file_size=1
max_size=$((10 * 1024 ** 3)) # 10 GiB
block_size=$((1024 * 1024))  # 1 MiB

while [ $file_size -lt $max_size ]; do
    filename="${file_size}_testfile.bin"
    echo "Creating file ${filename}, size ${file_size} bytes"

    if [ $file_size -le $((2 * 1024 ** 3)) ]; then
        dd if=/dev/urandom of=${filename} bs=${file_size} count=1
    else
        count=$((file_size / block_size))
        dd if=/dev/urandom of=${filename} bs=${block_size} count=${count}
    fi

    file_size=$((file_size << 4)) # Increase file_size by a factor of 16
done
)" << std::endl;
                }
                std::cout << fmt::format("Creating file {}, size {}", filename, format_size(file_size)) << std::endl;
                files_to_test.emplace_back(file_size, std::move(filename));
            };
            std::string results = "+-----------+---------------+-----------------+------------------+\n";
            results += "|    SMP    |    Sockets    |    File size    |    Throughput    |\n";
            results += "+-----------+---------------+-----------------+------------------+\n";
            for (sks = 1; sks <= 16; ++sks) {
                for (const auto& [size, name] : files_to_test) {
                    sharded<tester> test;
                    plog.info("Creating");
                    co_await test.start(dur, sks, 50_MiB, name, osz);
                    std::atomic<double> combined_throughput;
                    co_await test.invoke_on_all(&tester::run_upload, std::ref(combined_throughput));
                    auto throughput = format_size(combined_throughput.load()) + "/s";
                    results += fmt::format("|{:^11}|{:^15}|{:^17}|{:^18}|\n", smp::count, sks, format_size(size), throughput);
                    co_await test.stop();
                }
            }
            results += "+-----------+---------------+-----------------+------------------+\n";
            std::cout << results;
        } catch (...) {
            plog.error("Error running: {}", std::current_exception());
        }
        plog.info("Stopping (and printing results)");
    });
}
