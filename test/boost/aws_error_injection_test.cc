/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/tmpdir.hh"
#include "utils/exceptions.hh"
#include "utils/s3/aws_errors.hh"
#include "utils/s3/client.hh"
#include <cstdlib>
#include <seastar/core/fstream.hh>
#include <seastar/core/units.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/closeable.hh>

using namespace seastar;
using namespace std::chrono_literals;
struct server {
    enum class failure_policy : uint8_t {
        SUCCESS = 0,
        RETRYABLE_FAILURE = 1,
        NONRETRYABLE_FAILURE = 2,
        NEVERENDING_RETRYABLE_FAILURE = 3,
    };
    class dummy_aws_request_handler : public httpd::handler_base {
    public:
        explicit dummy_aws_request_handler(server& test_server) : _test_server(test_server) {}
        future<std::unique_ptr<http::reply>> handle(const sstring& path, std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) override {
            auto method = req->_method;
            auto url_params = req->query_parameters;
            testlog.debug("{}\t{}", req->_method, req->get_url());
            sstring response_body;
            if (method == "DELETE") {
                rep->set_status(http::reply::status_type::no_content);
                if (url_params.contains("uploadId") && _test_server.test_failure_policy == failure_policy::NONRETRYABLE_FAILURE) {
                    rep->set_status(http::reply::status_type::not_found);
                }
            } else if (method == "PUT") {
                rep->set_status(http::reply::status_type::ok);
                if (url_params.contains("partNumber") && url_params.contains("uploadId")) {
                    rep->add_header("ETag", "SomeTag_" + url_params.at("partNumber"));
                }
                rep->add_header("ETag", "SomeTag");
            } else if (method == "POST") {
                response_body = build_response(*req);
            }
            rep->write_body("txt", response_body);
            return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        }

    private:
        sstring build_response(const http::request& req) {
            assert(req._method == "POST");
            if (req.query_parameters.contains("uploads")) {
                return R"(<InitiateMultipartUploadResult>
                                <Bucket>bucket</Bucket>
                                <Key>key</Key>
                                <UploadId>UploadId</UploadId>
                            </InitiateMultipartUploadResult>)";
            }
            if (req.query_parameters.contains("uploadId")) {
                switch (_test_server.test_failure_policy) {
                case failure_policy::SUCCESS:
                    return R"(<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                     <Location>http://Example-Bucket.s3.Region.amazonaws.com/Example-Object</Location>
                                     <Bucket>Example-Bucket</Bucket>
                                     <Key>Example-Object</Key>
                                     <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
                                </CompleteMultipartUploadResult>)";
                case failure_policy::RETRYABLE_FAILURE:
                case failure_policy::NEVERENDING_RETRYABLE_FAILURE:
                    if (_test_server.test_failure_policy == failure_policy::RETRYABLE_FAILURE) {
                        // should succeed on retry
                        _test_server.test_failure_policy = failure_policy::SUCCESS;
                    }
                    assert(aws::aws_error_map.at("InternalError").is_retryable());
                    return R"(<?xml version="1.0" encoding="UTF-8"?>

                                 <Error>
                                  <Code>InternalError</Code>
                                  <Message>We encountered an internal error. Please try again.</Message>
                                  <RequestId>656c76696e6727732072657175657374</RequestId>
                                  <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                 </Error>)";
                case failure_policy::NONRETRYABLE_FAILURE:
                    assert(!aws::aws_error_map.at("InvalidAction").is_retryable());
                    return R"(<?xml version="1.0" encoding="UTF-8"?>

                                 <Error>
                                  <Code>InvalidAction</Code>
                                  <Message>Something went terribly wrong</Message>
                                  <RequestId>656c76696e6727732072657175657374</RequestId>
                                  <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                 </Error>)";
                default:
                    assert(false && "unexpected failure policy");
                }
            }
            assert(false && "unexpected query parameter");
        }

        server& _test_server;
    };

    explicit server(failure_policy _test_failure_policy) : test_failure_policy(_test_failure_policy) {
        handler = std::make_unique<dummy_aws_request_handler>(*this);
    }

    future<> start(const std::string& address, uint16_t port) {
        try {
            net::inet_address addr(address);
            std::cout << "Starting server on " << address << ":" << port << std::endl;
            http_server.start("test").get();

            http_server.server()
                .invoke_on_all([this](httpd::http_server& server) {
                    server._routes.add_default_handler(handler.get());
                    return make_ready_future<>();
                })
                .get();

            http_server.listen(socket_address{addr, port}).get();
            co_return;
        } catch (const std::exception& e) {
            http_server.server().stop().get();
            http_server.stop().get();
            throw std::runtime_error(format("Failed to start server. Reason: {}", e.what()));
        }
    }

    future<> stop() {
        co_await http_server.server().stop();
        co_await http_server.stop();
    }

private:
    std::unique_ptr<httpd::handler_base> handler;
    httpd::http_server_control http_server;
    std::atomic<failure_policy> test_failure_policy;
};


struct unshare_fixture {
    unshare_fixture() {
        if (std::getenv("RUN_IN_UNSHARE")) {
            int status = std::system("ip link set lo up");
            if (WIFEXITED(status)) {
                if (int exit_status = WEXITSTATUS(status); exit_status == 0) {
                    BOOST_TEST_MESSAGE("The network namespace was unshared");
                    return;
                }
            }
            fmt::print(std::cerr, "failed to setup lo: {}\n", status);
        } else {
            BOOST_TEST_MESSAGE("Cant unshare network namespace. Randomizing addresses and ports");
            get_port = [] {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<uint16_t> ports(std::numeric_limits<int16_t>::max(), std::numeric_limits<uint16_t>::max());
                return ports(gen);
            };
            get_address = [] {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<uint8_t> octet;
                return fmt::format("127.{}.{}.{}", octet(gen), octet(gen), octet(gen));
            };
        }
    }
    std::function<uint16_t(void)> get_port = [] { return 2012; };
    std::function<std::string(void)> get_address = [] { return "127.0.0.1"; };
};

[[maybe_unused]] static unshare_fixture network_unshare;

static s3::endpoint_config_ptr make_minio_config(uint16_t port) {
    s3::endpoint_config cfg = {
        .port = port,
        .use_https = false,
        .aws = {{
            .access_key_id = "foo",
            .secret_access_key = "bar",
            .session_token = "baz",
            .region = "us-east-1",
        }},
    };
    return make_lw_shared<s3::endpoint_config>(std::move(cfg));
}

void test_client_upload_file(std::string_view test_name, const std::string& address, uint16_t port, size_t total_size, size_t memory_size) {
    tmpdir tmp;
    const auto file_path = tmp.path() / fmt::format("test-{}", ::getpid());

    {
        file f = open_file_dma(file_path.native(), open_flags::create | open_flags::wo).get();
        auto output = make_file_output_stream(std::move(f)).get();
        auto close_file = deferred_close(output);
        std::string_view data = "1234567890ABCDEF";
        // so we can test !with_remainder case properly with multiple writes
        SCYLLA_ASSERT(total_size % data.size() == 0);

        for (size_t bytes_written = 0; bytes_written < total_size; bytes_written += data.size()) {
            output.write(data.data(), data.size()).get();
        }
    }

    const auto object_name = fmt::format("/{}/{}-{}", "test", test_name, ::getpid());

    semaphore mem{memory_size};
    auto client = s3::client::make(address, make_minio_config(port), mem);
    auto client_shutdown = deferred_close(*client);
    client->upload_file(file_path, object_name).get();
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_success) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::SUCCESS);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_NO_THROW(test_client_upload_file(seastar_test::get_name(), address, port, total_size, memory_size));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_retryable_success) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::RETRYABLE_FAILURE);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_NO_THROW(test_client_upload_file(seastar_test::get_name(), address, port, total_size, memory_size));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_failure_1) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::NEVERENDING_RETRYABLE_FAILURE);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_EXCEPTION(test_client_upload_file(seastar_test::get_name(), address, port, total_size, memory_size),
                            storage_io_error,
                            [](const storage_io_error& e) { return e.code().value() == EIO; });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_failure_2) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::NONRETRYABLE_FAILURE);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_EXCEPTION(test_client_upload_file(seastar_test::get_name(), address, port, total_size, memory_size),
                            storage_io_error,
                            [](const storage_io_error& e) { return e.code().value() == EIO; });
}

void do_test_client_multipart_upload(const std::string& address, uint16_t port) {
    const sstring name(fmt::format("/{}/testobject-{}", "test", "large", ::getpid()));

    testlog.info("Make client");
    semaphore mem(16 << 20);
    auto cln = s3::client::make(address, make_minio_config(port), mem);
    auto close_client = deferred_close(*cln);

    testlog.info("Upload object");
    auto out = output_stream<char>(cln->make_upload_sink(name));

    static constexpr unsigned chunk_size = 1000;
    auto rnd = tests::random::get_bytes(chunk_size);
    for (unsigned ch = 0; ch < 128_KiB; ch++) {
        out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
    }

    testlog.info("Flush multipart upload");
    out.flush().get();
    out.close().get();
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_success) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::SUCCESS);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    BOOST_REQUIRE_NO_THROW(do_test_client_multipart_upload(address, port));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_retryable_success) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::RETRYABLE_FAILURE);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    BOOST_REQUIRE_NO_THROW(do_test_client_multipart_upload(address, port));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_failure_1) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::NEVERENDING_RETRYABLE_FAILURE);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    BOOST_REQUIRE_EXCEPTION(
        do_test_client_multipart_upload(address, port), storage_io_error, [](const storage_io_error& e) { return e.code().value() == EIO; });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_failure_2) {
    auto port = network_unshare.get_port();
    auto address = network_unshare.get_address();
    server server(server::failure_policy::NONRETRYABLE_FAILURE);
    server.start(address, port).get();
    auto close_server = deferred_stop(server);
    BOOST_REQUIRE_EXCEPTION(
        do_test_client_multipart_upload(address, port), storage_io_error, [](const storage_io_error& e) { return e.code().value() == EIO; });
}
