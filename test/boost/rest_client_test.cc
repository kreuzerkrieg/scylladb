/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include "utils/rest/client.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"

void simple_rest_client() {
    auto host = tests::getenv_safe("MOCK_S3_SERVER_HOST");
    auto port = std::stoul(tests::getenv_safe("MOCK_S3_SERVER_PORT"));
    rest::httpclient client(host, port);
    for ([[maybe_unused]] auto i : {1, 2}) {
        BOOST_REQUIRE_NO_THROW([&] {
            client.add_header("host", host);
            client.add_header("X-aws-ec2-metadata-token-ttl-seconds", "21600");
            client.method(rest::httpclient::method_type::PUT);
            client.target("/latest/api/token");
            [[maybe_unused]] auto res = client.send().get();
        }());
    }
}

SEASTAR_THREAD_TEST_CASE(test_simple_rest_client) {
    simple_rest_client();
}

namespace {
// Retries a fixed number of times without sleeping, and records how often it was
// consulted.
class counting_retry_strategy : public seastar::http::retry_strategy {
    unsigned _max_retries;
public:
    mutable unsigned consulted = 0;

    explicit counting_retry_strategy(unsigned max_retries)
        : _max_retries(max_retries)
    {}
    seastar::future<bool> should_retry(std::exception_ptr, unsigned attempted_retries) const override {
        ++consulted;
        return seastar::make_ready_future<bool>(attempted_retries < _max_retries);
    }
};
}

// A handler that throws is how a caller tells the http client that a reply it
// accepted at the transport level is still a failure. Without a strategy that
// ends the request; with one the request is sent again.
SEASTAR_THREAD_TEST_CASE(test_rest_client_retry_strategy) {
    auto host = tests::getenv_safe("MOCK_S3_SERVER_HOST");
    auto port = std::stoul(tests::getenv_safe("MOCK_S3_SERVER_PORT"));

    // Throws out of the first `failures` replies, and returns how many replies it
    // took to get through.
    auto send = [&](unsigned failures, const seastar::http::retry_strategy* strategy) {
        rest::httpclient client(host, port);
        client.add_header("host", host);
        client.add_header("X-aws-ec2-metadata-token-ttl-seconds", "21600");
        client.method(rest::httpclient::method_type::PUT);
        client.target("/latest/api/token");

        unsigned replies = 0;
        client.send([&](const seastar::http::reply&, std::string_view) {
            if (++replies <= failures) {
                throw std::runtime_error("induced failure");
            }
        }, nullptr, strategy).get();
        return replies;
    };

    BOOST_REQUIRE_EQUAL(send(0, nullptr), 1u);
    BOOST_REQUIRE_THROW(send(1, nullptr), std::runtime_error);

    counting_retry_strategy retry_five(5);
    BOOST_REQUIRE_EQUAL(send(2, &retry_five), 3u);
    BOOST_REQUIRE_EQUAL(retry_five.consulted, 2u);

    // The strategy, not the client, decides when to give up.
    counting_retry_strategy retry_once(1);
    BOOST_REQUIRE_THROW(send(3, &retry_once), std::runtime_error);
    BOOST_REQUIRE_EQUAL(retry_once.consulted, 2u);
}
