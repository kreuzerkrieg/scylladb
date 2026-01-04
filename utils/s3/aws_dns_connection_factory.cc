/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <utility>

#include "aws_dns_connection_factory.hh"
namespace utils::http {
aws_dns_connection_factory::aws_dns_connection_factory(
    std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs)
    : dns_connection_factory(std::move(host), port, use_https, logger, std::move(certs)) {
}

aws_dns_connection_factory::aws_dns_connection_factory(std::string uri, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs)
    : dns_connection_factory(std::move(uri), logger, std::move(certs)) {
}

future<connected_socket> aws_dns_connection_factory::make(abort_source*) {
    co_await _addr_provider.reset();
    co_return co_await connect();
}
} // namespace utils::http
