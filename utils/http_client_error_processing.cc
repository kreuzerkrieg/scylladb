/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "http_client_error_processing.hh"
#include <seastar/http/exception.hh>
#include <gnutls/gnutls.h>
#include <charconv>
#include <limits>
#include <system_error>

namespace utils::http {

retryable from_http_code(seastar::http::reply::status_type http_code) {
    switch (http_code) {
    case seastar::http::reply::status_type::unauthorized:
    case seastar::http::reply::status_type::forbidden:
    case seastar::http::reply::status_type::not_found:
        return retryable::no;
    case seastar::http::reply::status_type::too_many_requests:
    case seastar::http::reply::status_type::internal_server_error:
    case seastar::http::reply::status_type::bandwidth_limit_exceeded:
    case seastar::http::reply::status_type::service_unavailable:
    case seastar::http::reply::status_type::request_timeout:
    case seastar::http::reply::status_type::page_expired:
    case seastar::http::reply::status_type::login_timeout:
    case seastar::http::reply::status_type::gateway_timeout:
    case seastar::http::reply::status_type::network_connect_timeout:
    case seastar::http::reply::status_type::network_read_timeout:
        return retryable::yes;
    default:
        return retryable{seastar::http::reply::classify_status(http_code) == seastar::http::reply::status_class::server_error};
    }
}

retryable from_system_error(const std::system_error& system_error) {
    switch (system_error.code().value()) {
    case static_cast<int>(std::errc::interrupted):
    case static_cast<int>(std::errc::resource_unavailable_try_again):
    case static_cast<int>(std::errc::timed_out):
    case static_cast<int>(std::errc::connection_aborted):
    case static_cast<int>(std::errc::connection_reset):
    case static_cast<int>(std::errc::connection_refused):
    case static_cast<int>(std::errc::broken_pipe):
    case static_cast<int>(std::errc::network_unreachable):
    case static_cast<int>(std::errc::host_unreachable):
    case static_cast<int>(std::errc::network_down):
    case static_cast<int>(std::errc::network_reset):
    case static_cast<int>(std::errc::no_buffer_space):
    // A reply body that ends before its declared Content-Length. The http client
    // reports that as a clean end of stream, so whoever notices has to raise it,
    // and a fresh request for the same range usually gets it whole.
    case static_cast<int>(std::errc::protocol_error):
    // GNU TLS section. Since we pack gnutls error codes in std::system_error and rethrow it as std::nested_exception we have to handle them here.
    case GNUTLS_E_PREMATURE_TERMINATION:
    case GNUTLS_E_AGAIN:
    case GNUTLS_E_INTERRUPTED:
    case GNUTLS_E_PUSH_ERROR:
    case GNUTLS_E_PULL_ERROR:
    case GNUTLS_E_TIMEDOUT:
    case GNUTLS_E_SESSION_EOF:
    case GNUTLS_E_BAD_COOKIE: // as per RFC6347 section-4.2.1 client should retry
        return retryable::yes;
    default:
        return retryable::no;
    }
}

bool body_ended_early(const seastar::http::reply& rep) {
    // A chunked reply carries no declared length: the counter sits at the sentinel
    // until the body has been read and is zeroed then, so it says nothing here.
    if (rep.left_content_length == std::numeric_limits<size_t>::max()) {
        return false;
    }
    return rep.left_content_length != 0;
}

[[noreturn]] void throw_body_ended_early(std::string_view what) {
    throw std::system_error(std::make_error_code(std::errc::protocol_error), std::string(what));
}

std::optional<answered_range> content_range(const seastar::http::reply& rep) {
    auto i = rep._headers.find("Content-Range");
    if (i == rep._headers.end()) {
        return std::nullopt;
    }
    std::string_view v(i->second);
    constexpr std::string_view unit = "bytes ";
    if (!v.starts_with(unit)) {
        return std::nullopt;
    }
    v.remove_prefix(unit.size());

    auto number = [](std::string_view& in, uint64_t& out) {
        auto [ptr, ec] = std::from_chars(in.data(), in.data() + in.size(), out);
        if (ec != std::errc{}) {
            return false;
        }
        in.remove_prefix(ptr - in.data());
        return true;
    };

    uint64_t first = 0, last = 0;
    if (!number(v, first) || !v.starts_with('-')) {
        return std::nullopt;
    }
    v.remove_prefix(1);
    if (!number(v, last) || !v.starts_with('/')) {
        return std::nullopt;
    }
    v.remove_prefix(1);
    uint64_t total = 0;
    if (!number(v, total)) {
        return answered_range{first, last, std::nullopt};
    }
    return answered_range{first, last, total};
}

bool answered_other_range(const answered_range& answered, uint64_t want_first, uint64_t want_last) {
    if (answered.first != want_first || answered.last > want_last) {
        return true;
    }
    if (answered.last == want_last) {
        return false;
    }
    // Short. Legitimate only when it stops because the object does.
    return !answered.total || answered.last + 1 != *answered.total;
}

} // namespace utils::http
