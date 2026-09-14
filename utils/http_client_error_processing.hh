/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/http/reply.hh>
#include <seastar/util/bool_class.hh>
#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

namespace utils::http {

using retryable = seastar::bool_class<struct is_retryable>;

retryable from_http_code(seastar::http::reply::status_type http_code);

retryable from_system_error(const std::system_error& system_error);

// True when the reply declared a body length that was not fully delivered.
// Only valid once the body has been read to end of stream: stopping early
// leaves the same trace.
bool body_ended_early(const seastar::http::reply& rep);

[[noreturn]] void throw_body_ended_early(std::string_view what);

// DIAGNOSTIC, SCYLLADB-4293. The inclusive byte range a reply says it carries,
// from "Content-Range: bytes <first>-<last>/<size>". This is the only statement
// the server makes about which bytes it sent, and neither client compares it
// with the range it asked for.
struct answered_range {
    uint64_t first;
    uint64_t last;
    // Disengaged for the "bytes <first>-<last>/*" form, where the server does
    // not say how long the object is.
    std::optional<uint64_t> total;
};

// Empty when the header is absent or unparsable, which includes the
// "bytes */<size>" form a 416 carries.
std::optional<answered_range> content_range(const seastar::http::reply& rep);

// True when the reply describes bytes other than the ones asked for. A shorter
// answer is not one of those by itself: a read that asks past the end of an
// object is answered with what exists, which is how both clients read a small
// component with a buffer-sized request.
bool answered_other_range(const answered_range& answered, uint64_t want_first, uint64_t want_last);
} // namespace utils::http
