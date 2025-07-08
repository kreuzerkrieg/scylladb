/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <chrono>
#include <seastar/core/format.hh>
#include <string>

template <typename clock = std::chrono::steady_clock>
typename clock::time_point iso8601ts_to_timepoint(const std::string& iso8601_ts) {
    std::istringstream in(iso8601_ts);
    std::chrono::utc_clock::time_point utc_tp;

    in >> std::chrono::parse("%FT%TZ", utc_tp);
    if (in.fail())
        throw std::runtime_error(seastar::format("Failed to parse ISO 8601 timestamp {} as UTC time", iso8601_ts));

    return clock::now() + (utc_tp - std::chrono::utc_clock::now());
}
