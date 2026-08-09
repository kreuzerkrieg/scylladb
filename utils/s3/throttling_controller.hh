/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <cstdint>

namespace s3 {

// Per-shard client-side send-rate limiter. acquire() is awaited before a request
// is dispatched; the on_*() methods report how the response came back.
class throttling_controller {
public:
    virtual ~throttling_controller() = default;

    // Waits until the client may send. With an abort source the wait resolves
    // with seastar::sleep_aborted when it is triggered.
    virtual seastar::future<> acquire(seastar::abort_source* as) = 0;

    // Outcome of a completed request. Only a throttling response pulls the send
    // rate down; any other failure moves it as a success does, since it says
    // nothing about the rate.
    virtual void on_throttled() = 0;
    virtual void on_success() = 0;
    virtual void on_error_not_throttled() = 0;

    // For metrics.
    virtual bool enabled() const = 0;
    virtual double fill_rate() const = 0;
    virtual double measured_tx_rate() const = 0;
    virtual uint64_t throttles() const = 0;
};

} // namespace s3
