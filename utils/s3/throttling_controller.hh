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

// Per-shard client-side send brake. acquire() is awaited before a request is
// dispatched and holds it back while the endpoint is refusing us; the on_*()
// methods report how the response came back.
class throttling_controller {
public:
    virtual ~throttling_controller() = default;

    // Waits until the client may send. With an abort source the wait resolves
    // with seastar::sleep_aborted when it is triggered.
    virtual seastar::future<> acquire(seastar::abort_source* as) = 0;

    // Outcome of a completed request. on_throttled() means the endpoint asked us
    // to slow down; on_success() returns a unit of retry budget.
    virtual void on_throttled() = 0;
    virtual void on_success() = 0;

    // Takes one unit of the client-wide retry budget, which bounds how much of
    // the client's work may be retries. False means the budget is spent and the
    // caller must not retry. Units are returned by on_success().
    virtual bool try_acquire_retry_quota() = 0;

    // For metrics.
    virtual uint64_t throttles() const = 0;
    // Times sending was held back after a throttling response.
    virtual uint64_t freezes() const = 0;
    // Retries the budget refused.
    virtual uint64_t retry_quota_denials() const = 0;
};

} // namespace s3
