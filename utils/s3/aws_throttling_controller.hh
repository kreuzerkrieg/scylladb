/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/s3/creds.hh"
#include "utils/s3/default_aws_retry_strategy.hh"
#include "utils/s3/throttling_controller.hh"

#include <seastar/core/lowres_clock.hh>
#include <chrono>
#include <cstdint>

namespace s3 {

// Holds sending back for a short interval after the endpoint refuses a request,
// and bounds how much of the client's work may be retries.
// Single-shard, so no locking.
class aws_throttling_controller final : public throttling_controller {
    // On a throttling response the controller stops admitting altogether for a
    // short interval. The duration is drawn per freeze so that controllers refused
    // at the same moment do not all resume at the same moment -- the trigger is
    // correlated across a fleet, since a 503 is effectively a broadcast.
    static constexpr seastar::lowres_clock::duration freeze_min = std::chrono::milliseconds(3000);
    static constexpr seastar::lowres_clock::duration freeze_max = std::chrono::milliseconds(5000);

    // Minimum quiet period after a freeze. The trigger fires per throttling response
    // and an episode delivers hundreds per second, so without this the controller
    // would stay frozen for the whole episode. Caps the duty cycle at roughly
    // freeze_max / (freeze_max + gap).
    static constexpr seastar::lowres_clock::duration freeze_min_gap = std::chrono::milliseconds(7000);

    seastar::lowres_clock::time_point _frozen_until{};
    seastar::lowres_clock::time_point _last_freeze_end{};
    uint64_t _freezes = 0;

    uint64_t _throttles = 0; // throttling responses observed, for metrics only

    // Client-wide retry budget: one unit per admitted retry, one returned per
    // success, so a request that retried and then succeeded costs nothing and the
    // pool only drains for retries that end in failure. Sized as the connection
    // budget times the retry depth, since every in-flight request holds a connection
    // and any request class may retry -- large enough that a full complement of
    // requests can spend its whole retry allowance before the budget binds.
    static constexpr unsigned initial_retry_quota = endpoint_config::default_connections_per_shard * aws::default_aws_retry_strategy::default_max_retries;

    unsigned _retry_quota = initial_retry_quota;
    uint64_t _retry_quota_denials = 0;

public:
    seastar::future<> acquire(seastar::abort_source* as) override;
    void on_throttled() override;
    void on_success() override;

    bool try_acquire_retry_quota() override;

    uint64_t throttles() const override { return _throttles; }
    uint64_t freezes() const override { return _freezes; }
    uint64_t retry_quota_denials() const override { return _retry_quota_denials; }
};

} // namespace s3
