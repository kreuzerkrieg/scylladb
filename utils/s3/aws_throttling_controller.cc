/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/s3/aws_throttling_controller.hh"

#include "utils/log.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <algorithm>
#include <chrono>
#include <random>

namespace s3 {

extern logging::logger s3l;

// Uniform in [lo, hi].
static seastar::lowres_clock::duration freeze_duration(seastar::lowres_clock::duration lo, seastar::lowres_clock::duration hi) {
    thread_local std::mt19937 engine{std::random_device{}()};
    std::uniform_int_distribution dist{lo.count(), hi.count()};
    return seastar::lowres_clock::duration(dist(engine));
}

// Holds the request back for the remainder of a freeze, and otherwise admits it
// immediately.
seastar::future<> aws_throttling_controller::acquire(seastar::abort_source* as) {
    auto now = seastar::lowres_clock::now();
    if (now >= _frozen_until) {
        co_return;
    }
    auto d = _frozen_until - now;
    if (as) {
        co_await seastar::sleep_abortable(d, *as);
    } else {
        co_await seastar::sleep(d);
    }
}

void aws_throttling_controller::on_throttled() {
    ++_throttles;

    const auto now = seastar::lowres_clock::now();
    if (now < _frozen_until) {
        // Already frozen. Extending on every response would never let go.
        return;
    }
    if (_last_freeze_end != seastar::lowres_clock::time_point{} && now - _last_freeze_end < freeze_min_gap) {
        // Inside the quiet gap that bounds the duty cycle.
        return;
    }

    const auto d = freeze_duration(freeze_min, freeze_max);
    _frozen_until = now + d;
    _last_freeze_end = _frozen_until;
    ++_freezes;

    // warn, because the s3 logger runs at warn by default and this needs to be
    // countable in a run. freeze_min_gap bounds how often it can fire.
    s3l.warn("froze sending for {} ms after a throttling response (freeze #{})",
             std::chrono::duration_cast<std::chrono::milliseconds>(d).count(),
             _freezes);
}

void aws_throttling_controller::on_success() {
    // One unit back per success, whatever the request spent getting there. A request
    // that retried once is therefore net zero, while one that retried repeatedly stays
    // net negative even though it succeeded -- retry-heavy traffic keeps draining the
    // pool, which is what makes the budget bite while an episode is still going on.
    _retry_quota = std::min(_retry_quota + 1, initial_retry_quota);
}

bool aws_throttling_controller::try_acquire_retry_quota() {
    if (_retry_quota == 0) {
        ++_retry_quota_denials;
        return false;
    }
    --_retry_quota;
    return true;
}

} // namespace s3
