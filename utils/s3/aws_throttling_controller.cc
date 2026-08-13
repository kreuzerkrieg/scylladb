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
#include <chrono>

namespace s3 {

extern logging::logger s3l;

// Holds the request back for the remainder of a freeze, and otherwise admits it
// immediately. Loops rather than sleeping once, because a sleep may wake early and
// this wait is the only thing that holds a request back.
seastar::future<> aws_throttling_controller::acquire(seastar::abort_source* as) {
    while (true) {
        const auto now = seastar::lowres_clock::now();
        if (now >= _frozen_until) {
            co_return;
        }
        const auto d = _frozen_until - now;
        if (as) {
            co_await seastar::sleep_abortable(d, *as);
        } else {
            co_await seastar::sleep(d);
        }
    }
}

void aws_throttling_controller::on_throttled() {
    ++_throttles;

    // Stop admitting for a short interval.
    const auto now = seastar::lowres_clock::now();
    if (now < _frozen_until) {
        // Already frozen. Extending on every response would never let go.
        return;
    }
    if (_last_freeze_end != seastar::lowres_clock::time_point{} && now - _last_freeze_end < freeze_min_gap) {
        // Inside the quiet gap that bounds the duty cycle.
        return;
    }

    const auto d = freeze_duration;
    _frozen_until = now + d;
    _last_freeze_end = _frozen_until;
    ++_freezes;

    // Countable through the send_freezes metric, so this is context for a human
    // reading a log rather than the measurement itself.
    s3l.info("froze sending for {} ms after a throttling response (freeze #{})",
             std::chrono::duration_cast<std::chrono::milliseconds>(d).count(),
             _freezes);
}

} // namespace s3
