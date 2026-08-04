/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "default_aws_retry_strategy.hh"
#include "aws_error.hh"
#include <seastar/core/sleep.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>
#include "utils/log.hh"

namespace seastar::http {
extern logging::logger rs_logger;
}

using namespace std::chrono_literals;
using namespace seastar::http;

namespace aws {

static seastar::future<> sleep_before_retry(size_t attempted_retries) {
    if (attempted_retries == 0) {
        return seastar::make_ready_future();
    }
    constexpr size_t scale_factor = 25;
    return seastar::sleep(std::chrono::milliseconds((1UL << attempted_retries) * scale_factor));
}

default_aws_retry_strategy::default_aws_retry_strategy(unsigned max_retries) : _max_retries(max_retries) {
}

seastar::future<bool> default_aws_retry_strategy::should_retry(std::exception_ptr error, unsigned attempted_retries) const {
    auto err = aws_error::from_exception_ptr(error);
    if (attempted_retries >= _max_retries) {
        // Parse the error before the cap check so the message can name the cause.
        // Without it "Retries exhausted" says only that a request died, and a run
        // cannot tell an exhaustion caused by throttling from any other kind --
        // which is the distinction the S3 throttling measurements turn on.
        rs_logger.warn("Retries exhausted. Reason: {}. Retry# {}", err.get_error_message(), attempted_retries);
        co_return false;
    }
    bool should_retry = err.is_retryable() == utils::http::retryable::yes;
    if (should_retry) {
        rs_logger.debug("AWS HTTP client request failed. Reason: {}. Retry# {}", err.get_error_message(), attempted_retries);
        co_await sleep_before_retry(attempted_retries);
    } else {
        rs_logger.warn("AWS HTTP client encountered non-retryable error. Reason: {}. Code: {}. Retry# {}",
                       err.get_error_message(),
                       std::to_underlying(err.get_error_type()),
                       attempted_retries);
    }
    co_return should_retry;
}

} // namespace aws
