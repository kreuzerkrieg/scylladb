/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/coroutine.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "utils/exponential_backoff_retry.hh"

using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_exponential_backoff_retry) {
    exponential_backoff_retry exr(10ms, 1000ms);

    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 10);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 20);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 40);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 80);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 160);

    exr.reset();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 10);
}
