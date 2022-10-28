/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/exception_utils.hh"

#include <regex>
#include <boost/test/unit_test.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

std::function<bool(const std::exception&)> exception_predicate::make(
        std::function<bool(const std::exception&)> check,
        std::function<sstring(const std::exception&)> err) {
    return [check = std::move(check), err = std::move(err)] (const std::exception& e) {
               const bool status = check(e);
               BOOST_CHECK_MESSAGE(status, err(e));
               return status;
           };
}

std::function<bool(const std::exception&)> exception_predicate::message_contains(
        const sstring& fragment,
        const std::experimental::source_location& loc) {
    return make([=] (const std::exception& e) { return sstring(e.what()).find(fragment) != sstring::npos; },
                [=] (const std::exception& e) {
                    return fmt::format("Message '{}' doesn't contain '{}'\n{}:{}: invoked here",
                                       e.what(), fragment, loc.file_name(), loc.line());
                });
}

std::function<bool(const std::exception&)> exception_predicate::message_equals(
        const sstring& text,
        const std::experimental::source_location& loc) {
    return make([=] (const std::exception& e) { return text == e.what(); },
                [=] (const std::exception& e) {
                    return fmt::format("Message '{}' doesn't equal '{}'\n{}:{}: invoked here",
                                       e.what(), text, loc.file_name(), loc.line());
                });
}

std::function<bool(const std::exception&)> exception_predicate::message_matches(
        const std::string& regex,
        const std::experimental::source_location& loc) {
    return make([=] (const std::exception& e) { return std::regex_search(e.what(), std::regex(regex)); },
                [=] (const std::exception& e) {
                    return fmt::format("Message '{}' doesn't match '{}'\n{}:{}: invoked here",
                                       e.what(), regex, loc.file_name(), loc.line());
                });
}