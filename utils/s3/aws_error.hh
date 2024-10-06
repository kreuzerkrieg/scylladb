/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>
#include <seastar/util/bool_class.hh>
#include <string>
#include <string_view>
#include <unordered_map>

namespace aws {

enum class aws_error_type : uint8_t {
    INCOMPLETE_SIGNATURE = 0,
    INTERNAL_FAILURE = 1,
    INVALID_ACTION = 2,
    INVALID_CLIENT_TOKEN_ID = 3,
    INVALID_PARAMETER_COMBINATION = 4,
    INVALID_QUERY_PARAMETER = 5,
    INVALID_PARAMETER_VALUE = 6,
    MISSING_ACTION = 7,
    MISSING_AUTHENTICATION_TOKEN = 8,
    MISSING_PARAMETER = 9,
    OPT_IN_REQUIRED = 10,
    REQUEST_EXPIRED = 11,
    SERVICE_UNAVAILABLE = 12,
    THROTTLING = 13,
    VALIDATION = 14,
    ACCESS_DENIED = 15,
    RESOURCE_NOT_FOUND = 16,
    UNRECOGNIZED_CLIENT = 17,
    MALFORMED_QUERY_STRING = 18,
    SLOW_DOWN = 19,
    REQUEST_TIME_TOO_SKEWED = 20,
    INVALID_SIGNATURE = 21,
    SIGNATURE_DOES_NOT_MATCH = 22,
    INVALID_ACCESS_KEY_ID = 23,
    REQUEST_TIMEOUT = 24,
    NOT_INITIALIZED = 25,
    MEMORY_ALLOCATION = 26,
    NETWORK_CONNECTION = 99,
    UNKNOWN = 100,
    CLIENT_SIGNING_FAILURE = 101,
    USER_CANCELLED = 102,
    ENDPOINT_RESOLUTION_FAILURE = 103,
    SERVICE_EXTENSION_START_RANGE = 128,
    // S3 specific
    BUCKET_ALREADY_EXISTS = 129,
    BUCKET_ALREADY_OWNED_BY_YOU = 130,
    INVALID_OBJECT_STATE = 131,
    NO_SUCH_BUCKET = 132,
    NO_SUCH_KEY = 133,
    NO_SUCH_UPLOAD = 134,
    OBJECT_ALREADY_IN_ACTIVE_TIER = 135,
    OBJECT_NOT_IN_ACTIVE_TIER = 136,
    // No error set
    OK = 255
};

class aws_error;
using retryable = seastar::bool_class<struct is_retryable>;
using aws_errors = std::unordered_map<std::string_view, const aws_error>;

class aws_error {
    aws_error_type _type{aws_error_type::OK};
    std::string _message;
    retryable _is_retryable{retryable::no};

public:
    aws_error() = default;
    aws_error(aws_error_type error_type, retryable is_retryable);
    aws_error(aws_error_type error_type, std::string&& error_message, retryable is_retryable);
    [[nodiscard]] const std::string& get_error_message() const { return _message; }
    [[nodiscard]] aws_error_type get_error_type() const { return _type; }
    [[nodiscard]] retryable is_retryable() const { return _is_retryable; }
    static std::optional<aws_error> parse(seastar::sstring&& body);
    static aws_error from_http_code(seastar::http::reply::status_type http_code);
    static const aws_errors& get_errors();
};

class aws_exception : public std::exception {
private:
  aws_error _error;

public:
  explicit aws_exception(const aws_error& error) noexcept : _error(error) {}
  explicit aws_exception(aws_error&& error) noexcept : _error(std::move(error)) {}

  const char* what() const noexcept override { return _error.get_error_message().c_str(); }

  const aws_error& error() const noexcept { return _error; }
};

} // namespace aws
