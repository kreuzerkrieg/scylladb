/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <optional>
#include <seastar/core/shared_ptr.hh>

namespace s3 {

struct endpoint_config {
    unsigned port;
    bool use_https;
    // Amazon Resource Names (ARNs) to access AWS resources
    std::optional<std::string> role_arn;
    std::string region;
    std::strong_ordering operator<=>(const endpoint_config& o) const = default;
};

struct aws_credentials {
    // the access key of the credentials
    std::string access_key_id;
    // the secret key of the credentials
    std::string secret_access_key;
    // the security token, only for session credentials
    std::string session_token;
    // session token expiration
    std::chrono::system_clock::time_point expires_at{std::chrono::system_clock::time_point::min()};
    operator bool() const { return !access_key_id.empty() && !secret_access_key.empty(); }
    std::strong_ordering operator<=>(const aws_credentials& o) const = default;
};

using endpoint_config_ptr = seastar::lw_shared_ptr<endpoint_config>;

} // namespace s3
