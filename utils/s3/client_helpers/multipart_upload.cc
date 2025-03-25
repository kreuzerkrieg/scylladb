/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "multipart_upload.hh"

namespace s3 {
bool multipart_upload::upload_started() const noexcept {
    return !_upload_id.empty();
}
multipart_upload::multipart_upload(seastar::shared_ptr<client> cln, seastar::sstring object_name, std::optional<tag> tag, seastar::abort_source* as)
    : _client(std::move(cln)), _object_name(std::move(object_name)), _tag(std::move(tag)), _as(as) {
}
} // namespace s3
