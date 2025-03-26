/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "multipart_upload.hh"

namespace s3 {

class copy_s3_object final : public multipart_upload {
public:
    copy_s3_object(
        seastar::shared_ptr<client> cln, seastar::sstring source_object, seastar::sstring target_object, std::optional<tag> tag, seastar::abort_source* as);

    seastar::future<> copy();

private:
    seastar::future<> copy_put();

    seastar::future<> copy_multipart(size_t source_size);

    seastar::future<> copy_part(size_t offset, size_t part_size);

    static constexpr size_t _max_copy_part_size = 5_GiB;
    seastar::sstring _source_object;
};

} // s3
