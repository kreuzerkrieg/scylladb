/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

namespace s3 {

class upload_sink_base : public multipart_upload, public data_sink_impl {
public:
    upload_sink_base(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag, seastar::abort_source* as)
        : multipart_upload(std::move(cln), std::move(object_name), std::move(tag), as)
    {
    }

    virtual future<> put(net::packet) override {
        throw_with_backtrace<std::runtime_error>("s3 put(net::packet) unsupported");
    }

    virtual future<> close() override;

    virtual size_t buffer_size() const noexcept override {
        return 128 * 1024;
    }

};

} // s3
