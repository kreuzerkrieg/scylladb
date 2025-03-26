/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

namespace s3 {

class upload_sink final : public upload_sink_base {
    memory_data_sink_buffers _bufs;
    future<> maybe_flush() {
        if (_bufs.size() >= aws_minimum_part_size) {
            co_await upload_part(std::move(_bufs));
        }
    }

public:
    upload_sink(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag = {}, seastar::abort_source* as = nullptr)
        : upload_sink_base(std::move(cln), std::move(object_name), std::move(tag), as)
    {}

    virtual future<> put(temporary_buffer<char> buf) override {
        _bufs.put(std::move(buf));
        return maybe_flush();
    }

    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        for (auto&& buf : data) {
            _bufs.put(std::move(buf));
        }
        return maybe_flush();
    }

    virtual future<> flush() override {
        if (_bufs.size() != 0) {
            // This is handy for small objects that are uploaded via the sink. It makes
            // upload happen in one REST call, instead of three (create + PUT + wrap-up)
            if (!upload_started()) {
                s3l.trace("Sink fallback to plain PUT for {}", _object_name);
                co_return co_await _client->put_object(_object_name, std::move(_bufs));
            }

            co_await upload_part(std::move(_bufs));
        }
        if (upload_started()) {
            std::exception_ptr ex;
            try {
                co_await finalize_upload();
            } catch (...) {
                ex = std::current_exception();
            }
            if (ex) {
                co_await abort_upload();
                std::rethrow_exception(ex);
            }
        }
    }
};

} // s3
