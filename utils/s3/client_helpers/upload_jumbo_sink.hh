/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

namespace s3 {

class upload_jumbo_sink final : public upload_sink_base {
    static constexpr tag piece_tag = { .key = "kind", .value = "piece" };

    const unsigned _maximum_parts_in_piece;
    std::unique_ptr<upload_sink> _current;

    future<> maybe_flush() {
        if (_current->parts_count() >= _maximum_parts_in_piece) {
            auto next = std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count() + 1), piece_tag);
            co_await upload_part(std::exchange(_current, std::move(next)));
            s3l.trace("Initiated {} piece (upload_id {})", parts_count(), _upload_id);
        }
    }

public:
    upload_jumbo_sink(shared_ptr<client> cln, sstring object_name, std::optional<unsigned> max_parts_per_piece, seastar::abort_source* as)
        : upload_sink_base(std::move(cln), std::move(object_name), std::nullopt, as)
        , _maximum_parts_in_piece(max_parts_per_piece.value_or(aws_maximum_parts_in_piece))
        , _current(std::make_unique<upload_sink>(_client, format("{}_{}", _object_name, parts_count()), piece_tag))
    {}

    virtual future<> put(temporary_buffer<char> buf) override {
        co_await _current->put(std::move(buf));
        co_await maybe_flush();
    }

    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        co_await _current->put(std::move(data));
        co_await maybe_flush();
    }

    virtual future<> flush() override {
        if (_current) {
            co_await upload_part(std::exchange(_current, nullptr));
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

    virtual future<> close() override {
        if (_current) {
            co_await _current->close();
            _current.reset();
        }
        co_await upload_sink_base::close();
    }
};

} // s3
