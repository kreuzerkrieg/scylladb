/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

namespace s3 {

class readable_file : public file_impl {
    shared_ptr<client> _client;
    sstring _object_name;
    std::optional<stats> _stats;
    seastar::abort_source* _as;

    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation on s3 readable file");
    }

    future<> maybe_update_stats() {
        if (_stats) {
            return make_ready_future<>();
        }

        return _client->get_object_stats(_object_name).then([this] (auto st) {
            _stats = std::move(st);
            return make_ready_future<>();
        });
    }

public:
    readable_file(shared_ptr<client> cln, sstring object_name, seastar::abort_source* as = nullptr)
        : _client(std::move(cln))
        , _object_name(std::move(object_name))
        , _as(as)
    {
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override { unsupported(); }

    virtual future<> flush(void) override { return make_ready_future<>(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { return make_ready_future<>(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { return make_ready_future<>(); }

    class readable_file_handle_impl final : public file_handle_impl {
        client::handle _h;
        sstring _object_name;

    public:
        readable_file_handle_impl(client::handle h, sstring object_name)
                : _h(std::move(h))
                , _object_name(std::move(object_name))
        {}

        virtual std::unique_ptr<file_handle_impl> clone() const override {
            return std::make_unique<readable_file_handle_impl>(_h, _object_name);
        }

        virtual shared_ptr<file_impl> to_file() && override {
            // TODO: cannot traverse abort source across shards.
            return make_shared<readable_file>(std::move(_h).to_client(), std::move(_object_name), nullptr);
        }
    };

    virtual std::unique_ptr<file_handle_impl> dup() override {
        return std::make_unique<readable_file_handle_impl>(client::handle(*_client), _object_name);
    }

    virtual future<uint64_t> size(void) override {
        return _client->get_object_size(_object_name);
    }

    virtual future<struct stat> stat(void) override {
        co_await maybe_update_stats();
        struct stat ret {};
        ret.st_nlink = 1;
        ret.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        ret.st_size = _stats->size;
        ret.st_blksize = 1 << 10; // huh?
        ret.st_blocks = _stats->size >> 9;
        // objects are immutable on S3, therefore we can use Last-Modified to set both st_mtime and st_ctime
        ret.st_mtime = _stats->last_modified;
        ret.st_ctime = _stats->last_modified;
        co_return ret;
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        co_await maybe_update_stats();
        if (pos >= _stats->size) {
            co_return 0;
        }

        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, len }, _as);
        std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
        co_return buf.size();
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        co_await maybe_update_stats();
        if (pos >= _stats->size) {
            co_return 0;
        }

        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, utils::iovec_len(iov) }, _as);
        uint64_t off = 0;
        for (auto& v : iov) {
            auto sz = std::min(v.iov_len, buf.size() - off);
            if (sz == 0) {
                break;
            }
            std::copy_n(buf.get() + off, sz, reinterpret_cast<uint8_t*>(v.iov_base));
            off += sz;
        }
        co_return off;
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override {
        co_await maybe_update_stats();
        if (offset >= _stats->size) {
            co_return temporary_buffer<uint8_t>();
        }

        auto buf = co_await _client->get_object_contiguous(_object_name, range{ offset, range_size }, _as);
        co_return temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), buf.size(), buf.release());
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }
};
} // s3
