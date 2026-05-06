/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/s3/object_storage_file_impl.hh"

#include <ranges>
#include <seastar/core/coroutine.hh>
#include <seastar/util/backtrace.hh>

#include "utils/assert.hh"
#include "utils/s3/client.hh"

static size_t iovec_len(const std::vector<iovec>& iov) {
    return std::ranges::fold_left(iov, 0ul,
        [](size_t acc, const iovec& v) { return acc + v.iov_len; });
}

namespace s3 {

// Private helpers

[[noreturn]] void object_storage_file_impl::unsupported(std::string_view operation) {
    throw_with_backtrace<std::logic_error>(format("unsupported operation '{}' on object storage file", operation));
}

future<> object_storage_file_impl::maybe_update_stats() const {
    if (_stats) {
        return make_ready_future<>();
    }

    return _client->get_object_stats(_object_name).then([this](auto st) {
        _stats = std::move(st);
        return make_ready_future<>();
    });
}

future<const stats&> object_storage_file_impl::native_stats() const {
    co_await maybe_update_stats();
    co_return *_stats;
}

future<> object_storage_file_impl::ensure_input_stream(uint64_t pos) {
    if (_read_stream && _read_pos == pos) {
        co_return;
    }
    // Position mismatch or no stream — (re)create from the requested offset
    if (_read_stream) {
        co_await _read_stream->close();
        _read_stream.reset();
    }
    auto file_size = (co_await native_stats()).size;
    if (pos >= file_size) {
        co_return;
    }
    auto ds = _client->make_chunked_download_source(_object_name, range{pos, file_size - pos}, _as);
    _read_stream.emplace(input_stream<char>(std::move(ds)));
    _read_pos = pos;
}

future<temporary_buffer<char>> object_storage_file_impl::read_from_stream(uint64_t pos, size_t len) {
    co_await ensure_input_stream(pos);
    if (!_read_stream) {
        co_return temporary_buffer<char>();
    }
    auto buf = co_await _read_stream->read_exactly(len);
    _read_pos += buf.size();
    co_return buf;
}

future<> object_storage_file_impl::ensure_output_stream() {
    if (!_write_stream) {
        auto ds = _client->make_upload_sink(_object_name, _as);
        _write_stream.emplace(output_stream<char>(std::move(ds), 128 * 1024));
        _write_pos = 0;
    }
    return make_ready_future<>();
}

future<size_t> object_storage_file_impl::write_to_stream(uint64_t pos, const char* data, size_t len) {
    if (pos != _write_pos) {
        throw_with_backtrace<std::logic_error>(
            format("non-sequential write at pos {} (expected {}): random writes not supported on object storage", pos, _write_pos));
    }
    co_await ensure_output_stream();
    co_await _write_stream->write(data, len);
    _write_pos += len;
    co_return len;
}

// Public interface

object_storage_file_impl::object_storage_file_impl(shared_ptr<client> cln, sstring object_name, seastar::abort_source* as)
    : _client(std::move(cln)), _object_name(std::move(object_name)), _as(as) {
}

future<size_t> object_storage_file_impl::write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) {
    return write_to_stream(pos, reinterpret_cast<const char*>(buffer), len);
}

future<size_t> object_storage_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) {
    if (pos != _write_pos) {
        throw_with_backtrace<std::logic_error>(
            format("non-sequential write at pos {} (expected {}): random writes not supported on object storage", pos, _write_pos));
    }
    co_await ensure_output_stream();
    size_t total = 0;
    for (auto& [iov_base, iov_len] : iov) {
        co_await _write_stream->write(reinterpret_cast<const char*>(iov_base), iov_len);
        total += iov_len;
    }
    _write_pos += total;
    co_return total;
}

future<size_t> object_storage_file_impl::read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) {
    auto buf = co_await read_from_stream(pos, len);
    SCYLLA_ASSERT(buf.size() <= len);
    std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
    co_return buf.size();
}

future<size_t> object_storage_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) {
    auto total_len = iovec_len(iov);
    auto buf = co_await read_from_stream(pos, total_len);
    SCYLLA_ASSERT(buf.size() <= total_len);
    size_t off = 0;
    for (const auto& [iov_base, iov_len] : iov) {
        auto sz = std::min(iov_len, buf.size() - off);
        if (sz == 0) {
            break;
        }
        std::copy_n(buf.get() + off, sz, reinterpret_cast<uint8_t*>(iov_base));
        off += sz;
    }
    co_return off;
}

future<temporary_buffer<uint8_t>> object_storage_file_impl::dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) {
    auto buf = co_await read_from_stream(offset, range_size);
    SCYLLA_ASSERT(buf.size() <= range_size);
    co_return temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), buf.size(), buf.release());
}

future<> object_storage_file_impl::flush() {
    if (_write_stream) {
        co_await _write_stream->flush();
    }
}

future<struct stat> object_storage_file_impl::stat() {
    const auto& st = co_await native_stats();
    struct stat ret{};
    ret.st_nlink = 1;
    ret.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    ret.st_size = static_cast<off_t>(st.size);
    ret.st_blksize = 1 << 10;
    ret.st_blocks = static_cast<blkcnt_t>(st.size >> 9);
    // objects are immutable on S3, therefore we can use Last-Modified to set both st_mtime and st_ctime
    ret.st_mtime = st.last_modified;
    ret.st_ctime = st.last_modified;
    co_return ret;
}

future<> object_storage_file_impl::truncate(uint64_t length) {
    unsupported(__FUNCTION__);
}

future<> object_storage_file_impl::discard(uint64_t offset, uint64_t length) {
    return make_ready_future<>();
}

future<> object_storage_file_impl::allocate(uint64_t position, uint64_t length) {
    return make_ready_future<>();
}

future<uint64_t> object_storage_file_impl::size() {
    co_return (co_await native_stats()).size;
}

future<> object_storage_file_impl::close() {
    if (_write_stream) {
        co_await _write_stream->close();
        _write_stream.reset();
    }
    if (_read_stream) {
        co_await _read_stream->close();
        _read_stream.reset();
    }
}

std::unique_ptr<file_handle_impl> object_storage_file_impl::dup() {
    unsupported(__FUNCTION__);
}

subscription<directory_entry> object_storage_file_impl::list_directory(std::function<future<>(directory_entry de)> next) {
    unsupported(__FUNCTION__);
}

} // namespace s3
