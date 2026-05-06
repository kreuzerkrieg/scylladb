/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "utils/s3/client.hh"

namespace s3 {

class object_storage_file_impl final : public file_impl {
    shared_ptr<client> _client;
    sstring _object_name;
    mutable std::optional<stats> _stats;
    seastar::abort_source* const _as;

    // Stream-based sequential read state
    std::optional<input_stream<char>> _read_stream;
    uint64_t _read_pos = 0;

    // Stream-based sequential write state
    std::optional<output_stream<char>> _write_stream;
    uint64_t _write_pos = 0;

    [[noreturn]] static void unsupported(std::string_view operation);
    future<> maybe_update_stats() const;
    future<const stats&> native_stats() const;
    future<> ensure_input_stream(uint64_t pos);
    future<temporary_buffer<char>> read_from_stream(uint64_t pos, size_t len);
    future<> ensure_output_stream();
    future<size_t> write_to_stream(uint64_t pos, const char* data, size_t len);

public:
    object_storage_file_impl(shared_ptr<client> cln, sstring object_name, seastar::abort_source* as = nullptr);

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override;
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override;
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override;
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override;
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override;

    future<> flush() override;
    future<struct stat> stat() override;
    future<> truncate(uint64_t length) override;
    future<> discard(uint64_t offset, uint64_t length) override;
    future<> allocate(uint64_t position, uint64_t length) override;
    future<uint64_t> size() override;
    future<> close() override;
    std::unique_ptr<file_handle_impl> dup() override;
    subscription<directory_entry> list_directory(std::function<future<>(directory_entry de)> next) override;
};

} // namespace s3
