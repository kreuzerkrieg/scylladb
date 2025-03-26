/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "multipart_upload.hh"
#include "utils/input_stream.hh"
#include <filesystem>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

namespace s3 {

// unlike upload_sink and upload_jumbo_sink, do_upload_file reads from the
// specified file, and sends the data read from disk right away to the wire,
// without accumulating them first.
class do_upload_file : private multipart_upload {
    const std::filesystem::path _path;
    size_t _part_size;
    upload_progress& _progress;

    // each time, we read up to transmit size from disk.
    // this is also an option which limits the number of multipart upload tasks.
    //
    // connected_socket::output() uses 8 KiB for its buffer_size, and
    // file_input_stream_options.buffer_size is also 8 KiB, taking the
    // read-ahead into consideration, for maximizing the throughput,
    // we use 64K buffer size.
    static constexpr size_t _transmit_size = 64_KiB;

    static seastar::file_input_stream_options input_stream_options();

    // transmit data from input to output in chunks sized up to unit_size
    static seastar::future<> copy_to(utils::input_stream<char> input, output_stream<char> output, size_t unit_size, upload_progress& progress);

    seastar::future<> upload_part(file f, uint64_t offset, uint64_t part_size);

    // returns pair<num_of_parts, part_size>
    static std::pair<unsigned, size_t> calc_part_size(size_t total_size, size_t part_size);

    seastar::future<> multi_part_upload(file&& f, uint64_t total_size, size_t part_size);

    seastar::future<> put_object(file&& f, uint64_t len);

public:
    do_upload_file(seastar::shared_ptr<client> cln,
                   std::filesystem::path path,
                   seastar::sstring object_name,
                   std::optional<tag> tag,
                   size_t part_size,
                   upload_progress& up,
                   seastar::abort_source* as);

    seastar::future<> upload();
};

} // namespace s3
