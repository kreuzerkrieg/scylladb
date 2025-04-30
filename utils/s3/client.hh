/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/client.hh>
#include <filesystem>
#include "utils/lister.hh"
#include "utils/s3/creds.hh"
#include "credentials_providers/aws_credentials_provider_chain.hh"
#include "utils/client_utils.hh"
#include "retryable_http_client.hh"
#include "utils/s3/client_fwd.hh"

using namespace seastar;
class memory_data_sink_buffers;

namespace s3 {

class client : public enable_shared_from_this<client> {
    class multipart_upload;
    class copy_s3_object;
    class upload_sink_base;
    class upload_sink;
    class upload_jumbo_sink;
    class download_source;
    class do_upload_file;
    class readable_file;
    std::string _host;
    endpoint_config_ptr _cfg;
    semaphore _creds_sem;
    timer<seastar::lowres_clock> _creds_invalidation_timer;
    timer<seastar::lowres_clock> _creds_update_timer;
    aws_credentials _credentials;
    aws::aws_credentials_provider_chain _creds_provider_chain;

    struct io_stats {
        uint64_t ops = 0;
        uint64_t bytes = 0;
        std::chrono::duration<double> duration = std::chrono::duration<double>(0);

        void update(uint64_t len, std::chrono::duration<double> lat) {
            ops++;
            bytes += len;
            duration += lat;
        }
    };
    struct group_client {
        aws::retryable_http_client retryable_client;
        io_stats read_stats;
        io_stats write_stats;
        seastar::metrics::metric_groups metrics;
        group_client(std::unique_ptr<http::experimental::connection_factory> f, unsigned max_conn, const aws::retry_strategy& retry_strategy);
        void register_metrics(std::string class_name, std::string host);
    };
    std::unordered_map<seastar::scheduling_group, group_client> _https;
    using global_factory = std::function<shared_ptr<client>(std::string)>;
    global_factory _gf;
    semaphore& _memory;
    std::unique_ptr<aws::retry_strategy> _retry_strategy;

    struct private_tag {};

    future<semaphore_units<>> claim_memory(size_t mem);

    future<> update_credentials_and_rearm();
    future<> authorize(http::request&);
    group_client& find_or_create_client();
    future<> make_request(http::request req, http::experimental::client::reply_handler handle = ignore_reply, std::optional<http::reply::status_type> expected = std::nullopt, seastar::abort_source* = nullptr);
    using reply_handler_ext = noncopyable_function<future<>(group_client&, const http::reply&, input_stream<char>&& body)>;
    future<> make_request(http::request req, reply_handler_ext handle, std::optional<http::reply::status_type> expected = std::nullopt, seastar::abort_source* = nullptr);
    future<> get_object_header(sstring object_name, http::experimental::client::reply_handler handler, seastar::abort_source* = nullptr);
public:

    client(std::string host, endpoint_config_ptr cfg, semaphore& mem, global_factory gf, private_tag, std::unique_ptr<aws::retry_strategy> rs = nullptr);
    static shared_ptr<client> make(std::string endpoint, endpoint_config_ptr cfg, semaphore& memory, global_factory gf = {});

    future<uint64_t> get_object_size(sstring object_name, seastar::abort_source* = nullptr);
    future<stats> get_object_stats(sstring object_name, seastar::abort_source* = nullptr);
    future<tag_set> get_object_tagging(sstring object_name, seastar::abort_source* = nullptr);
    future<> put_object_tagging(sstring object_name, tag_set tagging, seastar::abort_source* = nullptr);
    future<> delete_object_tagging(sstring object_name, seastar::abort_source* = nullptr);
    future<temporary_buffer<char>> get_object_contiguous(sstring object_name, std::optional<range> range = {}, seastar::abort_source* = nullptr);
    future<> put_object(sstring object_name, temporary_buffer<char> buf, seastar::abort_source* = nullptr);
    future<> put_object(sstring object_name, ::memory_data_sink_buffers bufs, seastar::abort_source* = nullptr);
    future<> copy_object(sstring source_object, sstring target_object, std::optional<size_t> part_size = {}, std::optional<tag> tag = {}, seastar::abort_source* = nullptr);
    future<> delete_object(sstring object_name, seastar::abort_source* = nullptr);

    file make_readable_file(sstring object_name, seastar::abort_source* = nullptr);
    data_sink make_upload_sink(sstring object_name, seastar::abort_source* = nullptr);
    data_sink make_upload_jumbo_sink(sstring object_name, std::optional<unsigned> max_parts_per_piece = {}, seastar::abort_source* = nullptr);
    data_source make_download_source(sstring object_name, std::optional<range> range = {}, seastar::abort_source* = nullptr);
    /// upload a file with specified path to s3
    ///
    /// @param path the path to the file
    /// @param object_name object name for the created object in S3
    /// @param tag an optional tag
    /// @param part_size the size of each part of the multipart upload
    future<> upload_file(std::filesystem::path path,
                         sstring object_name,
                         std::optional<tag> tag = {},
                         std::optional<size_t> max_part_size = {},
                         seastar::abort_source* = nullptr);
    future<> upload_file(std::filesystem::path path,
                         sstring object_name,
                         upload_progress& up,
                         seastar::abort_source* = nullptr);

    future<> update_config(endpoint_config_ptr);

    future<> close();

    class bucket_lister;

};

} // s3 namespace
