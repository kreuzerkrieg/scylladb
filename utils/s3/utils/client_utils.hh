/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#if __has_include(<rapidxml.h>)
#include <rapidxml.h>
#else
#include <rapidxml/rapidxml.hpp>
#endif
#include "seastar/core/iostream.hh"
#include <seastar/core/sstring.hh>
#include <utils/chunked_vector.hh>

namespace s3 {

struct range {
    uint64_t off;
    size_t len;
};

struct tag {
    std::string key;
    std::string value;
    auto operator<=>(const tag&) const = default;
};

using tag_set = std::vector<tag>;

seastar::sstring parse_multipart_upload_id(seastar::sstring& body);
unsigned prepare_multipart_upload_parts(const utils::chunked_vector<seastar::sstring>& etags);
seastar::future<> dump_multipart_upload_parts(seastar::output_stream<char> out, const utils::chunked_vector<seastar::sstring>& etags);
rapidxml::xml_node<>* first_node_of(rapidxml::xml_node<>* root, std::initializer_list<std::string_view> names);
tag_set parse_tagging(seastar::sstring& body);
seastar::sstring parse_multipart_copy_upload_etag(seastar::sstring& body);

} // namespace s3
