/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client_utils.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

static constexpr std::string_view multipart_upload_complete_header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
                                                                     "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";

static constexpr std::string_view multipart_upload_complete_entry = "<Part><ETag>{}</ETag><PartNumber>{}</PartNumber></Part>";

static constexpr std::string_view multipart_upload_complete_trailer = "</CompleteMultipartUpload>";

namespace s3 {

using namespace seastar;
static logging::logger s3l("s3");

sstring parse_multipart_upload_id(sstring& body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse initiate multipart upload response: {}", e.what());
        // The caller is supposed to check the upload-id to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc->first_node("InitiateMultipartUploadResult");
    auto uploadid_node = root_node->first_node("UploadId");
    return uploadid_node->value();
}

unsigned prepare_multipart_upload_parts(const utils::chunked_vector<sstring>& etags) {
    unsigned ret = multipart_upload_complete_header.size();

    unsigned nr = 1;
    for (auto& etag : etags) {
        if (etag.empty()) {
            // 0 here means some part failed to upload, see comment in upload_part()
            // Caller checks it an aborts the multipart upload altogether
            return 0;
        }
        // length of the format string - four braces + length of the etag + length of the number
        ret += multipart_upload_complete_entry.size() - 4 + etag.size() + format("{}", nr).size();
        nr++;
    }
    ret += multipart_upload_complete_trailer.size();
    return ret;
}

future<> dump_multipart_upload_parts(output_stream<char> out, const utils::chunked_vector<sstring>& etags) {
    std::exception_ptr ex;
    try {
        co_await out.write(multipart_upload_complete_header.data(), multipart_upload_complete_header.size());

        unsigned nr = 1;
        for (auto& etag : etags) {
            SCYLLA_ASSERT(!etag.empty());
            co_await out.write(format(multipart_upload_complete_entry.data(), etag, nr));
            nr++;
        }
        co_await out.write(multipart_upload_complete_trailer.data(), multipart_upload_complete_trailer.size());
        co_await out.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

rapidxml::xml_node<>* first_node_of(rapidxml::xml_node<>* root,
                                           std::initializer_list<std::string_view> names) {
    SCYLLA_ASSERT(root);
    auto* node = root;
    for (auto name : names) {
        node = node->first_node(name.data(), name.size());
        if (!node) {
            throw std::runtime_error(fmt::format("'{}' is not found", name));
        }
    }
    return node;
}

tag_set parse_tagging(sstring& body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse tagging response: {}", e.what());
        throw std::runtime_error("cannot parse tagging response");
    }
    tag_set tags;
    auto tagset_node = first_node_of(doc.get(), {"Tagging", "TagSet"});
    for (auto tag_node = tagset_node->first_node("Tag"); tag_node; tag_node = tag_node->next_sibling()) {
        // See https://docs.aws.amazon.com/AmazonS3/latest/API/API_Tag.html,
        // both "Key" and "Value" are required, but we still need to check them.
        auto key = tag_node->first_node("Key");
        if (!key) {
            throw std::runtime_error("'Key' missing in 'Tag'");
        }
        auto value = tag_node->first_node("Value");
        if (!value) {
            throw std::runtime_error("'Value' missing in 'Tag'");
        }
        tags.emplace_back(tag{key->value(), value->value()});
    }
    return tags;
}

sstring parse_multipart_copy_upload_etag(sstring& body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse multipart copy upload response: {}", e.what());
        // The caller is supposed to check the etag to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc->first_node("CopyPartResult");
    auto etag_node = root_node->first_node("ETag");
    return etag_node->value();
}

} // namespace s3
