/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#if __has_include(<rapidxml.h>)
#include <rapidxml.h>
#else
#include <rapidxml/rapidxml.hpp>
#endif

#include "utils/s3/aws_error.hh"

namespace aws {

aws_error::aws_error(aws_error_type error_type, retryable is_retryable) : _type(error_type), _is_retryable(is_retryable) {
}

aws_error::aws_error(aws_error_type error_type, std::string&& error_message, retryable is_retryable)
    : _type(error_type), _message(std::move(error_message)), _is_retryable(is_retryable) {
}

std::optional<aws_error>  aws_error::parse(seastar::sstring&& body) {
    if (body.empty()) {
        return {};
    }

    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error&) {
        // Most likely not an XML which is possible, just return
        return {};
    }

    const auto* error_node = doc->first_node("Error");
    if (!error_node && doc->first_node()) {
        error_node = doc->first_node()->first_node("Errors");
        if (error_node) {
            error_node = error_node->first_node("Error");
        }
    }

    if (!error_node) {
        return {};
    }

    const auto* code_node = error_node->first_node("Code");
    const auto* message_node = error_node->first_node("Message");
    aws_error ret_val;

    if (code_node && message_node) {
        std::string code = code_node->value();

        if (auto pound_loc = code.find('#'); pound_loc != std::string::npos) {
            code = code.substr(pound_loc + 1);
        } else if (auto colon_loc = code.find(':'); colon_loc != std::string::npos) {
            code = code.substr(0, colon_loc);
        }

        const auto& all_errors = aws_error::get_errors();
        if (auto found = all_errors.find(code); found != all_errors.end()) {
            ret_val = found->second;
        } else {
            ret_val._type = aws_error_type::UNKNOWN;
        }
        ret_val._message = message_node->value();
    } else {
        ret_val._type = aws_error_type::UNKNOWN;
    }
    return ret_val;
}

aws_error aws_error::from_http_code(seastar::http::reply::status_type http_code) {
    aws_error error;
    const auto& all_errors = aws_error::get_errors();
    switch (http_code) {
    case seastar::http::reply::status_type::unauthorized:
    case seastar::http::reply::status_type::forbidden:
        return all_errors.at("AccessDenied");
    case seastar::http::reply::status_type::not_found:
        return all_errors.at("ResourceNotFound");
    case seastar::http::reply::status_type::too_many_requests:
        return all_errors.at("SlowDown");
    case seastar::http::reply::status_type::internal_server_error:
        return all_errors.at("InternalError");
    case seastar::http::reply::status_type::bandwidth_limit_exceeded:
        return all_errors.at("Throttling");
    case seastar::http::reply::status_type::service_unavailable:
        return all_errors.at("ServiceUnavailable");
    case seastar::http::reply::status_type::request_timeout:
    case seastar::http::reply::status_type::page_expired:
    case seastar::http::reply::status_type::login_timeout:
    case seastar::http::reply::status_type::gateway_timeout:
    case seastar::http::reply::status_type::network_connect_timeout:
    case seastar::http::reply::status_type::network_read_timeout:
        return all_errors.at("RequestTimeout");
    default:
        int code_value = static_cast<int>(http_code);
        return {aws_error_type::UNKNOWN, aws::retryable{code_value >= 500 && code_value < 600}};
    }
}

const aws_errors& aws_error::get_errors() {
    static const std::unordered_map<std::string_view, const aws_error> aws_error_map{
        {"IncompleteSignature", aws_error(aws_error_type::INCOMPLETE_SIGNATURE, retryable::no)},
        {"IncompleteSignatureException", aws_error(aws_error_type::INCOMPLETE_SIGNATURE, retryable::no)},
        {"InvalidSignatureException", aws_error(aws_error_type::INVALID_SIGNATURE, retryable::no)},
        {"InvalidSignature", aws_error(aws_error_type::INVALID_SIGNATURE, retryable::no)},
        {"InternalFailureException", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InternalFailure", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InternalServerError", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InternalError", aws_error(aws_error_type::INTERNAL_FAILURE, retryable::yes)},
        {"InvalidActionException", aws_error(aws_error_type::INVALID_ACTION, retryable::no)},
        {"InvalidAction", aws_error(aws_error_type::INVALID_ACTION, retryable::no)},
        {"InvalidClientTokenIdException", aws_error(aws_error_type::INVALID_CLIENT_TOKEN_ID, retryable::no)},
        {"InvalidClientTokenId", aws_error(aws_error_type::INVALID_CLIENT_TOKEN_ID, retryable::no)},
        {"InvalidParameterCombinationException", aws_error(aws_error_type::INVALID_PARAMETER_COMBINATION, retryable::no)},
        {"InvalidParameterCombination", aws_error(aws_error_type::INVALID_PARAMETER_COMBINATION, retryable::no)},
        {"InvalidParameterValueException", aws_error(aws_error_type::INVALID_PARAMETER_VALUE, retryable::no)},
        {"InvalidParameterValue", aws_error(aws_error_type::INVALID_PARAMETER_VALUE, retryable::no)},
        {"InvalidQueryParameterException", aws_error(aws_error_type::INVALID_QUERY_PARAMETER, retryable::no)},
        {"InvalidQueryParameter", aws_error(aws_error_type::INVALID_QUERY_PARAMETER, retryable::no)},
        {"MalformedQueryStringException", aws_error(aws_error_type::MALFORMED_QUERY_STRING, retryable::no)},
        {"MalformedQueryString", aws_error(aws_error_type::MALFORMED_QUERY_STRING, retryable::no)},
        {"MissingActionException", aws_error(aws_error_type::MISSING_ACTION, retryable::no)},
        {"MissingAction", aws_error(aws_error_type::MISSING_ACTION, retryable::no)},
        {"MissingAuthenticationTokenException", aws_error(aws_error_type::MISSING_AUTHENTICATION_TOKEN, retryable::no)},
        {"MissingAuthenticationToken", aws_error(aws_error_type::MISSING_AUTHENTICATION_TOKEN, retryable::no)},
        {"MissingParameterException", aws_error(aws_error_type::MISSING_PARAMETER, retryable::no)},
        {"MissingParameter", aws_error(aws_error_type::MISSING_PARAMETER, retryable::no)},
        {"OptInRequired", aws_error(aws_error_type::OPT_IN_REQUIRED, retryable::no)},
        {"RequestExpiredException", aws_error(aws_error_type::REQUEST_EXPIRED, retryable::yes)},
        {"RequestExpired", aws_error(aws_error_type::REQUEST_EXPIRED, retryable::yes)},
        {"ServiceUnavailableException", aws_error(aws_error_type::SERVICE_UNAVAILABLE, retryable::yes)},
        {"ServiceUnavailableError", aws_error(aws_error_type::SERVICE_UNAVAILABLE, retryable::yes)},
        {"ServiceUnavailable", aws_error(aws_error_type::SERVICE_UNAVAILABLE, retryable::yes)},
        {"RequestThrottledException", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"RequestThrottled", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"ThrottlingException", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"ThrottledException", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"Throttling", aws_error(aws_error_type::THROTTLING, retryable::yes)},
        {"ValidationErrorException", aws_error(aws_error_type::VALIDATION, retryable::no)},
        {"ValidationException", aws_error(aws_error_type::VALIDATION, retryable::no)},
        {"ValidationError", aws_error(aws_error_type::VALIDATION, retryable::no)},
        {"AccessDeniedException", aws_error(aws_error_type::ACCESS_DENIED, retryable::no)},
        {"AccessDenied", aws_error(aws_error_type::ACCESS_DENIED, retryable::no)},
        {"ResourceNotFoundException", aws_error(aws_error_type::RESOURCE_NOT_FOUND, retryable::no)},
        {"ResourceNotFound", aws_error(aws_error_type::RESOURCE_NOT_FOUND, retryable::no)},
        {"UnrecognizedClientException", aws_error(aws_error_type::UNRECOGNIZED_CLIENT, retryable::no)},
        {"UnrecognizedClient", aws_error(aws_error_type::UNRECOGNIZED_CLIENT, retryable::no)},
        {"SlowDownException", aws_error(aws_error_type::SLOW_DOWN, retryable::yes)},
        {"SlowDown", aws_error(aws_error_type::SLOW_DOWN, retryable::yes)},
        {"SignatureDoesNotMatchException", aws_error(aws_error_type::SIGNATURE_DOES_NOT_MATCH, retryable::no)},
        {"SignatureDoesNotMatch", aws_error(aws_error_type::SIGNATURE_DOES_NOT_MATCH, retryable::no)},
        {"InvalidAccessKeyIdException", aws_error(aws_error_type::INVALID_ACCESS_KEY_ID, retryable::no)},
        {"InvalidAccessKeyId", aws_error(aws_error_type::INVALID_ACCESS_KEY_ID, retryable::no)},
        {"RequestTimeTooSkewedException", aws_error(aws_error_type::REQUEST_TIME_TOO_SKEWED, retryable::yes)},
        {"RequestTimeTooSkewed", aws_error(aws_error_type::REQUEST_TIME_TOO_SKEWED, retryable::yes)},
        {"RequestTimeoutException", aws_error(aws_error_type::REQUEST_TIMEOUT, retryable::yes)},
        {"RequestTimeout", aws_error(aws_error_type::REQUEST_TIMEOUT, retryable::yes)},
        {"NoSuchUpload", aws_error(aws_error_type::NO_SUCH_UPLOAD, retryable::no)},
        {"BucketAlreadyOwnedByYou", aws_error(aws_error_type::BUCKET_ALREADY_OWNED_BY_YOU, retryable::no)},
        {"ObjectAlreadyInActiveTierError", aws_error(aws_error_type::OBJECT_ALREADY_IN_ACTIVE_TIER, retryable::no)},
        {"NoSuchBucket", aws_error(aws_error_type::NO_SUCH_BUCKET, retryable::no)},
        {"NoSuchKey", aws_error(aws_error_type::NO_SUCH_KEY, retryable::no)},
        {"ObjectNotInActiveTierError", aws_error(aws_error_type::OBJECT_NOT_IN_ACTIVE_TIER, retryable::no)},
        {"BucketAlreadyExists", aws_error(aws_error_type::BUCKET_ALREADY_EXISTS, retryable::no)},
        {"InvalidObjectState", aws_error(aws_error_type::INVALID_OBJECT_STATE, retryable::no)}};
    return aws_error_map;
}
} // namespace aws
