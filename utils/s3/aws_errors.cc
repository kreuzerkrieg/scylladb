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

#include "utils/s3/aws_errors.hh"
#include <unordered_map>

namespace aws {
const std::unordered_map<std::string_view, const aws_error> aws_error_map{
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
    {"RequestTimeout", aws_error(aws_error_type::REQUEST_TIMEOUT, retryable::yes)}};

aws_error::aws_error(aws_error_type error_type, retryable is_retryable) : _type(error_type), _is_retryable(is_retryable) {
}

aws_error::aws_error(aws_error_type error_type, std::string&& error_message, retryable is_retryable)
    : _type(error_type), _message(std::move(error_message)), _is_retryable(is_retryable) {
}

aws_error aws_error::parse(seastar::sstring&& body) {
    aws_error ret_val;

    if (body.empty()) {
        return ret_val;
    }

    rapidxml::xml_document<> doc;
    try {
        doc.parse<0>(body.data());
    } catch (const rapidxml::parse_error&) {
        // Most likely not an XML which is possible, just return
        return ret_val;
    }

    const auto* error_node = doc.first_node("Error");
    if (!error_node) {
        error_node = doc.first_node()->first_node("Errors");
        if (error_node) {
            error_node = error_node->first_node("Error");
        }
    }

    if (!error_node) {
        return ret_val;
    }

    const auto* code_node = error_node->first_node("Code");
    const auto* message_node = error_node->first_node("Message");

    if (code_node && message_node) {
        std::string code = code_node->value();
        auto pound_loc = code.find_first_of('#');
        auto colon_loc = code.find_first_of(':');

        if (pound_loc != std::string::npos) {
            code = code.substr(pound_loc + 1);
        } else if (colon_loc != std::string::npos) {
            code = code.substr(0, colon_loc);
        }
        if (aws_error_map.contains(code)) {
            ret_val = aws_error_map.at(code);
        } else {
            ret_val._type = aws_error_type::UNKNOWN;
        }
        ret_val._message = message_node->value();
    } else {
        ret_val._type = aws_error_type::UNKNOWN;
    }
    return ret_val;
}

} // namespace aws
