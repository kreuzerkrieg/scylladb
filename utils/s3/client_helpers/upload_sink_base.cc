/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "upload_sink_base.hh"

namespace s3 {
future<> upload_sink_base::close() {
    if (upload_started()) {
        s3l.warn("closing incomplete multipart upload -> aborting");
        // If we got here, we need to pick up any background activity as it may
        // still trying to handle successful request and 'this' should remain alive
        //
        // The gate might have been closed by finalize_upload() so need to avoid
        // double close
        if (!_bg_flushes.is_closed()) {
            co_await _bg_flushes.close();
        }
        co_await abort_upload();
    } else {
        s3l.trace("closing multipart upload");
    }
}
} // s3