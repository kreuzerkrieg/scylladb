/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils.hh"

// TODO: move shared things/helpers from encryption.cc to here (?)
namespace encryption {
bool is_aligned(size_t n, size_t a) {
    return (n & (a - 1)) == 0;
}
} // namespace encryption
