# AGENTS.md — ScyllaDB AI Coding Agent Guide

## Project Overview
ScyllaDB is a high-performance distributed NoSQL database (C++23, Seastar framework), API-compatible with Apache Cassandra (CQL) and Amazon DynamoDB (Alternator). Core values: **performance, correctness, readability**.

## Architecture — Key Components
- **`cql3/`** — CQL frontend: parser, grammar (`Cql.g`), statements, query processor
- **`alternator/`** — DynamoDB-compatible API layer
- **`service/storage_proxy.cc`** — Coordinator: routes reads/writes, interacts with `messaging_service` (RPC), `cdc`, `view`
- **`replica/`** — Replica-side: `database` and `table` classes (data storage)
- **`raft/`** — Raft consensus for topology/schema/metadata (replaces Gossip); `service/raft/` for Scylla-specific Raft integration
- **`sstables/`** — On-disk storage format (Sorted String Tables)
- **`compaction/`** — Compaction strategies and manager
- **`mutation/`** — Core data model: writes are mutations (timestamped diffs, combinable out-of-order)
- **`locator/`** — Replication strategies, tablets
- **`message/`** — Inter-node RPC; `idl/` defines message schemas compiled by `idl-compiler.py`
- **`seastar/`** — Git submodule: async framework (futures/promises, shared-nothing per-core architecture)
- **`schema/`** — Schema/metadata definitions (keyspaces, tables)
- **`dht/`** — Distributed hash table, token ring partitioning
- **`gms/`** — Gossip protocol (legacy, being replaced by Raft)
- **`vector_search/`** — Vector search client and filtering for vector-based queries
- **`tasks/`** — Task manager for internal background tasks (compaction, repair, etc.)
- **`rust/`** — Rust interop via CXX bridge (e.g., UDF support); see `docs/dev/rust.md`

Data flow: `cql3`/`alternator` → `storage_proxy` → `messaging_service` (RPC) → `replica/database` → `sstables`

## Build System

### configure.py + Ninja (primary)
```bash
./configure.py --mode=dev          # Configure (dev/debug/release/sanitize)
ninja build/dev/scylla             # Build Scylla binary (sufficient for Python tests)
ninja dev-build                    # Build everything including tests
ninja build/dev/test/boost/<name>  # Build specific C++ test binary
```

### CMake (alternative)
```bash
cmake -B build -DCMAKE_BUILD_TYPE=Dev    # Build types: Dev, Debug, RelWithDebInfo, Sanitize, Coverage
cmake --build build --target scylla      # Build Scylla binary
cmake --build build                      # Build everything
```
CMake also supports multi-config generators (e.g., Ninja Multi-Config). When adding/removing source files, update both `configure.py` and `CMakeLists.txt`. CMake is well-suited for IDE integration (CLion, etc.).

### Common notes
- Source files and targets are tracked in `configure.py` (and `CMakeLists.txt`) — update when adding/removing files
- `test.py` does **not** auto-rebuild; you must build before running tests

### Rebuilding tests
- Many C++ tests share composite binaries (e.g., `combined_tests` in `test/boost/` contains multiple test files)
- To find which binary contains a test, check `configure.py` (primary source) or `test/<suite>/CMakeLists.txt`
- Rebuild a specific test binary: `ninja build/<mode>/test/<suite>/<binary_name>`
- Examples:
  - `ninja build/dev/test/boost/combined_tests` (contains `group0_voter_calculator_test.cc` and others)
  - `ninja build/dev/test/raft/replication_test` (standalone Raft test)

## Running Tests
```bash
./test.py --mode=dev test/boost/memtable_test.cc                    # C++ test file
./test.py --mode=dev test/boost/memtable_test.cc::test_case_name    # Single C++ test case
./test.py --mode=dev test/cqlpy/test_json.py                        # Python test file
./test.py --mode=dev test/cqlpy/test_json.py::test_function_name    # Single Python test
./test.py --mode=dev test/alternator/                                # All tests in directory
```
- Add `--no-gather-metrics` if cgroup permission errors occur
- New tests: validate stability with `--repeat 100 --max-failures 1`
- For Python tests, only `ninja build/dev/scylla` is needed (not full build)

## Test Suites
| Directory | Type | Description |
|-----------|------|-------------|
| `test/boost/` | C++ (Boost.Test) | Unit tests; white-box, internal API testing |
| `test/raft/` | C++ | Raft consensus unit tests |
| `test/unit/` | C++ | Stress and memory allocation tests (LSA, row cache) |
| `test/vector_search/` | C++ (Boost.Test) | Vector search client and filtering tests |
| `test/ldap/` | C++ | LDAP authentication/authorization tests |
| `test/cqlpy/` | Python (pytest) | Single-node CQL black-box tests |
| `test/alternator/` | Python (pytest) | Single-node DynamoDB API tests |
| `test/topology*/`, `test/cluster/` | Python (pytest) | Multi-node cluster tests |
| `test/nodetool/` | Python (pytest) | Nodetool command tests |
| `test/rest_api/` | Python (pytest) | Scylla REST API tests |
| `test/scylla_gdb/` | Python (pytest) | Tests for `scylla-gdb.py` helper script |
| `test/cql/` | CQL approval tests | Pre-recorded CQL input/output comparison |
| `test/perf/` | C++ | Microbenchmarks |

## Code Conventions
- **Seastar namespace**: `seastarx.hh` imports `using namespace seastar;` — do **not** prefix Seastar symbols with `seastar::`
- **Coding style**: [Seastar coding style](https://github.com/scylladb/seastar/blob/master/coding-style.md) — snake_case, 4-space indent
- **Headers must be self-contained**: each header compilable independently; verify with `ninja dev-headers`
- **Commit messages**: `module: short description` format (e.g., `sstables: close fd on error`)
- **Comments**: explain "why", not "what"; code should be self-documenting via clear naming
- **Prefer standard library** over custom implementations; add complexity only when clearly justified
- **Question requests**: don't blindly implement — evaluate trade-offs, identify issues, and suggest better alternatives when appropriate
- **Concurrency**: all background work must have `stop()`/`close()` to await completion; bound memory usage of concurrent ops
- **Hot paths**: avoid allocations, unbounded loops without preemption yields; use `seastar::future<>` properly
- **Invariant checking**: assert for critical invariants, throw for recoverable ones, log for ignorable

## Test Philosophy
- **No sleeps** — use condition-based waiting; sleeps cause flakiness and slow tests
- **Deterministic** — avoid random inputs; tests must be repeatable
- **Focused** — unit tests should ideally test one thing and one thing only
- **Minimal resources** — prefer single-node tests when sufficient
- **Bug fix tests** — must reference the issue (GitHub/JIRA) in comments, and demonstrate failure before the fix
- **Debug mode** is slower — reduce iterations/data size for debug builds

## Key Files for Orientation
- `docs/dev/repository_layout.md` — Full directory-by-directory guide
- `docs/dev/modules.md` — Module interaction diagram
- `docs/dev/review-checklist.md` — Code review standards
- `configure.py` — Build targets, test binary mappings, source file registry
- `test/README.md` — Test suite organization and `suite.ini` conventions

