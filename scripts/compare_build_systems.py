#!/usr/bin/env python3
"""
Compare configure.py and CMake build systems by parsing their ninja build files.

Checks three things:
  1. Per-file compilation flags  — are the same source files compiled with
     the same defines, warnings, optimization, and language flags?
  2. Link targets                — do both systems produce the same set of
     executables?
  3. Per-target linker settings  — are link flags and libraries identical for
     every common executable?

configure.py is treated as the baseline.  CMake should match it.

Exit codes:
    0  All checked modes match
    1  Differences found
    2  Build artifacts missing (configure both systems first)

Examples:
    # Quick check of dev mode (assumes both systems already configured)
    scripts/compare_build_systems.py -m dev

    # Check all modes, auto-configuring both systems first
    scripts/compare_build_systems.py --configure

    # CI mode: auto-configure, strict, all modes
    scripts/compare_build_systems.py --ci

    # Verbose output showing every flag
    scripts/compare_build_systems.py -m debug -v
"""

import argparse
import os
import re
import shlex
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

MODE_TO_CMAKE = {
    "debug":    "Debug",
    "dev":      "Dev",
    "release":  "RelWithDebInfo",
    "sanitize": "Sanitize",
    "coverage": "Coverage",
}
ALL_MODES = list(MODE_TO_CMAKE.keys())

# Target renames: configure.py name → cmake name.
# CMake requires globally unique target names so some tests are renamed.
KNOWN_RENAMES = {
    "test/manual/hint_test":      "test/manual/manual_hint_test",
    "test/manual/message":        "test/manual/message_test",
    "test/ldap/role_manager_test": "test/ldap/ldap_role_manager_test",
}

# Per-component Boost defines that CMake's imported targets add.
# configure.py uses the single BOOST_ALL_DYN_LINK instead.
_BOOST_PER_COMPONENT_DEFINES = re.compile(
    r"-DBOOST_\w+_(DYN_LINK|NO_LIB)$")

# Internal Scylla/Seastar/Abseil library targets that CMake creates as
# intermediate static/shared libraries.  configure.py links .o files
# directly.  These are structural differences, not bugs.
_INTERNAL_LIB_TARGETS = {
    # Scylla internal libraries
    "scylla-main", "scylla-precompiled-header", "test-lib", "test-raft",
    "test-perf", "api", "alternator", "audit", "scylla_audit", "auth",
    "scylla_auth", "cdc", "compaction", "cql3", "data_dictionary", "db",
    "dht", "scylla_dht", "gms", "idl", "index", "lang", "locator",
    "scylla_locator", "message", "mutation", "mutation_writer", "raft",
    "readers", "repair", "replica", "schema", "service", "sstables",
    "streaming", "tracing", "scylla_tracing", "transport", "types",
    "utils", "vector_search", "tools", "wasmtime_bindings",
    "rust_combined", "scylla_encryption", "inc", "ldap",
    # Abseil libraries
    "absl_bad_optional_access", "absl_bad_variant_access", "absl_civil_time",
    "absl_cord", "absl_cord_internal", "absl_cordz_functions",
    "absl_cordz_handle", "absl_cordz_info", "absl_crc32c",
    "absl_crc_cord_state", "absl_crc_cpu_detect", "absl_crc_internal",
    "absl_kernel_timeout_internal", "absl_log_severity",
    "absl_str_format_internal", "absl_string_view",
    "absl_base", "absl_city", "absl_debugging_internal",
    "absl_demangle_internal", "absl_exponential_biased",
    "absl_graphcycles_internal", "absl_hash", "absl_hashtablez_sampler",
    "absl_int128", "absl_low_level_hash", "absl_malloc_internal",
    "absl_raw_hash_set", "absl_raw_logging_internal", "absl_spinlock_wait",
    "absl_stacktrace", "absl_strings", "absl_strings_internal",
    "absl_symbolize", "absl_synchronization", "absl_throw_delegate",
    "absl_time", "absl_time_zone",
    # System/third-party libraries resolved transitively by CMake
    "boost_regex", "boost_filesystem", "boost_atomic",
    "crypto", "crypt", "ssl",
    "cryptopp", "deflate", "icui18n", "icuuc",
    "jsoncpp", "lber",
    "lua", "lua-5.4", "snappy", "systemd",
    "xxhash", "yaml-cpp", "zstd",
    "z", "m", "dl",
    "seastar_perf_testing", "seastar_testing",
    "atomic", "gmp", "hogweed", "hwloc", "idn2", "nettle",
    "p11-kit", "protobuf", "pthread", "sctp", "tasn1",
    "udev", "unistring", "uring",
    "ubsan", "fmt",
    # Libraries handled differently between build systems:
    # boost_date_time: linked transitively in CMake
    # stdc++fs: not needed with modern compilers (C++17 filesystem in libstdc++)
    "boost_date_time", "stdc++fs",
    # rt: linked transitively via Seastar's rt::rt imported target
    "rt",
}


# ═══════════════════════════════════════════════════════════════════════════
# Ninja file parsing
# ═══════════════════════════════════════════════════════════════════════════

def parse_ninja(filepath):
    """Parse a ninja build file into (variables, rules, builds).

    Follows subninja/include directives.  Returns:
      variables: dict[str, str]       — top-level variable assignments
      rules:     dict[str, dict]      — rule name → {command, ...}
      builds:    list[dict]           — build statements with outputs,
                                        rule, inputs, implicit, vars
    """
    variables = {}
    builds = []
    rules = {}

    def _parse(path, into_vars, into_builds, into_rules):
        base_dir = os.path.dirname(path)
        try:
            with open(path) as f:
                lines = f.readlines()
        except FileNotFoundError:
            return

        i = 0
        while i < len(lines):
            line = lines[i].rstrip("\n")

            if not line or line.startswith("#"):
                i += 1
                continue

            # subninja / include
            m = re.match(r"^(subninja|include)\s+(.+)", line)
            if m:
                inc_path = m.group(2).strip()
                if not os.path.isabs(inc_path):
                    inc_path = os.path.join(base_dir, inc_path)
                _parse(inc_path, into_vars, into_builds, into_rules)
                i += 1
                continue

            # Rule definition
            m = re.match(r"^rule\s+(\S+)", line)
            if m:
                rule_name = m.group(1)
                rule_vars = {}
                i += 1
                while i < len(lines) and lines[i].startswith("  "):
                    rline = lines[i].strip()
                    rm = re.match(r"(\S+)\s*=\s*(.*)", rline)
                    if rm:
                        rule_vars[rm.group(1)] = rm.group(2)
                    i += 1
                into_rules[rule_name] = rule_vars
                continue

            # Top-level variable
            m = re.match(r"^([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*(.*)", line)
            if m and not line.startswith(" "):
                into_vars[m.group(1)] = m.group(2)
                i += 1
                continue

            # Build statement
            m = re.match(r"^build\s+(.+?):\s+(\S+)\s*(.*)", line)
            if m:
                outputs_str = m.group(1)
                rule = m.group(2)
                rest = m.group(3)

                i += 1
                build_vars = {}
                while i < len(lines) and lines[i].startswith("  "):
                    bline = lines[i].strip()
                    bm = re.match(r"(\S+)\s*=\s*(.*)", bline)
                    if bm:
                        build_vars[bm.group(1)] = bm.group(2)
                    i += 1

                parts = re.split(r"\s*\|\|\s*|\s*\|\s*", rest)
                explicit = parts[0].strip() if parts else ""
                implicit = parts[1].strip() if len(parts) > 1 else ""

                into_builds.append({
                    "outputs": outputs_str.strip(),
                    "rule":    rule,
                    "inputs":  explicit,
                    "implicit": implicit,
                    "vars":    build_vars,
                })
                continue

            i += 1

    _parse(str(filepath), variables, builds, rules)
    return variables, rules, builds


def resolve_var(value, variables, depth=0):
    """Recursively resolve $var and ${var} references."""
    if depth > 10 or "$" not in value:
        return value

    def _repl(m):
        name = m.group(1) or m.group(2)
        return variables.get(name, "")

    result = re.sub(r"\$\{(\w+)\}|\$(\w+)", _repl, value)
    if "$" in result and result != value:
        return resolve_var(result, variables, depth + 1)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Flag extraction helpers
# ═══════════════════════════════════════════════════════════════════════════

def tokenize(flags_str):
    """Split a flags string into tokens, joining multi-word flags."""
    tokens = []
    parts = flags_str.split()
    i = 0
    while i < len(parts):
        if (parts[i] in ("-Xclang", "-mllvm", "--param", "-Xlinker")
                and i + 1 < len(parts)):
            tokens.append(f"{parts[i]} {parts[i+1]}")
            i += 2
        else:
            tokens.append(parts[i])
            i += 1
    return tokens


def categorize_compile_flags(command_str):
    """Extract and categorize compilation flags from a command string.

    Returns dict with keys: defines, warnings, f_flags, opt_flags,
    arch_flags, std_flags.
    """
    try:
        tokens = shlex.split(command_str)
    except ValueError:
        tokens = command_str.split()

    flags = {
        "defines":    set(),
        "warnings":   set(),
        "f_flags":    set(),
        "opt_flags":  set(),
        "arch_flags": set(),
        "std_flags":  set(),
    }

    skip_next = False
    for tok in tokens:
        if skip_next:
            skip_next = False
            continue

        if tok.startswith("-D"):
            if _BOOST_PER_COMPONENT_DEFINES.match(tok):
                continue
            # Normalize version defines that contain git hashes
            if tok.startswith("-DSCYLLA_RELEASE="):
                tok = "-DSCYLLA_RELEASE=<release>"
            elif tok.startswith("-DSCYLLA_VERSION="):
                tok = "-DSCYLLA_VERSION=<version>"
            flags["defines"].add(tok)
        elif tok.startswith("-W"):
            if tok == "-Winvalid-pch":
                continue
            # -Wno-backend-plugin is added by configure.py when a PGO
            # profile is available.  CMake handles PGO separately.
            if tok == "-Wno-backend-plugin":
                continue
            flags["warnings"].add(tok)
        elif tok.startswith("-f"):
            if "-ffile-prefix-map=" in tok:
                continue
            # LTO and PGO flags are configuration-dependent options
            # (--lto, --pgo, --use-profile for configure.py;
            # Scylla_PROFDATA_FILE for CMake), not mode-inherent.
            if (tok.startswith("-flto")
                    or tok == "-ffat-lto-objects"
                    or tok == "-fno-lto"
                    or tok.startswith("-fprofile-use=")
                    or tok.startswith("-fprofile-generate")
                    or tok == "-fpch-validate-input-files-content"):
                continue
            flags["f_flags"].add(tok)
        elif tok.startswith("-O"):
            flags["opt_flags"].add(tok)
        elif tok.startswith("-march="):
            flags["arch_flags"].add(tok)
        elif tok.startswith("-std="):
            flags["std_flags"].add(tok)
        elif tok in ("-c", "-o", "-MD", "-MT", "-MF", "-Xclang"):
            skip_next = True
        elif tok in ("-include-pch", "-include", "-emit-pch"):
            skip_next = True
        elif tok.startswith(("-I", "-iquote", "-isystem")):
            if tok in ("-I", "-iquote", "-isystem"):
                skip_next = True
            continue

    return flags


def normalize_lib_name(token):
    """Extract canonical library name from -l, .a, or .so tokens."""
    if token.startswith("-l"):
        return token[2:]
    basename = os.path.basename(token)
    m = re.match(r"lib(.+?)\.(?:a|so(?:\.\S*)?)", basename)
    return m.group(1) if m else None


def normalize_linker_flag(tok):
    """Normalize a linker flag to a canonical comparable form."""
    if tok.startswith("-Wl,"):
        parts = tok[4:].split(",")
        result = set()
        for part in parts:
            if "--dynamic-linker" in part:
                result.add("-Wl,--dynamic-linker=<padded>")
            elif "-rpath" in part:
                result.add("-Wl,-rpath=<paths>")
            elif "--build-id" in part:
                result.add(f"-Wl,{part}")
            elif part in ("--push-state", "--pop-state",
                          "--whole-archive", "--no-whole-archive",
                          "-Bstatic", "-Bdynamic"):
                continue
            elif "--strip" in part:
                result.add(f"-Wl,{part}")
            elif part and not part.startswith("/"):
                # Skip bare paths (rpath values, library search paths)
                result.add(f"-Wl,{part}")
        return result
    if tok.startswith("-Xlinker "):
        arg = tok.split(" ", 1)[1]
        if "--dynamic-linker" in arg:
            return {"-Wl,--dynamic-linker=<padded>"}
        if "--build-id" in arg:
            return {f"-Wl,{arg}"}
        if "-rpath" in arg:
            return {"-Wl,-rpath=<paths>"}
        if "--dependency-file" in arg:
            return set()
        if arg in ("--push-state", "--pop-state",
                    "--whole-archive", "--no-whole-archive",
                    "-Bstatic", "-Bdynamic"):
            return set()
        return {f"-Wl,{arg}"}
    if tok.startswith("-fuse-ld=") or tok.startswith("--ld-path="):
        name = tok.split("=", 1)[1]
        name = os.path.basename(name)
        if name.startswith("ld."):
            name = name[3:]
        return {f"linker={name}"}
    return {tok}


# ═══════════════════════════════════════════════════════════════════════════
# Source file extraction from ninja builds
# ═══════════════════════════════════════════════════════════════════════════

def _is_scylla_source(rel_path):
    """True if this is a Scylla-owned source file (not seastar/abseil)."""
    return (not rel_path.startswith("seastar/")
            and not rel_path.startswith("abseil/")
            and not rel_path.startswith("build")
            and rel_path != "tools/patchelf.cc"
            and rel_path != "exported_templates.cc"
            and (rel_path.endswith(".cc") or rel_path.endswith(".cpp")))


def extract_configure_compile_entries(variables, rules, builds,
                                      mode, source_dir):
    """Extract per-source-file flags from configure.py build.ninja.

    Returns dict: relative_source_path → categorized flags dict.
    """
    entries = {}
    mode_prefix = f"build/{mode}/"

    # Find compile rules for this mode
    compile_rules = {}
    for name, rvars in rules.items():
        if (name.startswith(f"cxx.{mode}")
                or name.startswith(f"cxx_with_pch.{mode}")):
            compile_rules[name] = rvars

    if not compile_rules:
        return entries

    for b in builds:
        if b["rule"] not in compile_rules:
            continue

        output = b["outputs"]
        output = output.replace("$builddir/", "build/")
        if not output.startswith(mode_prefix):
            continue

        # Get source file from inputs
        src_tokens = b["inputs"].strip().split()
        if not src_tokens:
            continue
        src = src_tokens[0]
        src = src.replace("$builddir/", "build/")

        # Make source path relative
        if os.path.isabs(src):
            try:
                rel_src = os.path.relpath(src, source_dir)
            except ValueError:
                rel_src = src
        else:
            rel_src = src

        if not _is_scylla_source(rel_src):
            continue

        # Build effective command by resolving variables.
        # Ninja scoping: build-statement variable VALUES are resolved
        # against the enclosing (file-level) scope, NOT against themselves.
        rule_def = compile_rules[b["rule"]]
        outer_scope = dict(variables)
        outer_scope.update(rule_def)
        resolved_build_vars = {}
        for k, v in b["vars"].items():
            resolved_build_vars[k] = resolve_var(v, outer_scope)

        merged = dict(variables)
        merged.update(rule_def)
        merged.update(resolved_build_vars)
        merged["in"] = b["inputs"]
        merged["out"] = b["outputs"]

        command = rule_def.get("command", "")
        resolved = resolve_var(command, merged)

        entries[rel_src] = categorize_compile_flags(resolved)

    return entries


def extract_cmake_compile_entries(builds, source_dir):
    """Extract per-source-file flags from CMake build.ninja.

    Returns dict: relative_source_path → categorized flags dict.
    """
    entries = {}

    for b in builds:
        if "CXX_COMPILER" not in b["rule"]:
            continue

        # Get source file from inputs
        src_tokens = b["inputs"].strip().split()
        if not src_tokens:
            continue
        src = src_tokens[0]

        if os.path.isabs(src):
            try:
                rel_src = os.path.relpath(src, source_dir)
            except ValueError:
                rel_src = src
        else:
            rel_src = src

        if not _is_scylla_source(rel_src):
            continue

        # Build a pseudo-command from DEFINES + FLAGS
        defines = b["vars"].get("DEFINES", "")
        flags = b["vars"].get("FLAGS", "")
        pseudo_cmd = f"{defines} {flags}"

        entries[rel_src] = categorize_compile_flags(pseudo_cmd)

    return entries


# ═══════════════════════════════════════════════════════════════════════════
# Link target extraction from ninja builds
# ═══════════════════════════════════════════════════════════════════════════

def _is_link_rule(rule):
    """True if the rule is a link rule (executable linker).

    Excludes link_stripped rules which are just stripped copies of the
    unstripped targets (configure.py creates both variants).
    """
    rl = rule.lower()
    return ("link" in rl and "static" not in rl and "shared" not in rl
            and "module" not in rl and "stripped" not in rl)


def _extract_link_info(build, variables, rules):
    """Extract linker flags and libraries from a link build statement."""
    rule_def = rules.get(build["rule"], {})

    # Resolve build variable values against the outer scope first
    # (ninja scoping: build var RHS is evaluated in file scope).
    outer_scope = dict(variables)
    outer_scope.update(rule_def)
    resolved_build_vars = {}
    for k, v in build["vars"].items():
        resolved_build_vars[k] = resolve_var(v, outer_scope)

    merged = dict(variables)
    merged.update(rule_def)
    merged.update(resolved_build_vars)
    merged["in"] = build["inputs"]
    merged["out"] = build["outputs"]

    # Resolve command from the rule template (configure.py style)
    command_template = rule_def.get("command", "")
    command = resolve_var(command_template, merged)

    # For CMake, also look at explicit LINK_FLAGS and LINK_LIBRARIES vars
    link_flags_var = build["vars"].get("LINK_FLAGS", "")
    link_libs_var = build["vars"].get("LINK_LIBRARIES", "")

    linker_flags = set()
    libraries = set()

    # Parse from resolved command (for configure.py)
    if command_template:
        try:
            tokens = shlex.split(command)
        except ValueError:
            tokens = command.split()

        skip = False
        for tok in tokens:
            if skip:
                skip = False
                continue
            if tok in ("-o", "-MF", "-MT"):
                skip = True
                continue
            # Skip LTO/PGO linker flags — configuration-dependent
            if (tok.startswith("-flto") or tok == "-fno-lto"
                    or tok == "-ffat-lto-objects"
                    or tok.startswith("-fprofile-use=")
                    or tok.startswith("-fprofile-generate")):
                continue
            if tok.startswith("-fsanitize") or tok.startswith("-fno-sanitize"):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok.startswith("-fuse-ld=") or tok.startswith("--ld-path="):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok.startswith("-Wl,"):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok == "-static-libstdc++":
                linker_flags.add(tok)
            elif tok == "-s":
                linker_flags.add("-Wl,--strip-all")

            # Libraries
            if tok.startswith("-l"):
                lib = tok[2:]
                libraries.add(lib)
            elif tok.endswith(".o"):
                continue
            elif tok.endswith(".a") or ".so" in tok:
                name = normalize_lib_name(tok)
                if name:
                    libraries.add(name)

    # Parse from explicit LINK_FLAGS/LINK_LIBRARIES (CMake style)
    if link_flags_var:
        for tok in tokenize(link_flags_var):
            # Skip LTO/PGO linker flags — configuration-dependent
            if (tok.startswith("-flto") or tok == "-fno-lto"
                    or tok == "-ffat-lto-objects"
                    or tok.startswith("-fprofile-use=")
                    or tok.startswith("-fprofile-generate")):
                continue
            if tok.startswith("-fsanitize") or tok.startswith("-fno-sanitize"):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok.startswith("-fuse-ld=") or tok.startswith("--ld-path="):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok.startswith("-Wl,") or tok.startswith("-Xlinker "):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok == "-s":
                linker_flags.add("-Wl,--strip-all")

    if link_libs_var:
        for tok in tokenize(link_libs_var):
            if tok.startswith("-fsanitize") or tok.startswith("-fno-sanitize"):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok.startswith("-Wl,") or tok.startswith("-Xlinker "):
                linker_flags.update(normalize_linker_flag(tok))
            elif tok.startswith("-l"):
                libraries.add(tok[2:])
            elif tok.endswith(".o"):
                continue
            elif tok.endswith(".a") or ".so" in tok:
                name = normalize_lib_name(tok)
                if name:
                    libraries.add(name)

    # Also extract libraries from build inputs (implicit deps)
    all_inputs = build["inputs"] + " " + build.get("implicit", "")
    all_inputs = resolve_var(all_inputs, merged)
    for tok in all_inputs.split():
        if tok.endswith(".o"):
            continue
        if tok.endswith(".a") or ".so" in tok:
            name = normalize_lib_name(tok)
            if name:
                libraries.add(name)

    return {"linker_flags": linker_flags, "libraries": libraries}


def extract_configure_link_targets(variables, rules, builds, mode):
    """Extract link targets from configure.py build.ninja.

    Returns dict: target_name → {linker_flags, libraries}.
    """
    result = {}
    mode_prefix = f"build/{mode}/"

    for b in builds:
        if not _is_link_rule(b["rule"]):
            continue
        if not b["rule"].endswith(f".{mode}"):
            continue

        target = b["outputs"].replace("$builddir/", "build/")
        if not target.startswith(mode_prefix):
            continue
        target = target[len(mode_prefix):]

        if (target.endswith(".stripped") or target.endswith(".debug")
                or target.endswith(".so") or target.endswith(".a")):
            continue
        if target.startswith("seastar/") or target.startswith("abseil/"):
            continue

        # Strip _g suffix (unstripped variant)
        if target.endswith("_g"):
            target = target[:-2]

        result[target] = _extract_link_info(b, variables, rules)

    return result


def extract_cmake_link_targets(variables, rules, builds, mode):
    """Extract link targets from CMake build.ninja.

    Returns dict: target_name → {linker_flags, libraries}.
    """
    result = {}
    cmake_type = MODE_TO_CMAKE.get(mode, "")

    for b in builds:
        if not _is_link_rule(b["rule"]):
            continue

        target = b["outputs"]

        # Skip non-executable link rules
        if (target.endswith(".stripped") or target.endswith(".debug")
                or target.endswith(".so") or target.endswith(".a")):
            continue
        if target.startswith("seastar/") or target.startswith("abseil/"):
            continue

        # Strip cmake type prefix if present (e.g., "Dev/scylla" → "scylla")
        if cmake_type and target.startswith(f"{cmake_type}/"):
            target = target[len(cmake_type) + 1:]

        result[target] = _extract_link_info(b, variables, rules)

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Configuration helpers
# ═══════════════════════════════════════════════════════════════════════════

def find_repo_root():
    """Find the repository root by looking for configure.py."""
    candidate = Path(__file__).resolve().parent.parent
    if (candidate / "configure.py").exists():
        return candidate
    candidate = Path.cwd()
    if (candidate / "configure.py").exists():
        return candidate
    sys.exit("ERROR: Cannot find repository root (no configure.py found)")


def run_configure_py(repo_root, modes, quiet=False):
    """Run configure.py for the given modes."""
    mode_args = []
    for m in modes:
        mode_args.extend(["--mode", m])
    cmd = [sys.executable, str(repo_root / "configure.py")] + mode_args
    if not quiet:
        print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(repo_root),
                            capture_output=quiet, text=True)
    if result.returncode != 0:
        print(f"ERROR: configure.py failed (exit {result.returncode})",
              file=sys.stderr)
        return False
    return True


def run_cmake_configure(repo_root, mode, quiet=False):
    """Run cmake configuration for the given mode."""
    cmake_type = MODE_TO_CMAKE[mode]
    build_dir = repo_root / "build" / mode
    ninja = "ninja"
    for name in ("ninja", "ninja-build"):
        r = subprocess.run(["which", name], capture_output=True, text=True)
        if r.returncode == 0:
            ninja = r.stdout.strip()
            break

    cmd = [
        "cmake",
        f"-DCMAKE_BUILD_TYPE={cmake_type}",
        f"-DCMAKE_MAKE_PROGRAM={ninja}",
        "-DCMAKE_C_COMPILER=clang",
        "-DCMAKE_CXX_COMPILER=clang++",
        "-G", "Ninja",
        "-S", str(repo_root),
        "-B", str(build_dir),
    ]
    if not quiet:
        print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(repo_root),
                            capture_output=quiet, text=True)
    if result.returncode != 0:
        print(f"ERROR: cmake failed for mode '{mode}' "
              f"(exit {result.returncode})", file=sys.stderr)
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════════
# Comparison logic
# ═══════════════════════════════════════════════════════════════════════════

def compare_flag_sets(label, set_a, set_b):
    """Compare two sets, return list of difference strings."""
    only_a = set_a - set_b
    only_b = set_b - set_a
    diffs = []
    if only_a:
        diffs.append(f"{label}: only in configure.py: {sorted(only_a)}")
    if only_b:
        diffs.append(f"{label}: only in CMake:         {sorted(only_b)}")
    return diffs


def compare_compile_entries(conf_entries, cmake_entries, verbose=False,
                            quiet=False):
    """Compare per-file compilation flags.

    Returns (ok, summary_dict).
    """
    common = sorted(set(conf_entries) & set(cmake_entries))
    only_conf = sorted(set(conf_entries) - set(cmake_entries))
    only_cmake = sorted(set(cmake_entries) - set(conf_entries))

    if not quiet:
        print(f"\n  Source files in both:              {len(common)}")
        print(f"  Source files only in configure.py: {len(only_conf)}")
        print(f"  Source files only in CMake:        {len(only_cmake)}")

        if only_conf:
            print("\n  Files only in configure.py:")
            for f in only_conf:
                print(f"    {f}")
        if only_cmake:
            print("\n  Files only in CMake:")
            for f in only_cmake:
                print(f"    {f}")

    files_with_diffs = 0
    aggregate = defaultdict(int)

    for src in common:
        conf_flags = conf_entries[src]
        cmake_flags = cmake_entries[src]

        file_diffs = []
        for cat in ("defines", "warnings", "f_flags", "opt_flags",
                     "arch_flags", "std_flags"):
            d = compare_flag_sets(cat, conf_flags[cat], cmake_flags[cat])
            file_diffs.extend(d)
            for flag in conf_flags[cat] - cmake_flags[cat]:
                aggregate[f"only-configure.py  {cat}: {flag}"] += 1
            for flag in cmake_flags[cat] - conf_flags[cat]:
                aggregate[f"only-cmake         {cat}: {flag}"] += 1

        if file_diffs:
            files_with_diffs += 1
            if verbose and not quiet:
                print(f"\n  DIFF {src}:")
                for d in file_diffs:
                    print(f"      {d}")

    if not quiet:
        print(f"\n  Files with flag differences: "
              f"{files_with_diffs} / {len(common)}")
        if aggregate:
            print("\n  Aggregate flag diffs (flag → # files):")
            for key, cnt in sorted(aggregate.items(), key=lambda x: -x[1]):
                print(f"    {key}  ({cnt} files)")

    ok = files_with_diffs == 0 and not only_conf and not only_cmake
    return ok, {
        "common": len(common),
        "only_conf": only_conf,
        "only_cmake": only_cmake,
        "files_with_diffs": files_with_diffs,
        "aggregate": dict(aggregate),
    }


def compare_link_target_sets(conf_targets, cmake_targets, verbose=False,
                              quiet=False):
    """Compare which targets exist in both systems.

    Returns (ok, summary_dict).
    """
    conf_set = set(conf_targets)
    cmake_set = set(cmake_targets)

    # Apply known renames
    renamed = []
    only_conf_raw = conf_set - cmake_set
    unexplained_conf = set()
    unexplained_cmake = set(cmake_set - conf_set)

    for t in only_conf_raw:
        cmake_name = KNOWN_RENAMES.get(t)
        if cmake_name and cmake_name in unexplained_cmake:
            renamed.append((t, cmake_name))
            unexplained_cmake.discard(cmake_name)
        else:
            unexplained_conf.add(t)

    if not quiet:
        print(f"\n  Targets in configure.py: {len(conf_set)}")
        print(f"  Targets in CMake:        {len(cmake_set)}")

        if renamed:
            print(f"\n  Known renames ({len(renamed)}):")
            for old, new in sorted(renamed):
                print(f"    {old}  →  {new}")
        if unexplained_conf:
            print(f"\n  ✗ Only in configure.py ({len(unexplained_conf)}):")
            for t in sorted(unexplained_conf):
                print(f"    {t}")
        if unexplained_cmake:
            print(f"\n  ✗ Only in CMake ({len(unexplained_cmake)}):")
            for t in sorted(unexplained_cmake):
                print(f"    {t}")

    ok = not unexplained_conf and not unexplained_cmake
    if ok and not quiet:
        print("  ✓ All targets match!")

    return ok, {
        "only_conf": sorted(unexplained_conf),
        "only_cmake": sorted(unexplained_cmake),
        "renamed": renamed,
    }


def compare_link_settings(conf_targets, cmake_targets, verbose=False,
                           quiet=False):
    """Compare linker flags and libraries for common targets.

    Returns (ok, summary_dict).
    """
    # Build common set accounting for renames
    rename_map = {}
    for conf_name, cmake_name in KNOWN_RENAMES.items():
        if conf_name in conf_targets and cmake_name in cmake_targets:
            rename_map[conf_name] = cmake_name

    common = sorted(set(conf_targets) & set(cmake_targets))
    # Add renamed targets
    for conf_name, cmake_name in rename_map.items():
        if conf_name not in common:
            common.append(conf_name)
    common.sort()

    # Standalone tools that have known structural differences
    _CPP_APPS = {"patchelf"}

    flag_diffs = 0
    lib_diffs = 0
    flag_agg_conf = defaultdict(int)
    flag_agg_cmake = defaultdict(int)
    lib_agg_conf = defaultdict(int)
    lib_agg_cmake = defaultdict(int)

    for target in common:
        conf = conf_targets[target]
        cmake_name = rename_map.get(target, target)
        cmake = cmake_targets.get(cmake_name)
        if not cmake:
            continue

        # Linker flags
        only_conf_flags = conf["linker_flags"] - cmake["linker_flags"]
        only_cmake_flags = cmake["linker_flags"] - conf["linker_flags"]

        # Known exception: standalone tools don't get -fno-lto in configure.py
        target_base = target.rsplit("/", 1)[-1] if "/" in target else target
        if target_base in _CPP_APPS:
            only_cmake_flags.discard("-fno-lto")

        if only_conf_flags or only_cmake_flags:
            flag_diffs += 1
            for f in only_conf_flags:
                flag_agg_conf[f] += 1
            for f in only_cmake_flags:
                flag_agg_cmake[f] += 1
            if verbose and not quiet:
                print(f"\n  {target}:")
                if only_conf_flags:
                    print(f"    Linker flags only in configure.py: "
                          f"{sorted(only_conf_flags)}")
                if only_cmake_flags:
                    print(f"    Linker flags only in CMake:        "
                          f"{sorted(only_cmake_flags)}")

        # Libraries
        conf_libs = conf["libraries"] - _INTERNAL_LIB_TARGETS
        cmake_libs = cmake["libraries"] - _INTERNAL_LIB_TARGETS
        only_conf_libs = conf_libs - cmake_libs
        only_cmake_libs = cmake_libs - conf_libs
        if only_conf_libs or only_cmake_libs:
            lib_diffs += 1
            for lib in only_conf_libs:
                lib_agg_conf[lib] += 1
            for lib in only_cmake_libs:
                lib_agg_cmake[lib] += 1
            if verbose and not quiet:
                print(f"\n  {target}:")
                if only_conf_libs:
                    print(f"    Libs only in configure.py: "
                          f"{sorted(only_conf_libs)}")
                if only_cmake_libs:
                    print(f"    Libs only in CMake:        "
                          f"{sorted(only_cmake_libs)}")

    if not quiet:
        print(f"\n  Linker flag differences: {flag_diffs} / {len(common)}")
        if flag_agg_conf or flag_agg_cmake:
            print("\n  Aggregate linker flag diffs:")
            for f, c in sorted(flag_agg_conf.items(), key=lambda x: -x[1]):
                print(f"    only-configure.py  {f}  ({c} targets)")
            for f, c in sorted(flag_agg_cmake.items(), key=lambda x: -x[1]):
                print(f"    only-cmake         {f}  ({c} targets)")

        print(f"\n  Library differences: {lib_diffs} / {len(common)}")
        if lib_agg_conf or lib_agg_cmake:
            print("\n  Aggregate library diffs:")
            for lib, c in sorted(lib_agg_conf.items(), key=lambda x: -x[1]):
                print(f"    only-configure.py  {lib}  ({c} targets)")
            for lib, c in sorted(lib_agg_cmake.items(), key=lambda x: -x[1]):
                print(f"    only-cmake         {lib}  ({c} targets)")

    ok = flag_diffs == 0 and lib_diffs == 0
    if ok and not quiet:
        print("  ✓ Linker flags and libraries match for all common targets!")
    return ok, {
        "flag_diffs": flag_diffs,
        "lib_diffs": lib_diffs,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Mode-level comparison orchestrator
# ═══════════════════════════════════════════════════════════════════════════

def compare_mode(mode, repo_root, conf_parsed=None, verbose=False,
                 quiet=False):
    """Run all comparisons for one mode.

    Args:
        conf_parsed: Pre-parsed configure.py build.ninja data
            (variables, rules, builds).  When provided, avoids re-reading
            the file — important when --configure is used, because CMake
            may write to the same build/<mode>/ directory and could
            overwrite a symlinked build.ninja.

    Returns:
        True  — everything matches
        False — differences found
        None  — build artifacts missing (skipped)
    """
    source_dir = str(repo_root)
    cmake_ninja = repo_root / "build" / mode / "build.ninja"

    if conf_parsed is not None:
        conf_vars, conf_rules, conf_builds = conf_parsed
    else:
        conf_ninja = repo_root / "build.ninja"
        if not conf_ninja.exists():
            if not quiet:
                print(f"  ⚠ configure.py build.ninja not found")
                print(f"    Run: ./configure.py --mode={mode}")
            return None
        conf_vars, conf_rules, conf_builds = parse_ninja(conf_ninja)

    if not cmake_ninja.exists():
        if not quiet:
            print(f"  ⚠ CMake build.ninja not found: {cmake_ninja}")
            print(f"    Run: cmake -B build/{mode} "
                  f"-DCMAKE_BUILD_TYPE={MODE_TO_CMAKE[mode]} -G Ninja")
        return None

    cmake_vars, cmake_rules, cmake_builds = parse_ninja(cmake_ninja)

    # Check that configure.py build.ninja has this mode
    has_mode = any(r.endswith(f".{mode}") for r in conf_rules)
    if not has_mode:
        if not quiet:
            print(f"  ⚠ configure.py build.ninja doesn't contain mode '{mode}'")
            print(f"    Re-run: ./configure.py --mode={mode}")
        return None

    all_ok = True

    # ── 1. Per-file compilation flags ─────────────────────────────
    if not quiet:
        print(f"\n  {'─'*56}")
        print(f"  Compilation flags (per-file)")
        print(f"  {'─'*56}")

    conf_entries = extract_configure_compile_entries(
        conf_vars, conf_rules, conf_builds, mode, source_dir)
    cmake_entries = extract_cmake_compile_entries(
        cmake_builds, source_dir)

    flags_ok, _ = compare_compile_entries(
        conf_entries, cmake_entries, verbose, quiet)
    if not flags_ok:
        all_ok = False

    # ── 2. Link targets ───────────────────────────────────────────
    if not quiet:
        print(f"\n  {'─'*56}")
        print(f"  Link targets")
        print(f"  {'─'*56}")

    conf_link = extract_configure_link_targets(
        conf_vars, conf_rules, conf_builds, mode)
    cmake_link = extract_cmake_link_targets(
        cmake_vars, cmake_rules, cmake_builds, mode)

    targets_ok, _ = compare_link_target_sets(
        conf_link, cmake_link, verbose, quiet)
    if not targets_ok:
        all_ok = False

    # ── 3. Linker flags & libraries ───────────────────────────────
    if not quiet:
        print(f"\n  {'─'*56}")
        print(f"  Linker flags & libraries")
        print(f"  {'─'*56}")

    linker_ok, _ = compare_link_settings(
        conf_link, cmake_link, verbose, quiet)
    if not linker_ok:
        all_ok = False

    return all_ok


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        prog="compare_build_systems.py",
        description=(
            "Compare configure.py and CMake build systems by parsing their "
            "ninja build files.  Checks per-file flags, link targets, and "
            "linker settings."),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  # Quick check of dev mode (both systems already configured)
  %(prog)s -m dev

  # Check all modes, auto-configuring both systems first
  %(prog)s --configure

  # CI mode: auto-configure, strict, all modes
  %(prog)s --ci

  # CI mode without auto-configure (artifacts already exist)
  %(prog)s --ci --no-configure

  # Verbose output showing per-file differences
  %(prog)s -m debug -v

mode mapping:
  configure.py    CMake
  ──────────────  ──────────────
  debug           Debug
  dev             Dev
  release         RelWithDebInfo
  sanitize        Sanitize
  coverage        Coverage
""")
    parser.add_argument(
        "-m", "--mode",
        choices=ALL_MODES + ["all"],
        default="all",
        help="Build mode to compare (default: all)")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show per-file/per-target differences")
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Minimal output — only summary and errors")
    parser.add_argument(
        "--configure",
        action="store_true",
        help="Auto-run configure.py and cmake before comparing")
    parser.add_argument(
        "--ci",
        action="store_true",
        help="CI mode: auto-configure + quiet + strict (exit 1 on any diff)")
    parser.add_argument(
        "--no-configure",
        action="store_true",
        help="In --ci mode, skip auto-configure (assume artifacts exist)")
    parser.add_argument(
        "--source-dir",
        type=Path, default=None,
        help="Repository root directory (default: auto-detect)")

    args = parser.parse_args()
    if args.ci:
        if not args.no_configure:
            args.configure = True
        args.quiet = True
    return args


def main():
    args = parse_args()
    repo_root = args.source_dir or find_repo_root()
    modes = ALL_MODES if args.mode == "all" else [args.mode]
    quiet = args.quiet

    if not quiet:
        print("=" * 70)
        print("Build System Comparison: configure.py vs CMake")
        print("=" * 70)

    # Auto-configure if requested
    if args.configure:
        if not quiet:
            print("\nConfiguring build systems...")
            print("\n─── configure.py ───")
        if not run_configure_py(repo_root, modes, quiet):
            return 2

    # Parse configure.py's build.ninja BEFORE running cmake.
    #
    # configure.py may generate a single multi-mode build.ninja at the
    # repo root, or per-mode files under build/<mode>/ (with the root
    # file being a symlink).  Either way we snapshot the data now, before
    # cmake could overwrite build/<mode>/build.ninja.
    #
    # Strategy: parse the root build.ninja once.  For each mode, check
    # whether it contains that mode's rules.  If not — fall back to the
    # per-mode file at build/<mode>/build.ninja (and parse it before cmake
    # touches it).
    conf_ninja = repo_root / "build.ninja"
    conf_parsed = None
    if conf_ninja.exists():
        if not quiet:
            print("\nParsing configure.py build.ninja...")
        conf_parsed = parse_ninja(conf_ninja)

    # Per-mode pre-parse: for modes not present in the root file,
    # capture the per-mode build.ninja before cmake overwrites it.
    conf_parsed_per_mode = {}
    for mode in modes:
        if conf_parsed is not None:
            _, conf_rules, _ = conf_parsed
            if any(r.endswith(f".{mode}") for r in conf_rules):
                conf_parsed_per_mode[mode] = conf_parsed
                continue
        # Mode not in root file — try per-mode file
        per_mode_ninja = repo_root / "build" / mode / "build.ninja"
        if per_mode_ninja.exists():
            if not quiet:
                print(f"  Parsing per-mode configure.py ninja for '{mode}'...")
            conf_parsed_per_mode[mode] = parse_ninja(per_mode_ninja)

    if args.configure:
        if not quiet:
            print("\n─── cmake ───")
        for m in modes:
            if not run_cmake_configure(repo_root, m, quiet):
                return 2

    # Compare each mode
    results = {}
    for mode in modes:
        cmake_mode = MODE_TO_CMAKE[mode]
        if not quiet:
            print(f"\n{'═' * 70}")
            print(f"Mode: {mode} (CMake: {cmake_mode})")
            print(f"{'═' * 70}")

        results[mode] = compare_mode(
            mode, repo_root,
            conf_parsed=conf_parsed_per_mode.get(mode),
            verbose=args.verbose, quiet=quiet)

    # Summary
    if not quiet:
        print(f"\n{'═' * 70}")
        print("Summary")
        print(f"{'═' * 70}")

    for mode, ok in results.items():
        if ok is None:
            status = "⚠ SKIPPED (build artifacts not found)"
        elif ok:
            status = "✓ MATCH"
        else:
            status = "✗ MISMATCH"

        if quiet:
            print(f"{mode}: {status}")
        else:
            cmake_mode = MODE_TO_CMAKE[mode]
            print(f"  {mode:10s} (CMake: {cmake_mode:15s}):  {status}")

    has_failures = any(v is False for v in results.values())
    has_skips = any(v is None for v in results.values())
    all_pass = all(v is True for v in results.values())

    if has_failures:
        if not quiet:
            print("\n✗ Some modes have differences.  See above for details.")
        return 1
    elif all_pass:
        if not quiet:
            print("\n✓ All modes match!")
        return 0
    elif has_skips and args.ci:
        if not quiet:
            print("\n✗ Some modes were skipped — build artifacts missing.")
        return 2
    else:
        if not quiet:
            print("\nSome modes were skipped.  Use --configure to generate "
                  "artifacts.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
