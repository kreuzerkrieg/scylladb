#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Compare the in-house configure.py build system with the CMake build system.
#
# Both build systems produce ninja build files (build.ninja). This script
# configures both, then parses and compares the resulting build.ninja files
# to find discrepancies in compilation flags, build targets, and linker
# commands.
#
# How it works:
#   1. Runs configure.py to produce a top-level build.ninja and extracts a
#      compile_commands.json from it (via "ninja -t compdb"). The compdb is
#      saved aside because both build systems share the same output directory
#      (build/<mode>/).
#   2. Runs cmake into build/<mode>/ to produce its own build.ninja (and
#      compile_commands.json).
#   3. Compares compilation flags per source file using the two compdbs.
#   4. Compares linked executable targets from both build.ninja files.
#   5. Compares linker flags and linked libraries per executable target
#      by parsing both build.ninja files directly.
#
# Usage:
#   # Full run for one mode (configure + compare):
#   python3 scripts/compare_build_systems.py [--mode MODE]
#
#   # All modes:
#   python3 scripts/compare_build_systems.py --all-modes
#
#   # Compare pre-existing builds (skip configure steps):
#   python3 scripts/compare_build_systems.py --skip-configure \
#       --inhouse-compdb build/debug_inhouse_compile_commands.json \
#       --cmake-compdb build/debug/compile_commands.json
#
#   # CI mode: all modes, exits 0 if identical, 1 if differences found.
#   # Prints a detailed report: files added/removed, compile flag diffs per
#   # file, linker flag and library diffs per target, plus aggregate summaries:
#   python3 scripts/compare_build_systems.py --ci
#
#   # Show all available options:
#   python3 scripts/compare_build_systems.py --help
#
# Requires: ninja, cmake, clang/clang++

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from collections import defaultdict


# Map configure.py mode names to CMake build types
MODE_TO_CMAKE_TYPE = {
    "dev": "Dev",
    "debug": "Debug",
    "release": "RelWithDebInfo",
    "sanitize": "Sanitize",
    "coverage": "Coverage",
}


def parse_args():
    p = argparse.ArgumentParser(
        description="Compare configure.py (ninja) vs CMake build systems",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Compare dev mode (default):
  python3 scripts/compare_build_systems.py

  # Compare a specific mode:
  python3 scripts/compare_build_systems.py --mode debug

  # Compare all modes:
  python3 scripts/compare_build_systems.py --all-modes

  # CI mode (all modes, exit 0/1):
  python3 scripts/compare_build_systems.py --ci

  # Compare pre-existing builds (skip configure steps):
  python3 scripts/compare_build_systems.py --skip-configure \\
      --inhouse-compdb build/debug_inhouse_compile_commands.json \\
      --cmake-compdb build/debug/compile_commands.json
""")
    p.add_argument("--mode", default="dev",
                   choices=list(MODE_TO_CMAKE_TYPE.keys()),
                   help="Build mode to compare (default: dev)")
    p.add_argument("--all-modes", action="store_true",
                   help="Compare all modes (dev, debug, release, sanitize, coverage)")
    p.add_argument("--ci", action="store_true",
                   help="CI mode: run all modes, configure both build systems, "
                        "exit 0 if identical, 1 if differences found. "
                        "Outputs a detailed report: files added/removed, "
                        "per-file compile flag diffs, per-target linker flag "
                        "and library diffs, plus aggregate summaries.")
    p.add_argument("--source-dir", default=None,
                   help="Path to scylla source tree (default: script's parent dir)")
    p.add_argument("--skip-configure", action="store_true",
                   help="Skip running configure steps, use existing compdbs")
    p.add_argument("--inhouse-compdb", default=None,
                   help="Path to in-house compile_commands.json "
                         "(derived from build.ninja; used with --skip-configure)")
    p.add_argument("--cmake-compdb", default=None,
                   help="Path to CMake compile_commands.json "
                        "(used with --skip-configure)")
    p.add_argument("--inhouse-build-dir", default=None,
                   help="In-house build directory containing build.ninja "
                        "(for target comparison)")
    p.add_argument("--cmake-build-dir", default=None,
                   help="CMake build directory (for target comparison)")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Show per-file flag diffs")
    p.add_argument("--file-filter", default=None,
                   help="Only compare files matching this regex pattern")
    return p.parse_args()


def find_source_dir(args):
    if args.source_dir:
        return os.path.realpath(args.source_dir)
    return os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))


def run_cmd(cmd, cwd=None, check=True):
    print(f"  $ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, cwd=cwd, check=check,
                            capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  STDERR: {result.stderr[:500]}")
    return result


def find_ninja():
    for name in ["ninja", "ninja-build"]:
        result = subprocess.run(["which", name], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    sys.exit("ERROR: ninja not found in PATH")


def find_cmake():
    result = subprocess.run(["which", "cmake"], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip()
    sys.exit("ERROR: cmake not found in PATH")


# ---------------------------------------------------------------------------
# Configure steps
# ---------------------------------------------------------------------------

def configure_inhouse(source_dir, mode):
    """Run configure.py --mode <mode>.

    configure.py generates build.ninja. We then extract a
    compile_commands.json from it (via ninja -t compdb if needed) and save
    it to a temp location before cmake overwrites the build/<mode>/ directory.
    """
    print(f"\n{'='*60}")
    print(f"Step 1: Configuring IN-HOUSE build (configure.py --mode {mode})")
    print(f"{'='*60}")

    run_cmd([
        sys.executable, os.path.join(source_dir, "configure.py"),
        "--mode", mode,
    ], cwd=source_dir)

    build_dir = os.path.join(source_dir, "build")
    compdb_path = os.path.join(build_dir, mode, "compile_commands.json")

    if not os.path.exists(compdb_path):
        # configure.py may generate the compdb via merge-compdb.py from
        # the build.ninja output.  If it's not there, fall back to
        # extracting it directly with ninja -t compdb.
        print("  compile_commands.json not found, extracting from build.ninja via ninja -t compdb...")
        ninja = find_ninja()
        buildfile = os.path.join(source_dir, "build.ninja")
        result = subprocess.run(
            [ninja, "-f", buildfile, "-t", "compdb"],
            cwd=source_dir, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            try:
                all_entries = json.loads(result.stdout)
                mode_entries = [e for e in all_entries
                                if f"/{mode}/" in e.get("command", "")
                                or f"/{mode}/" in e.get("file", "")]
                os.makedirs(os.path.dirname(compdb_path), exist_ok=True)
                with open(compdb_path, "w") as f:
                    json.dump(mode_entries if mode_entries else all_entries, f)
            except json.JSONDecodeError:
                print("  WARNING: Could not parse ninja compdb output")

    # Save a copy before cmake overwrites the build directory
    saved_path = os.path.join(build_dir, f"{mode}_inhouse_compile_commands.json")
    if os.path.exists(compdb_path):
        shutil.copy2(compdb_path, saved_path)
        print(f"  Saved in-house compdb to {saved_path}")
    else:
        print(f"  ERROR: {compdb_path} not found")
        saved_path = compdb_path

    return build_dir, saved_path


def configure_cmake(source_dir, mode):
    """Run cmake into build/<mode>/, generating its own build.ninja and compile_commands.json."""
    print(f"\n{'='*60}")
    print(f"Step 2: Configuring CMAKE build (mode: {mode})")
    print(f"{'='*60}")
    cmake_build_type = MODE_TO_CMAKE_TYPE[mode]
    build_dir = os.path.join(source_dir, "build", mode)

    cmake = find_cmake()
    ninja = find_ninja()

    run_cmd([
        cmake,
        f"-DCMAKE_BUILD_TYPE={cmake_build_type}",
        f"-DCMAKE_MAKE_PROGRAM={ninja}",
        "-DCMAKE_C_COMPILER=clang",
        "-DCMAKE_CXX_COMPILER=clang++",
        "-G", "Ninja",
        "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
        f"-S", source_dir,
        f"-B", build_dir,
    ], cwd=source_dir)

    compdb_path = os.path.join(build_dir, "compile_commands.json")
    return build_dir, compdb_path


# ---------------------------------------------------------------------------
# compile_commands.json loading and normalization
# (The in-house compdb is derived from build.ninja via ninja -t compdb;
#  the CMake compdb is generated directly by cmake.)
# ---------------------------------------------------------------------------

def load_compile_commands(path):
    """Load compile_commands.json, return dict keyed by source file path."""
    if not os.path.exists(path):
        real = os.path.realpath(path)
        if os.path.exists(real):
            path = real
        else:
            print(f"  WARNING: {path} does not exist")
            return {}
    with open(path) as f:
        entries = json.load(f)
    result = {}
    for entry in entries:
        filepath = entry.get("file", "")
        cmd = entry.get("command", "")
        if not cmd:
            args_list = entry.get("arguments", [])
            cmd = " ".join(args_list) if args_list else ""
        result[filepath] = cmd
    return result


def normalize_path(filepath, source_dir):
    """Normalize a source file path to be relative to source_dir."""
    if os.path.isabs(filepath):
        filepath = os.path.realpath(filepath)
    try:
        return os.path.relpath(filepath, source_dir)
    except ValueError:
        return filepath


def is_scylla_source(rel_path):
    """Return True if this is a Scylla source file (not seastar/abseil)."""
    return (not rel_path.startswith("seastar/") and
            not rel_path.startswith("abseil/") and
            not rel_path.startswith("build") and
            rel_path.endswith(".cc"))


# ---------------------------------------------------------------------------
# Flag extraction and comparison
# ---------------------------------------------------------------------------

def extract_flags(command_str):
    """Extract and categorize compilation flags from a command string."""
    try:
        tokens = shlex.split(command_str)
    except ValueError:
        tokens = command_str.split()

    flags = {
        "defines": set(),
        "warnings": set(),
        "f_flags": set(),
        "opt_flags": set(),
        "arch_flags": set(),
        "std_flags": set(),
    }

    skip_next = False
    for i, tok in enumerate(tokens):
        if skip_next:
            skip_next = False
            continue

        if tok.startswith("-D"):
            flags["defines"].add(tok)
        elif tok.startswith("-W"):
            flags["warnings"].add(tok)
        elif tok.startswith("-f"):
            # Skip file-prefix-map since absolute paths will differ
            if "-ffile-prefix-map=" in tok:
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
            # Include paths differ structurally between build systems, skip
            if tok in ("-I", "-iquote", "-isystem"):
                skip_next = True
            continue

    return flags


def compare_flag_sets(label, set_a, set_b):
    """Compare two sets, return diff strings."""
    only_a = set_a - set_b
    only_b = set_b - set_a
    diffs = []
    if only_a:
        diffs.append(f"{label}: only in configure.py: {sorted(only_a)}")
    if only_b:
        diffs.append(f"{label}: only in CMake:         {sorted(only_b)}")
    return diffs


def compare_compile_commands(inhouse_compdb, cmake_compdb, source_dir,
                              file_filter=None, verbose=False):
    """Compare compilation flags between the two build systems."""
    print(f"\n{'='*60}")
    print("Comparing compilation flags")
    print(f"{'='*60}")

    # Build relative-path dicts
    inhouse_by_rel = {}
    for filepath, cmd in inhouse_compdb.items():
        rel = normalize_path(filepath, source_dir)
        if is_scylla_source(rel):
            if not file_filter or re.search(file_filter, rel):
                inhouse_by_rel[rel] = cmd

    cmake_by_rel = {}
    for filepath, cmd in cmake_compdb.items():
        rel = normalize_path(filepath, source_dir)
        if is_scylla_source(rel):
            if not file_filter or re.search(file_filter, rel):
                cmake_by_rel[rel] = cmd

    common_files = set(inhouse_by_rel) & set(cmake_by_rel)
    only_inhouse = set(inhouse_by_rel) - set(cmake_by_rel)
    only_cmake = set(cmake_by_rel) - set(inhouse_by_rel)

    print(f"\n  Source files in both:              {len(common_files)}")
    print(f"  Source files only in configure.py: {len(only_inhouse)}")
    print(f"  Source files only in CMake:        {len(only_cmake)}")

    if only_inhouse:
        print("\n  Files only in configure.py:")
        for f in sorted(only_inhouse):
            print(f"    {f}")

    if only_cmake:
        print("\n  Files only in CMake:")
        for f in sorted(only_cmake):
            print(f"    {f}")

    # Compare flags for common files
    files_with_diffs = 0
    aggregate_diffs = defaultdict(int)

    for rel_file in sorted(common_files):
        ih_flags = extract_flags(inhouse_by_rel[rel_file])
        cm_flags = extract_flags(cmake_by_rel[rel_file])

        file_diffs = []
        for category in ("defines", "warnings", "f_flags", "opt_flags",
                          "arch_flags", "std_flags"):
            d = compare_flag_sets(category, ih_flags[category], cm_flags[category])
            file_diffs.extend(d)

            for flag in ih_flags[category] - cm_flags[category]:
                aggregate_diffs[f"only-configure.py  {category}: {flag}"] += 1
            for flag in cm_flags[category] - ih_flags[category]:
                aggregate_diffs[f"only-cmake         {category}: {flag}"] += 1

        if file_diffs:
            files_with_diffs += 1
            if verbose:
                print(f"\n  DIFF in {rel_file}:")
                for d in file_diffs:
                    print(f"      {d}")

    print(f"\n  Files with flag differences: {files_with_diffs} / {len(common_files)}")

    if aggregate_diffs:
        print("\n  Aggregate flag differences (flag → # files affected):")
        for diff_key, count in sorted(aggregate_diffs.items(), key=lambda x: -x[1]):
            print(f"    {diff_key}  ({count} files)")

    return files_with_diffs == 0 and not only_inhouse and not only_cmake


# ---------------------------------------------------------------------------
# Target comparison
# ---------------------------------------------------------------------------

def get_ninja_targets(cwd, ninja_path, buildfile=None):
    """Get ninja targets as set of (target, rule) tuples."""
    cmd = [ninja_path]
    if buildfile:
        cmd += ["-f", buildfile]
    cmd += ["-t", "targets", "all"]
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        return set()
    targets = set()
    for line in result.stdout.strip().split("\n"):
        if ": " in line:
            target, rule = line.split(": ", 1)
            targets.add((target.strip(), rule.strip()))
    return targets


def extract_link_targets(targets, strip_prefix=""):
    """Return set of target names that are produced by link rules."""
    exes = set()
    for target, rule in targets:
        rule_lower = rule.lower()
        if ("link" in rule_lower or "cxx_executable_linker" in rule_lower
                or rule.startswith("LINK") or rule.startswith("link")):
            name = target
            if strip_prefix and name.startswith(strip_prefix):
                name = name[len(strip_prefix):]
            if name.endswith(".stripped") or name.endswith(".debug"):
                continue
            exes.add(name)
    return exes


# Known target renames: configure.py name -> cmake name
# These arise because CMake requires globally unique target names.
KNOWN_RENAMES = {
    "test/manual/hint_test": "test/manual/manual_hint_test",
    "test/manual/message": "test/manual/message_test",
    "test/ldap/role_manager_test": "test/ldap/ldap_role_manager_test",
}


def compare_targets(source_dir, inhouse_build_dir, cmake_build_dir,
                     mode, ninja_path):
    """Compare executable targets from both build systems."""
    print(f"\n{'='*60}")
    print("Comparing build targets (linked executables)")
    print(f"{'='*60}")

    # In-house targets
    buildfile = os.path.join(source_dir, "build.ninja")
    if os.path.exists(buildfile):
        ih_all = get_ninja_targets(source_dir, ninja_path, buildfile=buildfile)
        # Only consider targets that belong to this mode
        mode_prefix = f"build/{mode}/"
        ih_mode_targets = {(t, r) for t, r in ih_all
                           if t.startswith(mode_prefix)}
        ih_exes = extract_link_targets(ih_mode_targets,
                                       strip_prefix=mode_prefix)
    else:
        ih_all = get_ninja_targets(inhouse_build_dir, ninja_path)
        ih_exes = extract_link_targets(ih_all)

    # CMake targets
    if cmake_build_dir and os.path.isdir(cmake_build_dir):
        cm_all = get_ninja_targets(cmake_build_dir, ninja_path)
        cm_exes = extract_link_targets(cm_all)
    else:
        cm_exes = set()

    if not ih_exes and not cm_exes:
        print("  Could not extract targets from either build system.")
        print("  Run without --skip-configure, or ensure both build systems")
        print("  are configured with their build.ninja files present.")
        return True

    print(f"\n  Linked targets in configure.py: {len(ih_exes)}")
    print(f"  Linked targets in CMake:        {len(cm_exes)}")

    only_ih = ih_exes - cm_exes
    only_cm = cm_exes - ih_exes

    if not only_ih and not only_cm:
        print("  ✓ All targets match!")
        return True

    # Account for known renames
    unexplained_ih = set()
    unexplained_cm = set(only_cm)
    renamed = []
    for t in only_ih:
        cmake_name = KNOWN_RENAMES.get(t)
        if cmake_name and cmake_name in unexplained_cm:
            renamed.append((t, cmake_name))
            unexplained_cm.discard(cmake_name)
        else:
            unexplained_ih.add(t)

    if renamed:
        print(f"\n  Known renames ({len(renamed)}):")
        for old, new in sorted(renamed):
            print(f"    {old}  →  {new}")

    if unexplained_ih:
        print(f"\n  ✗ Targets only in configure.py ({len(unexplained_ih)}):")
        for t in sorted(unexplained_ih):
            print(f"    {t}")

    if unexplained_cm:
        print(f"\n  ✗ Targets only in CMake ({len(unexplained_cm)}):")
        for t in sorted(unexplained_cm):
            print(f"    {t}")

    has_unexplained = bool(unexplained_ih or unexplained_cm)
    return not has_unexplained


# ---------------------------------------------------------------------------
# Ninja file parsing and link comparison
# ---------------------------------------------------------------------------

def _parse_ninja_vars_and_builds(filepath):
    """Parse a ninja file to extract top-level variables and build statements.

    Returns (variables_dict, list_of_build_dicts).
    Each build dict has keys: outputs, rule, inputs, implicit, order_only, vars.
    Also follows 'subninja' and 'include' directives.
    """
    variables = {}
    builds = []
    rules = {}  # rule_name -> {command: ..., ...}

    def parse_file(path, into_vars, into_builds, into_rules):
        base_dir = os.path.dirname(path)
        try:
            with open(path) as f:
                lines = f.readlines()
        except FileNotFoundError:
            return

        i = 0
        while i < len(lines):
            line = lines[i].rstrip('\n')

            # Skip empty/comment
            if not line or line.startswith('#'):
                i += 1
                continue

            # subninja / include
            m = re.match(r'^(subninja|include)\s+(.+)', line)
            if m:
                inc_path = m.group(2).strip()
                if not os.path.isabs(inc_path):
                    inc_path = os.path.join(base_dir, inc_path)
                parse_file(inc_path, into_vars, into_builds, into_rules)
                i += 1
                continue

            # Rule definition
            m = re.match(r'^rule\s+(\S+)', line)
            if m:
                rule_name = m.group(1)
                rule_vars = {}
                i += 1
                while i < len(lines) and lines[i].startswith('  '):
                    rline = lines[i].strip()
                    rm = re.match(r'(\S+)\s*=\s*(.*)', rline)
                    if rm:
                        rule_vars[rm.group(1)] = rm.group(2)
                    i += 1
                into_rules[rule_name] = rule_vars
                continue

            # Top-level variable
            m = re.match(r'^([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*(.*)', line)
            if m and not line.startswith(' '):
                into_vars[m.group(1)] = m.group(2)
                i += 1
                continue

            # Build statement
            m = re.match(r'^build\s+(.+?):\s+(\S+)\s*(.*)', line)
            if m:
                outputs_str = m.group(1)
                rule = m.group(2)
                rest = m.group(3)

                # Collect continuation lines
                i += 1
                build_vars = {}
                while i < len(lines) and lines[i].startswith('  '):
                    bline = lines[i].strip()
                    bm = re.match(r'(\S+)\s*=\s*(.*)', bline)
                    if bm:
                        build_vars[bm.group(1)] = bm.group(2)
                    i += 1

                # Split inputs by | and ||
                parts = re.split(r'\s*\|\|\s*|\s*\|\s*', rest)
                explicit = parts[0].strip() if len(parts) > 0 else ""
                implicit = parts[1].strip() if len(parts) > 1 else ""

                into_builds.append({
                    'outputs': outputs_str.strip(),
                    'rule': rule,
                    'inputs': explicit,
                    'implicit': implicit,
                    'vars': build_vars,
                })
                continue

            i += 1

    parse_file(filepath, variables, builds, rules)
    return variables, builds, rules


def _resolve_var(value, variables, depth=0):
    """Resolve $var and ${var} references in a string."""
    if depth > 10 or '$' not in value:
        return value
    def repl(m):
        name = m.group(1) or m.group(2)
        return variables.get(name, '')
    result = re.sub(r'\$\{(\w+)\}|\$(\w+)', repl, value)
    if '$' in result and result != value:
        return _resolve_var(result, variables, depth + 1)
    return result


def _extract_link_builds(builds, variables, rules, mode, strip_prefix=""):
    """Extract link build statements and resolve their commands.

    Returns dict: target_name -> {
        'linker_flags': set of flags,
        'libraries': set of library names,
        'raw_command': str
    }
    """
    result = {}

    for build in builds:
        rule = build['rule']

        # Detect link rules
        is_link = False
        if rule.startswith('link.') or rule.startswith('link_build.'):
            is_link = True
        elif 'EXECUTABLE_LINKER' in rule:
            is_link = True

        if not is_link:
            continue

        # Get target name
        target = build['outputs']
        target = target.replace('$builddir/', 'build/')

        if strip_prefix and target.startswith(strip_prefix):
            target = target[len(strip_prefix):]

        # Skip stripped/debug variants
        if target.endswith('.stripped') or target.endswith('.debug'):
            continue
        # In-house names _g suffix for unstripped
        if target.endswith('_g'):
            target = target[:-2]

        # Merge variables: global < rule < build-level
        merged_vars = dict(variables)
        rule_def = rules.get(rule, {})
        merged_vars.update(rule_def)
        merged_vars.update(build['vars'])

        # Resolve the command
        command_template = rule_def.get('command', '')
        # Set $in, $out, $out for template resolution
        merged_vars['in'] = build['inputs']
        merged_vars['out'] = build['outputs']

        command = _resolve_var(command_template, merged_vars)

        # Extract linker flags and libraries
        linker_flags = set()
        libraries = set()

        try:
            tokens = shlex.split(command)
        except ValueError:
            tokens = command.split()

        skip = False
        for j, tok in enumerate(tokens):
            if skip:
                skip = False
                continue
            if tok in ('-o', '-MF', '-MT'):
                skip = True
                continue

            # Linker flags
            if tok.startswith('-fsanitize'):
                linker_flags.add(tok)
            elif tok.startswith('-flto') or tok == '-fno-lto':
                linker_flags.add(tok)
            elif tok.startswith('-fuse-ld=') or tok.startswith('--ld-path='):
                # Normalize: both mean "use this linker"
                linker_name = tok.split('=', 1)[1]
                # Strip path prefix (ld.lld -> lld, /usr/bin/ld.lld -> lld)
                linker_name = os.path.basename(linker_name)
                if linker_name.startswith('ld.'):
                    linker_name = linker_name[3:]
                linker_flags.add(f'linker={linker_name}')
            elif tok.startswith('-Wl,'):
                if '--dynamic-linker' in tok:
                    linker_flags.add('-Wl,--dynamic-linker=<padded>')
                elif '-rpath' in tok:
                    # Normalize rpath: just note that rpath is used
                    linker_flags.add('-Wl,-rpath=<paths>')
                elif '--build-id' in tok:
                    # Extract just the build-id type
                    for part in tok.split(','):
                        if '--build-id' in part:
                            linker_flags.add(f'-Wl,{part}')
                elif '--strip' in tok:
                    linker_flags.add(tok)
                else:
                    # Keep other -Wl flags but normalize paths
                    if '-Bstatic' in tok or '-Bdynamic' in tok:
                        pass  # skip static/dynamic link mode toggles
                    else:
                        linker_flags.add(tok)
            elif tok == '-static-libstdc++':
                linker_flags.add(tok)

            # Libraries: normalize to canonical name
            if tok.startswith('-l'):
                lib_name = tok[2:]  # strip -l
                libraries.add(lib_name)
            elif tok.endswith('.a') or '.so' in tok:
                if tok.endswith('.o'):
                    continue  # skip .o files
                basename = os.path.basename(tok)
                m = re.match(r'lib(.+?)\.(?:a|so(?:\.\S*)?)', basename)
                if m:
                    libraries.add(m.group(1))

        # Also extract libraries from build inputs (implicit deps)
        all_inputs = build['inputs'] + ' ' + build.get('implicit', '')
        all_inputs = _resolve_var(all_inputs, merged_vars)
        for tok in all_inputs.split():
            tok = tok.strip()
            if tok.endswith('.o'):
                continue
            if tok.endswith('.a') or '.so' in tok:
                basename = os.path.basename(tok)
                m = re.match(r'lib(.+?)\.(?:a|so(?:\.\S*)?)', basename)
                if m:
                    libraries.add(m.group(1))

        result[target] = {
            'linker_flags': linker_flags,
            'libraries': libraries,
            'raw_command': command,
        }

    return result


def compare_ninja_builds(source_dir, cmake_build_dir, mode, verbose=False):
    """Compare link commands from in-house and CMake build.ninja files.

    Returns True if no significant differences found.
    """
    print(f"\n{'='*60}")
    print("Comparing linker flags and libraries (build.ninja)")
    print(f"{'='*60}")

    # Parse in-house build.ninja (at source root)
    ih_ninja = os.path.join(source_dir, "build.ninja")
    if not os.path.exists(ih_ninja):
        print("  In-house build.ninja not found, skipping ninja comparison")
        return True

    # Parse CMake build.ninja
    cm_ninja = os.path.join(cmake_build_dir, "build.ninja")
    if not os.path.exists(cm_ninja):
        print("  CMake build.ninja not found, skipping ninja comparison")
        return True

    print(f"  In-house: {ih_ninja}")
    print(f"  CMake:    {cm_ninja}")

    ih_vars, ih_builds, ih_rules = _parse_ninja_vars_and_builds(ih_ninja)
    cm_vars, cm_builds, cm_rules = _parse_ninja_vars_and_builds(cm_ninja)

    ih_links = _extract_link_builds(
        ih_builds, ih_vars, ih_rules, mode,
        strip_prefix=f"build/{mode}/")
    cm_links = _extract_link_builds(
        cm_builds, cm_vars, cm_rules, mode)

    # Normalize CMake target paths (strip Debug/ prefix if present)
    cm_links_normalized = {}
    for target, data in cm_links.items():
        t = target
        cmake_type = MODE_TO_CMAKE_TYPE.get(mode, '')
        if t.startswith(f"{cmake_type}/"):
            t = t[len(cmake_type) + 1:]
        cm_links_normalized[t] = data
    cm_links = cm_links_normalized

    # Apply known renames
    ih_links_renamed = {}
    for t, data in ih_links.items():
        ih_links_renamed[KNOWN_RENAMES.get(t, t)] = data
    ih_links = ih_links_renamed

    ih_targets = set(ih_links.keys())
    cm_targets = set(cm_links.keys())
    common = sorted(ih_targets & cm_targets)

    print(f"\n  Linked targets in-house:  {len(ih_targets)}")
    print(f"  Linked targets CMake:    {len(cm_targets)}")
    print(f"  Common targets:          {len(common)}")

    # Compare linker flags and libraries
    flag_diffs = 0
    lib_diffs = 0
    flag_agg_ih = defaultdict(int)
    flag_agg_cm = defaultdict(int)
    lib_agg_ih = defaultdict(int)
    lib_agg_cm = defaultdict(int)

    # Internal project libraries that CMake builds as static/shared archives
    # but in-house links as individual .o files or via different mechanisms.
    # These are expected structural differences, not real discrepancies.
    INTERNAL_PROJECT_LIBS = {
        # Scylla component libraries
        'api', 'alternator', 'cdc', 'compaction', 'cql3',
        'data_dictionary', 'db', 'gms', 'index', 'lang', 'message',
        'mutation', 'mutation_writer', 'raft', 'readers', 'repair',
        'replica', 'schema', 'service', 'sstables', 'streaming',
        'transport', 'types', 'utils', 'vector_search',
        'scylla-main', 'scylla-precompiled-header',
        'scylla_audit', 'scylla_auth', 'scylla_dht',
        'scylla_encryption', 'scylla_locator', 'scylla_tracing',
        'test-lib', 'test-perf', 'test-raft', 'tools',
        'ldap',  # ent/ldap/libldap.a (project) vs -lldap (system)
        'wasmtime_bindings', 'rust_combined', 'inc',
        # Seastar
        'seastar_testing', 'seastar_perf_testing',
    }

    # Abseil component libraries — CMake links them individually as
    # shared libs, in-house links them as static archives built via
    # a sub-ninja invocation.  The component names differ.
    ABSEIL_LIB_PREFIX = 'absl_'

    for target in common:
        ih = ih_links[target]
        cm = cm_links[target]

        # Linker flags diff
        ih_flags = ih['linker_flags']
        cm_flags = cm['linker_flags']
        only_ih_flags = ih_flags - cm_flags
        only_cm_flags = cm_flags - ih_flags
        if only_ih_flags or only_cm_flags:
            flag_diffs += 1
            for f in only_ih_flags:
                flag_agg_ih[f] += 1
            for f in only_cm_flags:
                flag_agg_cm[f] += 1
            if verbose:
                print(f"\n  {target}:")
                if only_ih_flags:
                    print(f"    Linker flags only in configure.py: "
                          f"{sorted(only_ih_flags)}")
                if only_cm_flags:
                    print(f"    Linker flags only in CMake:        "
                          f"{sorted(only_cm_flags)}")

        # Library diff — exclude internal project archives and abseil components
        ih_libs = ih['libraries']
        cm_libs = cm['libraries']
        only_ih_libs = {lib for lib in (ih_libs - cm_libs)
                        if lib not in INTERNAL_PROJECT_LIBS
                        and not lib.startswith(ABSEIL_LIB_PREFIX)}
        only_cm_libs = {lib for lib in (cm_libs - ih_libs)
                        if lib not in INTERNAL_PROJECT_LIBS
                        and not lib.startswith(ABSEIL_LIB_PREFIX)}
        if only_ih_libs or only_cm_libs:
            lib_diffs += 1
            for lib in only_ih_libs:
                lib_agg_ih[lib] += 1
            for lib in only_cm_libs:
                lib_agg_cm[lib] += 1
            if verbose:
                print(f"\n  {target}:")
                if only_ih_libs:
                    print(f"    Libs only in configure.py: "
                          f"{sorted(only_ih_libs)}")
                if only_cm_libs:
                    print(f"    Libs only in CMake:        "
                          f"{sorted(only_cm_libs)}")

    # Print aggregate results
    all_ok = True

    print(f"\n  Linker flag differences: {flag_diffs} / {len(common)}")
    if flag_agg_ih or flag_agg_cm:
        all_ok = False
        print(f"\n  Aggregate linker flag differences:")
        for flag, count in sorted(flag_agg_ih.items(), key=lambda x: -x[1]):
            print(f"    only-configure.py  {flag}  ({count} targets)")
        for flag, count in sorted(flag_agg_cm.items(), key=lambda x: -x[1]):
            print(f"    only-cmake         {flag}  ({count} targets)")

    print(f"\n  Library differences: {lib_diffs} / {len(common)}")
    if lib_agg_ih or lib_agg_cm:
        all_ok = False
        print(f"\n  Aggregate library differences:")
        for lib, count in sorted(lib_agg_ih.items(), key=lambda x: -x[1]):
            print(f"    only-configure.py  {lib}  ({count} targets)")
        for lib, count in sorted(lib_agg_cm.items(), key=lambda x: -x[1]):
            print(f"    only-cmake         {lib}  ({count} targets)")

    if all_ok:
        print("  ✓ Linker flags and libraries match for all common targets!")

    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

class Discrepancy:
    """A single discrepancy found between build systems."""
    # Categories
    COMP_UNIT_MISSING = "compilation_unit_missing"
    COMPILE_FLAGS     = "compile_flags_differ"
    TARGET_MISSING    = "target_missing"
    LINKER_FLAGS      = "linker_flags_differ"
    LINKER_LIBS       = "linker_libs_differ"

    def __init__(self, category, mode, description, details=None):
        self.category = category
        self.mode = mode
        self.description = description
        self.details = details or []

    def __str__(self):
        return f"[{self.mode}] {self.category}: {self.description}"


def collect_compile_discrepancies(inhouse_compdb, cmake_compdb, source_dir,
                                  mode, file_filter=None):
    """Collect structured discrepancies from compile flags comparison.

    The in-house compdb is extracted from build.ninja; the CMake compdb
    is generated directly by cmake.

    Returns (ok, list_of_Discrepancy).
    """
    discs = []

    inhouse_by_rel = {}
    for filepath, cmd in inhouse_compdb.items():
        rel = normalize_path(filepath, source_dir)
        if is_scylla_source(rel):
            if not file_filter or re.search(file_filter, rel):
                inhouse_by_rel[rel] = cmd

    cmake_by_rel = {}
    for filepath, cmd in cmake_compdb.items():
        rel = normalize_path(filepath, source_dir)
        if is_scylla_source(rel):
            if not file_filter or re.search(file_filter, rel):
                cmake_by_rel[rel] = cmd

    only_inhouse = sorted(set(inhouse_by_rel) - set(cmake_by_rel))
    only_cmake = sorted(set(cmake_by_rel) - set(inhouse_by_rel))

    for f in only_inhouse:
        discs.append(Discrepancy(
            Discrepancy.COMP_UNIT_MISSING, mode,
            f"compilation unit '{f}' is in configure.py but NOT in cmake"))
    for f in only_cmake:
        discs.append(Discrepancy(
            Discrepancy.COMP_UNIT_MISSING, mode,
            f"compilation unit '{f}' is in cmake but NOT in configure.py"))

    common_files = sorted(set(inhouse_by_rel) & set(cmake_by_rel))
    for rel_file in common_files:
        ih_flags = extract_flags(inhouse_by_rel[rel_file])
        cm_flags = extract_flags(cmake_by_rel[rel_file])

        file_details = []
        for category in ("defines", "warnings", "f_flags", "opt_flags",
                          "arch_flags", "std_flags"):
            only_ih = ih_flags[category] - cm_flags[category]
            only_cm = cm_flags[category] - ih_flags[category]
            if only_ih:
                file_details.append(
                    f"  {category} only in configure.py: {sorted(only_ih)}")
            if only_cm:
                file_details.append(
                    f"  {category} only in cmake: {sorted(only_cm)}")

        if file_details:
            discs.append(Discrepancy(
                Discrepancy.COMPILE_FLAGS, mode,
                f"compilation unit '{rel_file}' has different compile flags",
                details=file_details))

    ok = not discs
    return ok, discs


def collect_target_discrepancies(source_dir, inhouse_build_dir,
                                 cmake_build_dir, mode, ninja_path):
    """Collect structured discrepancies from target comparison.

    Returns (ok, list_of_Discrepancy).
    """
    discs = []

    buildfile = os.path.join(source_dir, "build.ninja")
    if os.path.exists(buildfile):
        ih_all = get_ninja_targets(source_dir, ninja_path, buildfile=buildfile)
        # The in-house build.ninja may contain targets for multiple modes.
        # Only consider targets that belong to this mode.
        mode_prefix = f"build/{mode}/"
        ih_mode_targets = {(t, r) for t, r in ih_all
                           if t.startswith(mode_prefix)}
        if not ih_mode_targets:
            # This mode was not configured in-house
            return True, discs
        ih_exes = extract_link_targets(ih_mode_targets,
                                       strip_prefix=mode_prefix)
    else:
        ih_all = get_ninja_targets(inhouse_build_dir, ninja_path)
        ih_exes = extract_link_targets(ih_all)

    if cmake_build_dir and os.path.isdir(cmake_build_dir):
        cm_all = get_ninja_targets(cmake_build_dir, ninja_path)
        cm_exes = extract_link_targets(cm_all)
    else:
        cm_exes = set()

    if not ih_exes and not cm_exes:
        return True, discs

    # Apply known renames
    only_ih = ih_exes - cm_exes
    only_cm = cm_exes - ih_exes
    unexplained_ih = set()
    unexplained_cm = set(only_cm)
    for t in only_ih:
        cmake_name = KNOWN_RENAMES.get(t)
        if cmake_name and cmake_name in unexplained_cm:
            unexplained_cm.discard(cmake_name)
        else:
            unexplained_ih.add(t)

    for t in sorted(unexplained_ih):
        discs.append(Discrepancy(
            Discrepancy.TARGET_MISSING, mode,
            f"target '{t}' is in configure.py but NOT in cmake"))
    for t in sorted(unexplained_cm):
        discs.append(Discrepancy(
            Discrepancy.TARGET_MISSING, mode,
            f"target '{t}' is in cmake but NOT in configure.py"))

    ok = not discs
    return ok, discs


def collect_ninja_discrepancies(source_dir, cmake_build_dir, mode):
    """Collect structured discrepancies from build.ninja link comparison.

    Returns (ok, list_of_Discrepancy).
    """
    discs = []

    ih_ninja = os.path.join(source_dir, "build.ninja")
    cm_ninja = os.path.join(cmake_build_dir, "build.ninja")
    if not os.path.exists(ih_ninja) or not os.path.exists(cm_ninja):
        return True, discs

    ih_vars, ih_builds, ih_rules = _parse_ninja_vars_and_builds(ih_ninja)
    cm_vars, cm_builds, cm_rules = _parse_ninja_vars_and_builds(cm_ninja)

    ih_links = _extract_link_builds(
        ih_builds, ih_vars, ih_rules, mode,
        strip_prefix=f"build/{mode}/")
    cm_links = _extract_link_builds(
        cm_builds, cm_vars, cm_rules, mode)

    # Normalize CMake target paths
    cm_links_norm = {}
    cmake_type = MODE_TO_CMAKE_TYPE.get(mode, '')
    for target, data in cm_links.items():
        t = target
        if t.startswith(f"{cmake_type}/"):
            t = t[len(cmake_type) + 1:]
        cm_links_norm[t] = data
    cm_links = cm_links_norm

    # Apply known renames
    ih_links_renamed = {}
    for t, data in ih_links.items():
        ih_links_renamed[KNOWN_RENAMES.get(t, t)] = data
    ih_links = ih_links_renamed

    common = sorted(set(ih_links) & set(cm_links))

    INTERNAL_PROJECT_LIBS = {
        'api', 'alternator', 'cdc', 'compaction', 'cql3',
        'data_dictionary', 'db', 'gms', 'index', 'lang', 'message',
        'mutation', 'mutation_writer', 'raft', 'readers', 'repair',
        'replica', 'schema', 'service', 'sstables', 'streaming',
        'transport', 'types', 'utils', 'vector_search',
        'scylla-main', 'scylla-precompiled-header',
        'scylla_audit', 'scylla_auth', 'scylla_dht',
        'scylla_encryption', 'scylla_locator', 'scylla_tracing',
        'test-lib', 'test-perf', 'test-raft', 'tools',
        'ldap', 'wasmtime_bindings', 'rust_combined', 'inc',
        'seastar_testing', 'seastar_perf_testing',
    }
    ABSEIL_LIB_PREFIX = 'absl_'

    for target in common:
        ih = ih_links[target]
        cm = cm_links[target]

        # Linker flags
        only_ih_flags = ih['linker_flags'] - cm['linker_flags']
        only_cm_flags = cm['linker_flags'] - ih['linker_flags']
        if only_ih_flags or only_cm_flags:
            details = []
            if only_ih_flags:
                details.append(
                    f"  linker flags only in configure.py: {sorted(only_ih_flags)}")
            if only_cm_flags:
                details.append(
                    f"  linker flags only in cmake: {sorted(only_cm_flags)}")
            discs.append(Discrepancy(
                Discrepancy.LINKER_FLAGS, mode,
                f"binary '{target}' has different linker flags",
                details=details))

        # Libraries (filter internal/abseil)
        only_ih_libs = {lib for lib in (ih['libraries'] - cm['libraries'])
                        if lib not in INTERNAL_PROJECT_LIBS
                        and not lib.startswith(ABSEIL_LIB_PREFIX)}
        only_cm_libs = {lib for lib in (cm['libraries'] - ih['libraries'])
                        if lib not in INTERNAL_PROJECT_LIBS
                        and not lib.startswith(ABSEIL_LIB_PREFIX)}
        if only_ih_libs or only_cm_libs:
            details = []
            if only_ih_libs:
                details.append(
                    f"  libraries only in configure.py: {sorted(only_ih_libs)}")
            if only_cm_libs:
                details.append(
                    f"  libraries only in cmake: {sorted(only_cm_libs)}")
            discs.append(Discrepancy(
                Discrepancy.LINKER_LIBS, mode,
                f"binary '{target}' links different libraries",
                details=details))

    ok = not discs
    return ok, discs


def run_comparison(source_dir, mode, args, ninja_path):
    """Run comparison for a single mode.

    Returns (flags_ok, targets_ok, ninja_ok, all_discrepancies).
    """
    cmake_build_type = MODE_TO_CMAKE_TYPE[mode]
    ci_mode = getattr(args, 'ci', False)

    if not ci_mode:
        print(f"\n{'#'*60}")
        print(f"# Mode: {mode} (CMake: {cmake_build_type})")
        print(f"{'#'*60}")

    inhouse_compdb_path = None
    cmake_compdb_path = None
    inhouse_build_dir = args.inhouse_build_dir or os.path.join(source_dir, "build")
    cmake_build_dir = args.cmake_build_dir

    if args.skip_configure:
        if args.inhouse_compdb:
            p = args.inhouse_compdb
            inhouse_compdb_path = p if os.path.isabs(p) else os.path.join(source_dir, p)
        else:
            # Look for saved copy first, then fall back to the live file
            for c in [
                os.path.join(source_dir, "build",
                             f"{mode}_inhouse_compile_commands.json"),
                os.path.join(source_dir, "build", mode,
                             "compile_commands.json"),
                os.path.join(source_dir, "compile_commands.json"),
            ]:
                real = os.path.realpath(c)
                if os.path.exists(real):
                    inhouse_compdb_path = real
                    break

        if args.cmake_compdb:
            p = args.cmake_compdb
            cmake_compdb_path = p if os.path.isabs(p) else os.path.join(source_dir, p)
        else:
            # CMake generates compile_commands.json in build/<mode>/
            for c in [
                os.path.join(source_dir, "build", mode,
                             "compile_commands.json"),
            ]:
                if os.path.exists(c):
                    cmake_compdb_path = c
                    break
    else:
        inhouse_build_dir, inhouse_compdb_path = configure_inhouse(source_dir, mode)
        cmake_build_dir_out, cmake_compdb_path = configure_cmake(source_dir, mode)
        cmake_build_dir = cmake_build_dir_out

    # Result sentinel: None means "skipped" (no artifacts for this mode)
    SKIPPED = (None, None, None, [])

    if not inhouse_compdb_path or not os.path.exists(str(inhouse_compdb_path)):
        msg = (f"In-house compile_commands.json (derived from build.ninja) not found for mode '{mode}'."
               f" Looked for: {inhouse_compdb_path}")
        if not ci_mode:
            print(f"\nERROR: {msg}")
            print(f"  Run configure.py first, or use --inhouse-compdb to specify path.")
        return SKIPPED

    if not cmake_compdb_path or not os.path.exists(str(cmake_compdb_path)):
        msg = (f"CMake compile_commands.json not found for mode '{mode}'."
               f" Looked for: {cmake_compdb_path}")
        if not ci_mode:
            print(f"\nERROR: {msg}")
            print(f"  Run cmake first, or use --cmake-compdb to specify path.")
        return SKIPPED

    if not ci_mode:
        print(f"\nIn-house compdb (from build.ninja): {inhouse_compdb_path}")
        print(f"CMake compile_commands.json:        {cmake_compdb_path}")

    inhouse_compdb = load_compile_commands(inhouse_compdb_path)
    cmake_compdb = load_compile_commands(cmake_compdb_path)

    if not ci_mode:
        print(f"\n  In-house entries: {len(inhouse_compdb)}")
        print(f"  CMake entries:    {len(cmake_compdb)}")

    if not inhouse_compdb:
        if not ci_mode:
            print(f"\nERROR: In-house compile_commands.json (from build.ninja) is empty or invalid")
        return SKIPPED
    if not cmake_compdb:
        if not ci_mode:
            print(f"\nERROR: CMake compile_commands.json is empty or invalid")
        return SKIPPED

    all_discs = []

    # Compare compilation flags
    if ci_mode:
        flags_ok, compile_discs = collect_compile_discrepancies(
            inhouse_compdb, cmake_compdb, source_dir, mode,
            file_filter=args.file_filter)
        all_discs.extend(compile_discs)
    else:
        flags_ok = compare_compile_commands(
            inhouse_compdb, cmake_compdb, source_dir,
            file_filter=args.file_filter, verbose=args.verbose)

    # Compare targets
    cmake_bd = cmake_build_dir or os.path.join(source_dir, "build", mode)
    if ci_mode:
        targets_ok, target_discs = collect_target_discrepancies(
            source_dir, inhouse_build_dir, cmake_bd, mode, ninja_path)
        all_discs.extend(target_discs)
    else:
        targets_ok = compare_targets(
            source_dir, inhouse_build_dir, cmake_bd, mode, ninja_path)

    # Compare linker flags and libraries from build.ninja
    if ci_mode:
        ninja_ok, ninja_discs = collect_ninja_discrepancies(
            source_dir, cmake_bd, mode)
        all_discs.extend(ninja_discs)
    else:
        ninja_ok = compare_ninja_builds(
            source_dir, cmake_bd, mode, verbose=args.verbose)

    return flags_ok, targets_ok, ninja_ok, all_discs


def _print_ci_report(results, modes_to_check):
    """Print a CI-friendly report with structured hints.

    Returns exit code: 0 if all checked modes pass, 1 otherwise.
    """
    all_ok = True
    all_discs = []
    checked_modes = []
    skipped_modes = []

    for mode in modes_to_check:
        flags_ok, targets_ok, ninja_ok, discs = results[mode]
        if flags_ok is None:
            # Mode was skipped (no build artifacts)
            skipped_modes.append(mode)
            continue
        checked_modes.append(mode)
        mode_ok = flags_ok and targets_ok and ninja_ok
        all_ok = all_ok and mode_ok
        all_discs.extend(discs)

    if not checked_modes:
        print("ERROR: no modes had build artifacts to compare."
              " Run without --skip-configure to configure both build systems.")
        return 1

    if skipped_modes:
        print(f"NOTE: skipped modes (no build artifacts): "
              f"{', '.join(skipped_modes)}")
        print()

    if all_ok:
        print("OK: configure.py and cmake build systems are identical "
              f"for modes: {', '.join(checked_modes)}")
        return 0

    # Group discrepancies by category for readability
    by_category = defaultdict(list)
    for d in all_discs:
        by_category[d.category].append(d)

    category_labels = {
        Discrepancy.COMP_UNIT_MISSING: "COMPILATION UNIT MISSING",
        Discrepancy.COMPILE_FLAGS:     "COMPILE FLAGS DIFFER",
        Discrepancy.TARGET_MISSING:    "TARGET MISSING",
        Discrepancy.LINKER_FLAGS:      "LINKER FLAGS DIFFER",
        Discrepancy.LINKER_LIBS:       "LINKER LIBRARIES DIFFER",
    }

    total = len(all_discs)
    print(f"FAIL: {total} discrepancies found between configure.py "
          f"and cmake for modes: {', '.join(checked_modes)}")
    print()

    for cat in (Discrepancy.COMP_UNIT_MISSING,
                Discrepancy.COMPILE_FLAGS,
                Discrepancy.TARGET_MISSING,
                Discrepancy.LINKER_FLAGS,
                Discrepancy.LINKER_LIBS):
        items = by_category.get(cat, [])
        if not items:
            continue
        label = category_labels[cat]
        print(f"--- {label} ({len(items)}) ---")
        for d in items:
            print(f"  [{d.mode}] {d.description}")
            for detail in d.details:
                print(f"    {detail}")
        print()

    # Aggregate compile-flag summary across all files and modes
    compile_flag_items = by_category.get(Discrepancy.COMPILE_FLAGS, [])
    if compile_flag_items:
        agg = defaultdict(int)
        for d in compile_flag_items:
            for detail in d.details:
                agg[detail.strip()] += 1
        print("--- AGGREGATE COMPILE FLAG DIFFS (flag difference → # files) ---")
        for diff_key, count in sorted(agg.items(), key=lambda x: -x[1]):
            print(f"  {diff_key}  ({count} files)")
        print()

    # Aggregate linker-flag summary across all targets and modes
    linker_flag_items = by_category.get(Discrepancy.LINKER_FLAGS, [])
    if linker_flag_items:
        agg = defaultdict(int)
        for d in linker_flag_items:
            for detail in d.details:
                agg[detail.strip()] += 1
        print("--- AGGREGATE LINKER FLAG DIFFS (flag difference → # targets) ---")
        for diff_key, count in sorted(agg.items(), key=lambda x: -x[1]):
            print(f"  {diff_key}  ({count} targets)")
        print()

    # Aggregate library summary across all targets and modes
    linker_lib_items = by_category.get(Discrepancy.LINKER_LIBS, [])
    if linker_lib_items:
        agg = defaultdict(int)
        for d in linker_lib_items:
            for detail in d.details:
                agg[detail.strip()] += 1
        print("--- AGGREGATE LINKER LIBRARY DIFFS (library difference → # targets) ---")
        for diff_key, count in sorted(agg.items(), key=lambda x: -x[1]):
            print(f"  {diff_key}  ({count} targets)")
        print()

    # Per-mode summary
    print("--- PER-MODE SUMMARY ---")
    for mode in modes_to_check:
        flags_ok, targets_ok, ninja_ok, discs = results[mode]
        if flags_ok is None:
            print(f"  {mode}: SKIPPED (no build artifacts)")
            continue
        mode_ok = flags_ok and targets_ok and ninja_ok
        status = "PASS" if mode_ok else "FAIL"
        count = len(discs)
        print(f"  {mode}: {status} ({count} discrepancies)")

    return 1


def main():
    args = parse_args()
    source_dir = find_source_dir(args)
    ninja_path = find_ninja()

    if args.ci:
        # CI mode: all modes, configure both systems, structured output
        modes_to_check = list(MODE_TO_CMAKE_TYPE.keys())
        # CI implies not skipping configure (run both build systems)
        # unless --skip-configure is explicitly set

        results = {}
        for mode in modes_to_check:
            flags_ok, targets_ok, ninja_ok, discs = run_comparison(
                source_dir, mode, args, ninja_path)
            results[mode] = (flags_ok, targets_ok, ninja_ok, discs)

        exit_code = _print_ci_report(results, modes_to_check)
        sys.exit(exit_code)

    # Manual mode
    if args.all_modes:
        modes_to_check = list(MODE_TO_CMAKE_TYPE.keys())
    else:
        modes_to_check = [args.mode]

    print(f"Source directory: {source_dir}")
    print(f"Modes to check:  {', '.join(modes_to_check)}")
    print(f"Ninja:           {ninja_path}")

    if args.skip_configure:
        print("\nSkipping configure steps (--skip-configure)")

    results = {}
    for mode in modes_to_check:
        flags_ok, targets_ok, ninja_ok, _ = run_comparison(
            source_dir, mode, args, ninja_path)
        results[mode] = (flags_ok, targets_ok, ninja_ok)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    all_ok = True
    for mode in modes_to_check:
        flags_ok, targets_ok, ninja_ok = results[mode]
        if flags_ok is None:
            print(f"  {mode:10s}: SKIPPED (no build artifacts)")
            continue
        mode_ok = flags_ok and targets_ok and ninja_ok
        all_ok = all_ok and mode_ok
        cmake_type = MODE_TO_CMAKE_TYPE[mode]
        status = "✓ PASS" if mode_ok else "✗ FAIL"
        flags_status = "✓" if flags_ok else "✗"
        targets_status = "✓" if targets_ok else "✗"
        ninja_status = "✓" if ninja_ok else "✗"
        print(f"  {mode:10s} (CMake: {cmake_type:15s}):  {status}"
              f"  [flags: {flags_status}, targets: {targets_status},"
              f" linker: {ninja_status}]")

    print(f"\n  Overall: {'✓ ALL PASS' if all_ok else '✗ DIFFERENCES FOUND'}")
    print()

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()

