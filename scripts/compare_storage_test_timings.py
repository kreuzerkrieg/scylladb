#!/usr/bin/env python3

from __future__ import annotations

"""Generate a local/S3/GCP test timing comparison table from testlog junit XML files.

The script scans junit XML files under a testlog directory and groups parametrized
tests by storage backend based on parameter names in square brackets:
`[local]`, `[s3]`, `[gs]`, `[gcs]`, or `[gcp]`.
It then prints a markdown table with average durations and regression percentages
versus local storage.
"""

import argparse
import logging
import statistics
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path


STORAGE_ALIASES = {
    "local": "local",
    "s3": "s3",
    "gs": "gcp",
    "gcs": "gcp",
    "gcp": "gcp",
}

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a timing comparison table for local/S3/GCP parametrized tests from testlog junit XML files."
    )
    parser.add_argument(
        "--testlog-dir",
        type=Path,
        default=Path("testlog"),
        help="Path to testlog directory (default: ./testlog).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output file path. By default prints to stdout.",
    )
    return parser.parse_args()


def discover_junit_files(testlog_dir: Path) -> list[Path]:
    return sorted(path for path in testlog_dir.rglob("*.xml") if path.is_file())


def normalize_storage_variant(test_name: str) -> tuple[str, str | None]:
    if "[" not in test_name or not test_name.endswith("]"):
        return test_name, None

    base_name, variant = test_name.rsplit("[", 1)
    variant = variant[:-1].strip().lower()
    normalized_variant = STORAGE_ALIASES.get(variant)
    if normalized_variant is None:
        return test_name, None
    return base_name, normalized_variant


def collect_timings(xml_files: list[Path]) -> dict[str, dict[str, list[float]]]:
    """Collect testcase durations from junit XML files for storage-parametrized tests.

    Returns:
        Nested mapping:
            full_test_name -> storage_backend -> list[duration_seconds]
    """
    timings: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    for xml_file in xml_files:
        try:
            root = ET.parse(xml_file).getroot()
        except ET.ParseError:
            LOGGER.warning("skipping malformed junit xml: %s", xml_file)
            continue

        testcases = root.findall(".//testcase")
        for testcase in testcases:
            name = testcase.get("name")
            classname = testcase.get("classname", "")
            time_str = testcase.get("time")
            if not name or time_str is None:
                continue

            try:
                duration = float(time_str)
            except ValueError:
                continue

            base_name, storage = normalize_storage_variant(name)
            if storage is None:
                continue

            full_test_name = f"{classname}::{base_name}" if classname else base_name
            timings[full_test_name][storage].append(duration)

    return timings


def fmt_duration(value: float | None) -> str:
    return f"{value:.3f}" if value is not None else "-"


def fmt_regression(local: float | None, other: float | None) -> str:
    if local is None or other is None:
        return "-"
    if local == 0:
        return "N/A"
    delta = ((other - local) / local) * 100
    return f"{delta:+.1f}%"


def build_markdown_table(timings: dict[str, dict[str, list[float]]]) -> str:
    """Build a markdown table with mean timings and local-vs-object-storage deltas."""
    header = (
        "| test | local(s) | s3(s) | gcp(s) | s3 vs local | gcp vs local | local n | s3 n | gcp n |\n"
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    lines = [header]

    for test_name in sorted(timings):
        variants = timings[test_name]
        local = statistics.mean(variants["local"]) if variants.get("local") else None
        s3 = statistics.mean(variants["s3"]) if variants.get("s3") else None
        gcp = statistics.mean(variants["gcp"]) if variants.get("gcp") else None

        # Keep rows that contain at least one object-storage counterpart.
        if s3 is None and gcp is None:
            continue

        lines.append(
            "| {test} | {local} | {s3} | {gcp} | {s3_reg} | {gcp_reg} | {local_n} | {s3_n} | {gcp_n} |".format(
                test=test_name,
                local=fmt_duration(local),
                s3=fmt_duration(s3),
                gcp=fmt_duration(gcp),
                s3_reg=fmt_regression(local, s3),
                gcp_reg=fmt_regression(local, gcp),
                local_n=len(variants.get("local", [])),
                s3_n=len(variants.get("s3", [])),
                gcp_n=len(variants.get("gcp", [])),
            )
        )

    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    testlog_dir = args.testlog_dir

    if not testlog_dir.exists():
        print(f"testlog directory does not exist: {testlog_dir}", file=sys.stderr)
        return 1

    xml_files = discover_junit_files(testlog_dir)
    if not xml_files:
        print(f"no xml files found under: {testlog_dir}", file=sys.stderr)
        return 1

    timings = collect_timings(xml_files)
    table = build_markdown_table(timings)

    if args.output:
        args.output.write_text(table + "\n", encoding="utf-8")
    else:
        print(table)

    return 0


if __name__ == "__main__":
    sys.exit(main())
