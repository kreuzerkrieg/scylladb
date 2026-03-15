#!/usr/bin/env python3
"""Extract per-tablet restore transition timeline from ScyllaDB logs.

Scans coordinator and node logs for restore-related events and builds
a table showing the lifecycle of each tablet through its restore stages.

Supports two log layouts:
  - Test logs:   LOGDIR/*.log
  - Server logs: LOGDIR/*/messages.log
"""

import re
import os
import sys
from collections import defaultdict

LOGDIR = sys.argv[1] if len(sys.argv) > 1 else "testlog/release"

# Timestamp extraction: grab the first timestamp-like token from each line.
#   Test logs:   "INFO  2026-03-12 17:43:54,967 ..."  -> "2026-03-12 17:43:54,967"
#   Server logs: "2026-03-12T12:22:53.332 host ..."   -> "2026-03-12T12:22:53.332"
_TS_RE = re.compile(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.,]\d{3})')

# Message patterns – searched anywhere in the line, no level filtering.
_P_SUBMIT = re.compile(r'raft_topology - Restoring tablet (\S+)\s*$')
_P_COORD = re.compile(r'raft_topology - Restoring tablet=(\S+) from (\S+) on \[(.+?)\]')
_P_CLEARED = re.compile(r'raft_topology - Clearing restore transition for (\S+)')
_P_RESOLVED = re.compile(r'restore for tablet (\S+) resolved')
_P_STILL = re.compile(r'Tablet (\S+) still doing restore')
_P_BARRIER = re.compile(r'barrier for tablet (\S+) resolved')
_P_DOWNLOAD = re.compile(r'Downloading sstables for tablet (\S+)')
_P_RESTORED_ON = re.compile(r'Tablet (\S+) restored on (\S+)')
_P_FAILED = re.compile(r'Restore failed for (\S+)')
_P_BUSY = re.compile(r'transit_tablet\((\S+)\): topology busy')
# New: per-replica RPC tracking
_P_RPC_SEND = re.compile(r'Sending restore RPC for tablet (\S+) to replica (\S+):(\d+)')
_P_RPC_FAIL = re.compile(r'Restore RPC for tablet (\S+) to replica (\S+):(\d+) failed after (\d+)s: (.*)')
_P_STUCK_WARN = re.compile(r'Tablet (\S+) has been doing (\S+) for (\d+)s, may be stuck')
# New: download progress on receiving node
_P_DL_PROGRESS = re.compile(r'Tablet (\S+) downloading (\d+) SSTables from prefix (\d+)/(\d+)')
_P_DL_COMPLETE = re.compile(r'Tablet (\S+) prefix (\d+)/(\d+) download complete in (\d+)s')

# Collect events per tablet
tablets = defaultdict(lambda: {
    "submit": None,
    "barrier_resolved": None,
    "coord_start": None,
    "replicas": None,
    "snapshot": None,
    "downloads": [],
    "restored_on": [],
    "resolved": None,
    "cleared": None,
    "failed": None,
    "still_doing_count": 0,
    "transit_busy": None,
    # Per-replica RPC tracking
    "rpc_sent": [],        # (time, host, shard)
    "rpc_done": [],        # (time, host, elapsed_s)
    "rpc_failed": [],      # (time, host, shard, elapsed_s, error)
    "stuck_warn": None,    # (time, elapsed_s)
})

# Discover log files
log_entries = []
for entry in sorted(os.listdir(LOGDIR)):
    full = os.path.join(LOGDIR, entry)
    if os.path.isfile(full) and entry.endswith('.log'):
        log_entries.append((full, entry.replace('.log', '')))
    elif os.path.isdir(full):
        msg_log = os.path.join(full, 'messages.log')
        if os.path.isfile(msg_log):
            log_entries.append((msg_log, entry))

print(f"Found {len(log_entries)} log files")


def extract_ts(line: str) -> str:
    """Extract timestamp string from a log line."""
    m = _TS_RE.search(line)
    return m.group(1) if m else ""


for path, node_name in log_entries:
    with open(path) as fh:
        for line in fh:
            # Quick pre-filter: skip lines without relevant keywords
            if ('tablet' not in line and 'Tablet' not in line
                    and 'restore' not in line and 'Restore' not in line):
                continue

            ts = extract_ts(line)

            m = _P_SUBMIT.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["submit"] is None:
                    t["submit"] = ts
                continue

            m = _P_COORD.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["coord_start"] is None:
                    t["coord_start"] = ts
                    t["snapshot"] = m.group(2)
                    t["replicas"] = m.group(3)
                continue

            m = _P_BARRIER.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["barrier_resolved"] is None:
                    t["barrier_resolved"] = ts
                continue

            m = _P_RESOLVED.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["resolved"] is None:
                    t["resolved"] = ts
                continue

            m = _P_CLEARED.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["cleared"] is None:
                    t["cleared"] = ts
                continue

            m = _P_DOWNLOAD.search(line)
            if m:
                tablets[m.group(1)]["downloads"].append((ts, node_name))
                continue

            m = _P_RESTORED_ON.search(line)
            if m:
                tablets[m.group(1)]["restored_on"].append((ts, m.group(2)))
                continue

            m = _P_STILL.search(line)
            if m:
                tablets[m.group(1)]["still_doing_count"] += 1
                continue

            m = _P_FAILED.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["failed"] is None:
                    t["failed"] = ts
                continue

            m = _P_BUSY.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["transit_busy"] is None:
                    t["transit_busy"] = ts
                continue

            m = _P_RPC_SEND.search(line)
            if m:
                tablets[m.group(1)]["rpc_sent"].append((ts, m.group(2), m.group(3)))
                continue

            m = _P_RPC_FAIL.search(line)
            if m:
                tablets[m.group(1)]["rpc_failed"].append((ts, m.group(2), m.group(3), m.group(4), m.group(5)))
                continue

            m = _P_STUCK_WARN.search(line)
            if m:
                t = tablets[m.group(1)]
                if t["stuck_warn"] is None:
                    t["stuck_warn"] = (ts, m.group(3))
                continue


# Find the restore table(s)
restore_table_ids = set()
for tid, t in tablets.items():
    if t["submit"] is not None or t["coord_start"] is not None:
        restore_table_ids.add(tid.rsplit(':', 1)[0])

restore_tablets = {tid: t for tid, t in tablets.items()
                   if tid.rsplit(':', 1)[0] in restore_table_ids}


def tablet_sort_key(tablet_id: str):
    """Sort by table UUID then tablet number."""
    parts = tablet_id.rsplit(':', 1)
    return (parts[0], int(parts[1]))


sorted_tablets = sorted(restore_tablets.keys(), key=tablet_sort_key)

if not sorted_tablets:
    print("No tablet restore transitions found in logs.")
    sys.exit(0)

table_uuid = sorted_tablets[0].rsplit(':', 1)[0]
print(f"Table: {table_uuid}")
print(f"Total tablets: {len(sorted_tablets)}")
print()


def ts_short(ts):
    """Shorten timestamp to just HH:MM:SS.mmm."""
    if not ts:
        return "-"
    if 'T' in ts:
        return ts.split('T', 1)[1]
    parts = ts.split(' ')
    return parts[1] if len(parts) > 1 else ts


hdr = (f"{'Tablet':>6}   {'Submit':>15}   {'Barrier':>15}   "
       f"{'Coord Start':>15}   {'Resolved':>15}   {'Cleared':>15}   "
       f"{'#Polls':>6}   {'#DL':>4}   Replicas")
print(hdr)
print("-" * len(hdr))

for tid in sorted_tablets:
    t = tablets[tid]
    tab_num = tid.rsplit(':', 1)[1]

    replicas_short = t["replicas"] or "-"
    replicas_short = re.sub(r'([0-9a-f]{8})-[0-9a-f-]+', r'\1', replicas_short)

    print(f"{tab_num:>6}   {ts_short(t['submit']):>15}   "
          f"{ts_short(t['barrier_resolved']):>15}   "
          f"{ts_short(t['coord_start']):>15}   "
          f"{ts_short(t['resolved']):>15}   "
          f"{ts_short(t['cleared']):>15}   "
          f"{t['still_doing_count']:>6}   "
          f"{len(t['downloads']):>4}   {replicas_short}")

# Stuck / failed
stuck = [tid for tid in sorted_tablets
         if not tablets[tid]["cleared"] and not tablets[tid]["failed"]]
failed = [tid for tid in sorted_tablets if tablets[tid]["failed"]]

if stuck:
    print(f"\n{'='*60}")
    print(f"STUCK TABLETS ({len(stuck)}):")
    for tid in stuck:
        t = tablets[tid]
        print(f"  {tid}: submitted={t['submit']}, "
              f"coord_start={t['coord_start']}, "
              f"still_doing_polls={t['still_doing_count']}")
        if t["stuck_warn"]:
            print(f"    WARNING: stuck for {t['stuck_warn'][1]}s at {t['stuck_warn'][0]}")
        if t["rpc_sent"]:
            print(f"    RPCs sent: {len(t['rpc_sent'])}")
            for rts, host, shard in t["rpc_sent"]:
                host_short = host[:8] if len(host) > 8 else host
                print(f"      {ts_short(rts)} -> {host_short}:{shard}")
        if t["restored_on"]:
            print(f"    Restored on: {len(t['restored_on'])} replicas")
            for rts, host in t["restored_on"]:
                host_short = host[:8] if len(host) > 8 else host
                print(f"      {ts_short(rts)} <- {host_short}")
        else:
            print(f"    Restored on: NONE (no replica reported completion)")
        if t["rpc_failed"]:
            print(f"    RPC FAILURES:")
            for rts, host, shard, elapsed, err in t["rpc_failed"]:
                host_short = host[:8] if len(host) > 8 else host
                print(f"      {ts_short(rts)} {host_short}:{shard} after {elapsed}s: {err[:100]}")

if failed:
    print(f"\n{'='*60}")
    print(f"FAILED TABLETS ({len(failed)}):")
    for tid in failed:
        print(f"  {tid}: failed at {tablets[tid]['failed']}")

if not stuck and not failed:
    print(f"\n{'='*60}")
    print("ALL TABLETS COMPLETED SUCCESSFULLY")

# Download distribution
print(f"\n{'='*60}")
print("DOWNLOAD DISTRIBUTION PER NODE:")
dl_per_node = defaultdict(int)
for tid in sorted_tablets:
    for _, node in tablets[tid]["downloads"]:
        dl_per_node[node] += 1
for node in sorted(dl_per_node):
    print(f"  {node}: {dl_per_node[node]} downloads")
