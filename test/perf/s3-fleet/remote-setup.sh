#!/usr/bin/env bash
# Runs ON the instance. Kept as a real file rather than an inline heredoc:
# every fleet failure so far has come from shell escaping in nested quoting.
#
#   remote-setup.sh <presigned-binary-url>
#
# Prints "SETUP_OK" as its last line only on success. Anything else means the
# instance is not usable and the caller must not count it as ready.
set -uo pipefail

URL="${1:?usage: remote-setup.sh <url>}"
fail() { echo "SETUP_FAIL: $*" >&2; exit 1; }

# --- storage -------------------------------------------------------------------
# Follows dist/common/scripts/scylla_raid_setup, so the data path matches what a
# real node runs on: RAID-0 with 1024 KB chunks and XFS, not ext4 on mdadm's
# default 512 KB chunk.
#
# This is not cosmetic for an S3 measurement. In s3::client::chunked_download_source
# the disk write is the consumer that decides whether a large object is fetched as
# one ranged GET or as many 5 MiB chunks, so filesystem throughput moves the
# request rate being measured -- a ~19x swing when the corpus is written.
#
# Identify instance-store volumes by model string. Deriving them by excluding the
# root disk does not work: Fedora roots on btrfs, so `findmnt -no SOURCE /` yields
# "/dev/nvme0n1p3[/root]" -- the subvolume suffix makes it an invalid device path,
# lsblk -no PKNAME returns nothing, and a `grep -v "$(...)"` built from it then
# filters out every disk.
if ! mountpoint -q /mnt/data; then
    mapfile -t DRIVES < <(lsblk -dno NAME,MODEL | grep 'Instance Storage' | awk '{print "/dev/"$1}')
    n=${#DRIVES[@]}
    echo "instance-store drives: $n ${DRIVES[*]:-none}"

    command -v mkfs.xfs >/dev/null || sudo dnf install -y -q xfsprogs || fail "install xfsprogs"

    if (( n == 0 )); then
        # EBS-only instance: a download-only run writes nothing but the binary.
        sudo mkdir -p /mnt/data || fail "mkdir /mnt/data"
    else
        for d in "${DRIVES[@]}"; do
            sudo wipefs -a "$d" || fail "wipefs $d"
        done
        if (( n == 1 )); then
            FSDEV="${DRIVES[0]}"
        else
            command -v mdadm >/dev/null || sudo dnf install -y -q mdadm || fail "install mdadm"
            sudo udevadm settle
            # -c1024: same chunk size scylla_raid_setup uses
            sudo mdadm --create --verbose --force --run /dev/md0 --level=0 -c1024 \
                --raid-devices="$n" "${DRIVES[@]}" || fail "mdadm create"
            FSDEV=/dev/md0
            sudo wipefs -a "$FSDEV" || fail "wipefs $FSDEV"
            sudo udevadm settle
        fi
        # -K skips discard; rmapbt/reflink off, as scylla_raid_setup does. Block size
        # is left at the default, which is what scylla does on kernel >= 5.12.
        sudo mkfs.xfs "$FSDEV" -K -m rmapbt=0 -m reflink=0 || fail "mkfs.xfs $FSDEV"
        sudo udevadm settle
        sudo mkdir -p /mnt/data || fail "mkdir"
        sudo mount -o noatime "$FSDEV" /mnt/data || fail "mount $FSDEV"
    fi
    sudo chown "$USER:$USER" /mnt/data || fail "chown"
fi
mountpoint -q /mnt/data || [[ -d /mnt/data ]] || fail "/mnt/data missing after setup"

# --- runtime libraries -------------------------------------------------------
# Fedora Cloud Base is minimal: it has base ICU but not the rest of what the
# binary links against.
# screen is required to start the workload: ssh does not return from a
# backgrounded remote command while it holds the session's stdin.
sudo dnf install -y -q screen libdeflate snappy cryptopp yaml-cpp boost-regex \
    boost-program-options boost-container boost-test libatomic lksctp-tools \
    protobuf hwloc-libs liburing jsoncpp lttng-ust || fail "dnf install"

# The AMI is pinned but the build host keeps updating, so a Fedora release match is
# not enough: on 2026-09-09 the AMI carried libstdc++-16.1.1 while the binary was
# linked against 16.2.1 and needed GLIBCXX_3.4.36, which fails the library check
# below with a message that names neither the AMI nor the toolchain.
sudo dnf upgrade -y -q libstdc++ libgcc || fail "upgrade libstdc++"

# --- binary ------------------------------------------------------------------
curl -fsSL "$URL" -o /mnt/data/perf_s3_downloader || fail "download binary"
chmod +x /mnt/data/perf_s3_downloader             || fail "chmod binary"

missing=$(ldd /mnt/data/perf_s3_downloader 2>&1 | grep 'not found')
[[ -n "$missing" ]] && fail "unresolved libraries: $missing"

/mnt/data/perf_s3_downloader --help >/dev/null 2>&1 || fail "binary does not run"

echo "df: $(df -h /mnt/data | tail -1)"
echo "fs: $(findmnt -no FSTYPE,OPTIONS /mnt/data 2>/dev/null) chunk=$(cat /sys/block/md0/md/chunk_size 2>/dev/null || echo n/a)"
echo "SETUP_OK"
