#!/bin/bash -x

set -euo pipefail

go_arch() {
    echo amd64
}

MINIO_BINARIES_DIR=/usr/local/bin

fake_gcs_install() {
    VERSION="1.52.3"
    BASE_URL="https://github.com/fsouza/fake-gcs-server/releases/download/v${VERSION}"
    CHECKSUMS_URL="${BASE_URL}/checksums.txt"

    FILENAME="fake-gcs-server_${VERSION}_Linux_$(go_arch).tar.gz"
    LOCAL_TAR="${MINIO_BINARIES_DIR}/${FILENAME}"
    LOCAL_BIN="${MINIO_BINARIES_DIR}/fake-gcs-server"

    # Fetch checksum for this file
    EXPECTED_SUM=$(curl -fsSL "$CHECKSUMS_URL" | grep "  ${FILENAME}$" | cut -d' ' -f1)

    # If file exists, verify it
    if [ -f "$LOCAL_TAR" ]; then
        echo "${EXPECTED_SUM}  ${LOCAL_TAR}" | sha256sum -c --status 2>/dev/null
        if [ $? -eq 0 ]; then
            echo "fake-gcs-server archive is up-to-date"
        else
            echo "Checksum mismatch, re-downloading fake-gcs-server"
            rm -f "$LOCAL_TAR"
        fi
    fi
    # Download if missing
    if [ ! -f "$LOCAL_TAR" ]; then
        curl -fSL -o "$LOCAL_TAR" "${BASE_URL}/${FILENAME}"
    fi
    # Verify again after download
    echo "${EXPECTED_SUM}  ${LOCAL_TAR}" | sha256sum -c --status
    if [ $? -ne 0 ]; then
        echo "Checksum verification failed after download"
        return 1
    fi
    # Extract binary
    tar -xzf "$LOCAL_TAR" -C "$MINIO_BINARIES_DIR"
    chmod +x "$LOCAL_BIN"

    echo "fake-gcs-server installed successfully"
}

fake_gcs_install
