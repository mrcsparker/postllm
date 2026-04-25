#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <pg-major> [out-dir]" >&2
  exit 1
fi

PG_MAJOR="$1"
OUT_DIR="${2:-${ROOT_DIR}/target_ci/release-assets}"
PG_FEATURE="pg${PG_MAJOR}"
PACKAGE_DIR="${OUT_DIR}/package-${PG_FEATURE}"
DIST_DIR="${OUT_DIR}/dist"
VERSION="$(awk -F'"' '/^version = / { print $2; exit }' "${ROOT_DIR}/Cargo.toml")"
PG_CONFIG="$(cargo pgrx info pg-config "${PG_FEATURE}")"
ARCHIVE_NAME="postllm-v${VERSION}-${PG_FEATURE}-linux-x86_64"
ARCHIVE_PATH="${DIST_DIR}/${ARCHIVE_NAME}.tar.gz"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"

rm -rf "${PACKAGE_DIR}"
mkdir -p "${PACKAGE_DIR}" "${DIST_DIR}"

cargo pgrx package \
  --release \
  --pg-config "${PG_CONFIG}" \
  --out-dir "${PACKAGE_DIR}" \
  --no-default-features \
  --features "${PG_FEATURE}"

tar -C "${PACKAGE_DIR}" -czf "${ARCHIVE_PATH}" .
shasum -a 256 "${ARCHIVE_PATH}" | awk '{print $1}' > "${CHECKSUM_PATH}"

echo "archive=${ARCHIVE_PATH}"
echo "checksum=${CHECKSUM_PATH}"
