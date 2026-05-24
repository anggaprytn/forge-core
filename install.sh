#!/usr/bin/env bash
set -euo pipefail

FORCE=0
RELEASE_TAG=""
ARTIFACT=""
ALLOW_UNSIGNED_RELEASE=0
PUBLIC_KEY_PATH=""

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BIN_DEST="${FORGE_BIN_DEST:-/usr/local/bin/forge}"
PREVIOUS_BIN_DEST="${FORGE_PREVIOUS_BIN_DEST:-/usr/local/bin/forge.previous}"
CONFIG_DIR="${FORGE_CONFIG_DIR:-/etc/forge}"
CONFIG_PATH="${FORGE_CONFIG_PATH:-$CONFIG_DIR/forge.conf}"
ENV_PATH="${FORGE_ENV_PATH:-$CONFIG_DIR/forge.env}"
UNIT_PATH="${FORGE_UNIT_PATH:-/etc/systemd/system/forge.service}"
SERVICE_SRC="${FORGE_SERVICE_SRC:-$REPO_ROOT/deploy/forge.service}"
CONFIG_TEMPLATE="${FORGE_CONFIG_TEMPLATE:-$REPO_ROOT/deploy/forge.conf.example}"
ENV_TEMPLATE="${FORGE_ENV_TEMPLATE:-$REPO_ROOT/examples/forge.env.example}"
STORAGE_ROOT="${FORGE_STORAGE_ROOT:-/var/lib/forge}"
SRV_ROOT="${FORGE_SRV_ROOT:-/srv/forge}"
SAMPLE_ROOT="${FORGE_SAMPLE_ROOT:-/srv/forge/sample-http-app}"
ALLOW_UNPRIVILEGED_INSTALL="${FORGE_ALLOW_UNPRIVILEGED_INSTALL:-0}"
INSTALL_TIMEOUT_SECS="${FORGE_INSTALL_TIMEOUT_SECS:-300}"
RELEASE_REPOSITORY="${FORGE_RELEASE_REPOSITORY:-anggaprytn/forge-core}"
RELEASE_API_BASE_URL="${FORGE_RELEASE_API_BASE_URL:-https://api.github.com}"
RELEASE_PUBLIC_KEY_PATH="${FORGE_RELEASE_PUBLIC_KEY:-$CONFIG_DIR/release-public-key.pem}"
ARTIFACT_STAGE_DIR=""

usage() {
  cat <<'EOF'
usage: ./install.sh [--version <tag>] [--release <tag>] [--artifact <path>] [--public-key <path>] [--force] [--allow-unsigned-release]

Installs Forge from a pinned GitHub release or a local artifact, otherwise from the local source tree.
- Preserves /etc/forge/forge.conf and /etc/forge/forge.env unless --force is used.
- Installs the binary atomically.
- Keeps the previous binary as /usr/local/bin/forge.previous.
- Never deletes /var/lib/forge.
- Does not overwrite systemd drop-ins.
- Release installs require signature verification when a public key is configured.
EOF
}

log() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

die() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

run_with_timeout() {
  local timeout_secs="$1"
  shift
  set +e
  perl -e 'alarm shift @ARGV; exec @ARGV' "$timeout_secs" "$@"
  local status=$?
  set -e
  if [ "$status" -eq 142 ]; then
    die "$1 timed out after ${timeout_secs}s"
  fi
  return "$status"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --force)
      FORCE=1
      ;;
    --version)
      shift
      [ $# -gt 0 ] || die "--version requires a value"
      RELEASE_TAG="$1"
      ;;
    --release)
      shift
      [ $# -gt 0 ] || die "--release requires a value"
      RELEASE_TAG="$1"
      ;;
    --artifact)
      shift
      [ $# -gt 0 ] || die "--artifact requires a value"
      ARTIFACT="$1"
      ;;
    --public-key)
      shift
      [ $# -gt 0 ] || die "--public-key requires a value"
      PUBLIC_KEY_PATH="$1"
      ;;
    --allow-unsigned-release)
      ALLOW_UNSIGNED_RELEASE=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
  shift
done

if [ -n "$PUBLIC_KEY_PATH" ]; then
  RELEASE_PUBLIC_KEY_PATH="$PUBLIC_KEY_PATH"
fi

if [ "$(id -u)" -eq 0 ] || [ "$ALLOW_UNPRIVILEGED_INSTALL" = "1" ]; then
  USE_SUDO=0
else
  command -v sudo >/dev/null 2>&1 || die "run as root or install sudo"
  USE_SUDO=1
fi

as_root() {
  if [ "$USE_SUDO" -eq 1 ]; then
    sudo "$@"
  else
    "$@"
  fi
}

random_hex() {
  local bytes="${1:?missing byte count}"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex "$bytes"
  else
    od -An -N"$bytes" -tx1 /dev/urandom | tr -d ' \n'
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

reject_world_writable() {
  local path="$1"
  local mode
  if mode="$(stat -c '%a' "$path" 2>/dev/null)"; then
    :
  else
    mode="$(stat -f '%Lp' "$path")"
  fi
  [ $((10#$mode & 2)) -eq 0 ] || die "refusing world-writable artifact: $path"
}

verify_checksum_if_available() {
  local artifact="$1" checksum_path expected actual file_name
  checksum_path="$(dirname "$artifact")/checksums.txt"
  [ -f "$checksum_path" ] || return 0
  file_name="$(basename "$artifact")"
  actual="$(sha256_file "$artifact")"
  expected="$(awk -v name="$file_name" '$2 == name || $2 == "*"name { print $1 }' "$checksum_path" | tail -n 1)"
  [ -n "$expected" ] || return 0
  [ "$expected" = "$actual" ] || die "artifact checksum mismatch for $artifact"
}

install_if_missing_or_forced() {
  local src="$1" dest="$2" mode="$3"
  if [ -e "$dest" ] && [ "$FORCE" -ne 1 ]; then
    log "preserving existing $dest"
    return 0
  fi
  as_root install -D -m "$mode" "$src" "$dest"
  log "installed $dest"
}

write_if_missing_or_forced() {
  local dest="$1" mode="$2" tmp
  tmp="$(mktemp)"
  cat >"$tmp"
  if [ -e "$dest" ] && [ "$FORCE" -ne 1 ]; then
    rm -f "$tmp"
    log "preserving existing $dest"
    return 0
  fi
  as_root install -D -m "$mode" "$tmp" "$dest"
  rm -f "$tmp"
  log "installed $dest"
}

resolve_platform_artifact_name() {
  if [ -n "${FORGE_RELEASE_PLATFORM:-}" ]; then
    printf '%s\n' "$FORGE_RELEASE_PLATFORM"
    return 0
  fi
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os:$arch" in
    Linux:x86_64) printf '%s\n' "linux-amd64" ;;
    Darwin:arm64) printf '%s\n' "darwin-arm64" ;;
    *) die "unsupported installer host $os/$arch for --version" ;;
  esac
}

resolve_artifact_from_version() {
  local version="$1" platform
  platform="$(resolve_platform_artifact_name)"
  local candidate="$REPO_ROOT/dist/forge-$version-$platform.tar.gz"
  [ -f "$candidate" ] || die "artifact not found for version $version at $candidate"
  printf '%s\n' "$candidate"
}

curl_download() {
  local url="$1" destination="$2"
  command -v curl >/dev/null 2>&1 || die "curl is required for release downloads"
  curl -fsSL \
    -H 'Accept: application/vnd.github+json' \
    -H 'User-Agent: forge-install' \
    "$url" \
    -o "$destination"
}

verify_checksum_entry() {
  local path="$1" checksum_path="$2" expected actual file_name
  file_name="$(basename "$path")"
  actual="$(sha256_file "$path")"
  expected="$(awk -v name="$file_name" '$2 == name || $2 == "*"name { print $1 }' "$checksum_path" | tail -n 1)"
  [ -n "$expected" ] || return 0
  [ "$expected" = "$actual" ] || die "checksum mismatch for $file_name"
}

manifest_artifact_checksum() {
  local manifest_path="$1" artifact_name="$2"
  python3 - "$manifest_path" "$artifact_name" <<'PY'
import json
import sys

manifest_path, artifact_name = sys.argv[1:3]
manifest = json.load(open(manifest_path, "r", encoding="utf-8"))
for artifact in manifest.get("artifacts", []):
    if artifact.get("name") == artifact_name:
        print(artifact.get("sha256", ""))
        raise SystemExit(0)
raise SystemExit(1)
PY
}

verify_manifest_signature() {
  local manifest_path="$1" signature_path="$2" public_key_path="$3" decoded_signature
  command -v openssl >/dev/null 2>&1 || die "openssl is required to verify signed releases"
  [ -f "$public_key_path" ] || die "release public key not found at $public_key_path"
  decoded_signature="$(mktemp)"
  python3 - "$signature_path" "$decoded_signature" <<'PY'
import base64
import pathlib
import sys

signature_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
raw = "".join(signature_path.read_text(encoding="utf-8").split())
output_path.write_bytes(base64.b64decode(raw))
PY
  openssl pkeyutl -verify -pubin -inkey "$public_key_path" \
    -sigfile "$decoded_signature" \
    -rawin \
    -in "$manifest_path" >/dev/null 2>&1 || {
      rm -f "$decoded_signature"
      die "release manifest signature verification failed"
    }
  rm -f "$decoded_signature"
}

verify_release_bundle() {
  local artifact="$1" manifest_path="$2" signature_path="$3" checksums_path="$4"
  local artifact_name expected actual
  artifact_name="$(basename "$artifact")"
  [ -f "$manifest_path" ] || die "release manifest download missing"

  if [ -f "$checksums_path" ]; then
    verify_checksum_entry "$artifact" "$checksums_path"
    verify_checksum_entry "$manifest_path" "$checksums_path"
    [ -f "$signature_path" ] && verify_checksum_entry "$signature_path" "$checksums_path"
  fi

  if [ -f "$RELEASE_PUBLIC_KEY_PATH" ]; then
    [ -f "$signature_path" ] || die "release signature download missing"
    verify_manifest_signature "$manifest_path" "$signature_path" "$RELEASE_PUBLIC_KEY_PATH"
  elif [ "$ALLOW_UNSIGNED_RELEASE" -ne 1 ]; then
    die "release public key not configured; set FORGE_RELEASE_PUBLIC_KEY or place release-public-key.pem under $CONFIG_DIR, or rerun with --allow-unsigned-release"
  fi

  expected="$(manifest_artifact_checksum "$manifest_path" "$artifact_name")" || die "artifact $artifact_name not listed in release manifest"
  actual="$(sha256_file "$artifact")"
  [ "$expected" = "$actual" ] || die "artifact checksum mismatch against signed release manifest"
}

download_release_assets() {
  local tag="$1" platform api_url release_json artifact_url manifest_url signature_url checksums_url
  local artifact_name artifact_path manifest_path signature_path checksums_path release_fields_file
  platform="$(resolve_platform_artifact_name)"
  api_url="$RELEASE_API_BASE_URL/repos/$RELEASE_REPOSITORY/releases/tags/$tag"
  ARTIFACT_STAGE_DIR="$(mktemp -d)"
  release_json="$ARTIFACT_STAGE_DIR/release.json"
  curl_download "$api_url" "$release_json" || die "failed to fetch GitHub release metadata for tag $tag from $RELEASE_REPOSITORY"

  release_fields_file="$ARTIFACT_STAGE_DIR/release-fields.txt"
  python3 - "$release_json" "$platform" >"$release_fields_file" <<'PY'
import json
import sys

release = json.load(open(sys.argv[1], "r", encoding="utf-8"))
platform = sys.argv[2]
assets = {asset["name"]: asset["browser_download_url"] for asset in release.get("assets", [])}
artifact_name = next(
    (name for name in assets if name.startswith("forge-") and name.endswith(f"-{platform}.tar.gz")),
    None,
)
if not artifact_name:
    raise SystemExit(1)
for key in [
    artifact_name,
    "release-manifest.json",
    "release-manifest.sig",
    "checksums.txt",
]:
    print(assets.get(key, ""))
print(artifact_name)
PY
  artifact_url="$(sed -n '1p' "$release_fields_file")"
  manifest_url="$(sed -n '2p' "$release_fields_file")"
  signature_url="$(sed -n '3p' "$release_fields_file")"
  checksums_url="$(sed -n '4p' "$release_fields_file")"
  artifact_name="$(sed -n '5p' "$release_fields_file")"

  [ -n "$artifact_url" ] || die "release $tag is missing a forge artifact for platform $platform"
  [ -n "$manifest_url" ] || die "release $tag is missing release-manifest.json"
  [ -n "$checksums_url" ] || die "release $tag is missing checksums.txt"

  artifact_path="$ARTIFACT_STAGE_DIR/$artifact_name"
  manifest_path="$ARTIFACT_STAGE_DIR/release-manifest.json"
  signature_path="$ARTIFACT_STAGE_DIR/release-manifest.sig"
  checksums_path="$ARTIFACT_STAGE_DIR/checksums.txt"
  curl_download "$artifact_url" "$artifact_path" || die "failed to download $artifact_name"
  curl_download "$manifest_url" "$manifest_path" || die "failed to download release-manifest.json"
  [ -n "$signature_url" ] && curl_download "$signature_url" "$signature_path" || true
  curl_download "$checksums_url" "$checksums_path" || die "failed to download checksums.txt"
  verify_release_bundle "$artifact_path" "$manifest_path" "$signature_path" "$checksums_path"
  printf '%s\n' "$artifact_path"
}

stage_artifact() {
  local artifact="$1" stage_dir="$2"
  reject_world_writable "$artifact"
  verify_checksum_if_available "$artifact"
  mkdir -p "$stage_dir"
  run_with_timeout "$INSTALL_TIMEOUT_SECS" tar -xzf "$artifact" -C "$stage_dir"
  [ -x "$stage_dir/forge" ] || die "artifact missing forge binary"
  [ -f "$stage_dir/forge.conf.example" ] || die "artifact missing forge.conf.example"
  [ -f "$stage_dir/forge.env.example" ] || die "artifact missing forge.env.example"
}

install_binary_atomically() {
  local src="$1" dest="$2" previous="$3"
  local tmp
  tmp="$(mktemp "${dest}.tmp.XXXXXX")"
  as_root install -m 0755 "$src" "$tmp"
  if [ -f "$dest" ]; then
    as_root cp "$dest" "$previous"
    log "updated rollback candidate $previous"
  fi
  as_root mv "$tmp" "$dest"
  log "installed $dest atomically"
}

ensure_layout() {
  as_root install -d -m 0755 \
    "$(dirname "$BIN_DEST")" \
    "$CONFIG_DIR" \
    "$STORAGE_ROOT" \
    "$SRV_ROOT" \
    "$SAMPLE_ROOT"
  as_root install -d -m 0755 \
    "$STORAGE_ROOT/projects" \
    "$STORAGE_ROOT/events" \
    "$STORAGE_ROOT/secrets" \
    "$STORAGE_ROOT/indexes" \
    "$STORAGE_ROOT/idempotency" \
    "$STORAGE_ROOT/queue"
}

install_default_config() {
  local config_source="$1"
  if [ -e "$CONFIG_PATH" ] && [ "$FORCE" -ne 1 ]; then
    log "preserving existing $CONFIG_PATH"
  else
    local bearer_token config_tmp
    bearer_token="$(random_hex 32)"
    config_tmp="$(mktemp)"
    sed "s/replace-with-a-long-random-token/$bearer_token/" "$config_source" >"$config_tmp"
    as_root install -m 0644 "$config_tmp" "$CONFIG_PATH"
    rm -f "$config_tmp"
    log "installed $CONFIG_PATH"
  fi
}

install_default_env() {
  local env_source="$1"
  if [ -e "$ENV_PATH" ] && [ "$FORCE" -ne 1 ]; then
    log "preserving existing $ENV_PATH"
  else
    local tmp master_key
    master_key="$(random_hex 32)"
    tmp="$(mktemp)"
    sed "s/replace-with-64-hex-characters/$master_key/" "$env_source" >"$tmp"
    as_root install -m 0644 "$tmp" "$ENV_PATH"
    rm -f "$tmp"
    log "installed $ENV_PATH"
  fi
}

install_systemd_unit() {
  if [ ! -f "$SERVICE_SRC" ]; then
    warn "deploy/forge.service not found; skipping systemd unit install"
    return 0
  fi
  install_if_missing_or_forced "$SERVICE_SRC" "$UNIT_PATH" 0644
  if [ -d "$(dirname "$UNIT_PATH")/forge.service.d" ]; then
    warn "systemd override files detected under $(dirname "$UNIT_PATH")/forge.service.d; leaving them unchanged"
  fi
  if command -v systemctl >/dev/null 2>&1; then
    as_root systemctl daemon-reload || true
  fi
}

resolve_binary_source() {
  if [ -n "$RELEASE_TAG" ] && [ -n "$ARTIFACT" ]; then
    die "cannot combine --artifact with --version/--release"
  fi

  if [ -n "$RELEASE_TAG" ] && [ -z "$ARTIFACT" ]; then
    ARTIFACT="$(download_release_assets "$RELEASE_TAG")"
  fi

  if [ -n "$ARTIFACT" ]; then
    local stage_dir
    stage_dir="$(mktemp -d)"
    stage_artifact "$ARTIFACT" "$stage_dir"
    ARTIFACT_STAGE_DIR="$stage_dir"
    BIN_SRC="$stage_dir/forge"
    CONFIG_SRC="$stage_dir/forge.conf.example"
    ENV_SRC="$stage_dir/forge.env.example"
    return 0
  fi

  [ -f "$CONFIG_TEMPLATE" ] || die "missing config template: $CONFIG_TEMPLATE"
  [ -f "$ENV_TEMPLATE" ] || die "missing env template: $ENV_TEMPLATE"
  cd "$REPO_ROOT"
  if command -v cargo >/dev/null 2>&1; then
    log "building forge with cargo"
    cargo build --release --bin forge
    BIN_SRC="$REPO_ROOT/target/release/forge"
  elif [ -x "$REPO_ROOT/target/release/forge" ]; then
    BIN_SRC="$REPO_ROOT/target/release/forge"
    log "cargo not found; using existing $BIN_SRC"
  else
    die "cargo not found and no prebuilt binary exists at target/release/forge"
  fi
  CONFIG_SRC="$CONFIG_TEMPLATE"
  ENV_SRC="$ENV_TEMPLATE"
}

main() {
  resolve_binary_source
  [ -x "$BIN_SRC" ] || die "forge binary not found at $BIN_SRC"

  ensure_layout
  install_binary_atomically "$BIN_SRC" "$BIN_DEST" "$PREVIOUS_BIN_DEST"
  install_default_config "$CONFIG_SRC"
  install_default_env "$ENV_SRC"
  install_systemd_unit

  cat <<'EOF'

Release install notes:
  - syncforge remains development-only.
  - Release artifacts plus 'forge upgrade plan/apply' are the preferred operator path.
  - /var/lib/forge is preserved across installs and upgrades.
  - Existing forge.conf and forge.env stay in place unless --force is used.
EOF

  if [ -n "$ARTIFACT_STAGE_DIR" ] && [ -d "$ARTIFACT_STAGE_DIR" ]; then
    rm -rf "$ARTIFACT_STAGE_DIR"
  fi
}

main "$@"
