#!/usr/bin/env bash
set -euo pipefail

FORCE=0
VERSION=""
ARTIFACT=""

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

usage() {
  cat <<'EOF'
usage: ./install.sh [--version <version>] [--artifact <path>] [--force]

Installs Forge from a pinned release artifact when provided, otherwise from the local source tree.
- Preserves /etc/forge/forge.conf and /etc/forge/forge.env unless --force is used.
- Installs the binary atomically.
- Keeps the previous binary as /usr/local/bin/forge.previous.
- Never deletes /var/lib/forge.
- Does not overwrite systemd drop-ins.
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

while [ $# -gt 0 ]; do
  case "$1" in
    --force)
      FORCE=1
      ;;
    --version)
      shift
      [ $# -gt 0 ] || die "--version requires a value"
      VERSION="$1"
      ;;
    --artifact)
      shift
      [ $# -gt 0 ] || die "--artifact requires a value"
      ARTIFACT="$1"
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

stage_artifact() {
  local artifact="$1" stage_dir="$2"
  reject_world_writable "$artifact"
  verify_checksum_if_available "$artifact"
  mkdir -p "$stage_dir"
  tar -xzf "$artifact" -C "$stage_dir"
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
  if [ -n "$VERSION" ] && [ -z "$ARTIFACT" ]; then
    ARTIFACT="$(resolve_artifact_from_version "$VERSION")"
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
}

main "$@"
