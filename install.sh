#!/usr/bin/env bash
set -euo pipefail

FORCE=0

usage() {
  cat <<'EOF'
usage: ./install.sh [--force]

Installs the Forge binary, config, env file, storage directories, and systemd unit
without changing runtime semantics or exposing the API publicly by default.
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

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BIN_DEST="/usr/local/bin/forge"
CONFIG_DIR="/etc/forge"
CONFIG_PATH="$CONFIG_DIR/forge.conf"
ENV_PATH="$CONFIG_DIR/forge.env"
UNIT_PATH="/etc/systemd/system/forge.service"
SERVICE_SRC="$REPO_ROOT/deploy/forge.service"
CONFIG_TEMPLATE="$REPO_ROOT/deploy/forge.conf.example"
STORAGE_ROOT="/var/lib/forge"
SAMPLE_ROOT="/srv/forge/sample-http-app"

if [ "$(id -u)" -eq 0 ]; then
  SUDO=()
else
  command -v sudo >/dev/null 2>&1 || die "run as root or install sudo"
  SUDO=(sudo)
fi

random_hex() {
  local bytes="${1:?missing byte count}"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex "$bytes"
  else
    od -An -N"$bytes" -tx1 /dev/urandom | tr -d ' \n'
  fi
}

install_if_missing_or_forced() {
  local src="$1"
  local dest="$2"
  local mode="$3"

  if [ -e "$dest" ] && [ "$FORCE" -ne 1 ]; then
    log "preserving existing $dest"
    return 0
  fi

  "${SUDO[@]}" install -D -m "$mode" "$src" "$dest"
  log "installed $dest"
}

write_if_missing_or_forced() {
  local dest="$1"
  local mode="$2"
  local tmp
  tmp="$(mktemp)"
  cat >"$tmp"

  if [ -e "$dest" ] && [ "$FORCE" -ne 1 ]; then
    rm -f "$tmp"
    log "preserving existing $dest"
    return 0
  fi

  "${SUDO[@]}" install -D -m "$mode" "$tmp" "$dest"
  rm -f "$tmp"
  log "installed $dest"
}

[ -f "$CONFIG_TEMPLATE" ] || die "missing config template: $CONFIG_TEMPLATE"

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

[ -x "$BIN_SRC" ] || die "forge binary not found at $BIN_SRC"

if ! id forge >/dev/null 2>&1; then
  command -v useradd >/dev/null 2>&1 || die "useradd is required to create the forge service account"
  "${SUDO[@]}" useradd --system --home-dir /srv/forge --shell /usr/sbin/nologin forge
  log "created forge system user"
fi

"${SUDO[@]}" install -d -m 0755 /usr/local/bin "$CONFIG_DIR" "$STORAGE_ROOT" /srv/forge "$SAMPLE_ROOT"
"${SUDO[@]}" install -d -m 0755 \
  "$STORAGE_ROOT/projects" \
  "$STORAGE_ROOT/events" \
  "$STORAGE_ROOT/secrets" \
  "$STORAGE_ROOT/indexes" \
  "$STORAGE_ROOT/idempotency" \
  "$STORAGE_ROOT/queue"
"${SUDO[@]}" chown -R forge:forge "$STORAGE_ROOT" /srv/forge

"${SUDO[@]}" install -m 0755 "$BIN_SRC" "$BIN_DEST"
log "installed $BIN_DEST"

if [ -e "$CONFIG_PATH" ] && [ "$FORCE" -ne 1 ]; then
  log "preserving existing $CONFIG_PATH"
else
  bearer_token="$(random_hex 32)"
  config_tmp="$(mktemp)"
  sed "s/replace-with-a-long-random-token/$bearer_token/" "$CONFIG_TEMPLATE" >"$config_tmp"
  "${SUDO[@]}" install -m 0644 "$config_tmp" "$CONFIG_PATH"
  rm -f "$config_tmp"
  log "installed $CONFIG_PATH"
fi

if [ -e "$ENV_PATH" ] && [ "$FORCE" -ne 1 ]; then
  log "preserving existing $ENV_PATH"
else
  master_key="$(random_hex 32)"
  write_if_missing_or_forced "$ENV_PATH" 0644 <<EOF
FORGE_MASTER_KEY=$master_key
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019
FORGE_CADDY_PUBLIC_URL=http://127.0.0.1
EOF
fi

if [ -f "$SERVICE_SRC" ]; then
  unit_installed=0
  if [ -e "$UNIT_PATH" ] && [ "$FORCE" -ne 1 ]; then
    log "preserving existing $UNIT_PATH"
  else
    install_if_missing_or_forced "$SERVICE_SRC" "$UNIT_PATH" 0644
    unit_installed=1
  fi

  if [ "$unit_installed" -eq 1 ] && command -v systemctl >/dev/null 2>&1; then
    "${SUDO[@]}" systemctl daemon-reload
    log "reloaded systemd units"
  fi
else
  warn "deploy/forge.service not found; skipping systemd unit install"
fi

if ! command -v docker >/dev/null 2>&1; then
  warn "Docker is not installed. Install and start Docker yourself; this script does not install it."
elif ! docker version >/dev/null 2>&1; then
  warn "Docker is installed but not reachable for the current user. Start the daemon and re-run 'forge doctor'."
fi

if ! command -v caddy >/dev/null 2>&1; then
  warn "Caddy is not installed. Install it yourself with the admin API enabled on http://127.0.0.1:2019."
elif ! caddy version >/dev/null 2>&1; then
  warn "Caddy is installed but could not be queried. Ensure the binary works and the admin API stays on localhost."
fi

cat <<'EOF'

WorkingDirectory note:
  Manual 'forge deploy <project> <environment>' builds from the daemon WorkingDirectory.
  The installed unit defaults to /srv/forge/sample-http-app. Point it at the project root you
  want manual deploys to build from before enabling the service.

Next steps:
  forge doctor
  sudo systemctl enable --now forge
  forge init
  forge deploy api production
EOF
