#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${FORGE_PACKAGE_OUTPUT_DIR:-$REPO_ROOT/dist}"
VERSION="${FORGE_PACKAGE_VERSION:-$(sed -n 's/^version = "\(.*\)"/\1/p' "$REPO_ROOT/Cargo.toml" | head -n 1)}"
TARGETS="${FORGE_PACKAGE_TARGETS:-}"
BIN_DIR="${FORGE_PACKAGE_BIN_DIR:-}"
README_SRC="${FORGE_PACKAGE_README:-$REPO_ROOT/README.md}"
CONFIG_SRC="${FORGE_PACKAGE_CONFIG:-$REPO_ROOT/deploy/forge.conf.example}"
ENV_SRC="${FORGE_PACKAGE_ENV:-$REPO_ROOT/examples/forge.env.example}"
LICENSE_SRC="${FORGE_PACKAGE_LICENSE:-$REPO_ROOT/LICENSE}"
INSTALLER_SRC="$REPO_ROOT/install.sh"
PACKAGE_TIMEOUT_SECS="${FORGE_PACKAGE_TIMEOUT_SECS:-1800}"

log() {
  printf '[INFO] %s\n' "$*"
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

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1"
  else
    shasum -a 256 "$1"
  fi
}

host_targets() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os:$arch" in
    Linux:x86_64)
      printf '%s\n' "linux-amd64"
      ;;
    Darwin:arm64)
      printf '%s\n' "darwin-arm64"
      ;;
    *)
      die "unsupported packaging host $os/$arch; set FORGE_PACKAGE_TARGETS and FORGE_PACKAGE_BIN_DIR if you want to package prebuilt binaries"
      ;;
  esac
}

target_triple() {
  case "$1" in
    linux-amd64) printf '%s\n' "x86_64-unknown-linux-gnu" ;;
    darwin-arm64) printf '%s\n' "aarch64-apple-darwin" ;;
    *) return 1 ;;
  esac
}

binary_path_for_target() {
  local target="$1"
  if [ -n "$BIN_DIR" ]; then
    printf '%s\n' "$BIN_DIR/$target/forge"
    return 0
  fi
  local triple
  triple="$(target_triple "$target")" || die "unsupported target label: $target"
  run_with_timeout "$PACKAGE_TIMEOUT_SECS" cargo build --release --bin forge --target "$triple" >/dev/null
  printf '%s\n' "$REPO_ROOT/target/$triple/release/forge"
}

stage_target() {
  local target="$1"
  local bin_path archive_name stage_dir
  bin_path="$(binary_path_for_target "$target")"
  [ -x "$bin_path" ] || die "missing forge binary for $target at $bin_path"

  archive_name="forge-$VERSION-$target.tar.gz"
  stage_dir="$(mktemp -d)"
  install -m 0755 "$bin_path" "$stage_dir/forge"
  install -m 0755 "$INSTALLER_SRC" "$stage_dir/install.sh"
  install -m 0644 "$README_SRC" "$stage_dir/README.md"
  install -m 0644 "$CONFIG_SRC" "$stage_dir/forge.conf.example"
  install -m 0644 "$ENV_SRC" "$stage_dir/forge.env.example"
  if [ -f "$LICENSE_SRC" ]; then
    install -m 0644 "$LICENSE_SRC" "$stage_dir/LICENSE"
  fi

  mkdir -p "$DIST_DIR"
  (
    cd "$stage_dir"
    run_with_timeout "$PACKAGE_TIMEOUT_SECS" tar -czf "$DIST_DIR/$archive_name" \
      forge \
      install.sh \
      README.md \
      forge.conf.example \
      forge.env.example \
      $( [ -f "$stage_dir/LICENSE" ] && printf '%s' "LICENSE" )
  )
  rm -rf "$stage_dir"
  log "packaged $DIST_DIR/$archive_name"
}

main() {
  [ -n "$VERSION" ] || die "could not determine package version"
  [ -f "$INSTALLER_SRC" ] || die "missing installer: $INSTALLER_SRC"
  [ -f "$README_SRC" ] || die "missing README/RELEASE_NOTES: $README_SRC"
  [ -f "$CONFIG_SRC" ] || die "missing config example: $CONFIG_SRC"
  [ -f "$ENV_SRC" ] || die "missing env example: $ENV_SRC"

  if [ -z "$TARGETS" ]; then
    TARGETS="$(host_targets | paste -sd, -)"
  fi

  rm -rf "$DIST_DIR"
  mkdir -p "$DIST_DIR"
  IFS=',' read -r -a target_list <<<"$TARGETS"
  for target in "${target_list[@]}"; do
    stage_target "$target"
  done

  : >"$DIST_DIR/checksums.txt"
  for archive in "$DIST_DIR"/*.tar.gz; do
    sha256_file "$archive" >>"$DIST_DIR/checksums.txt"
  done
  log "wrote $DIST_DIR/checksums.txt"
}

main "$@"
