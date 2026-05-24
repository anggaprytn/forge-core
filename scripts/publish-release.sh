#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${FORGE_PACKAGE_OUTPUT_DIR:-$REPO_ROOT/dist}"
SIGNING_KEY="${FORGE_RELEASE_SIGNING_KEY:-}"
REPOSITORY="${FORGE_RELEASE_REPOSITORY:-}"
ALLOW_NON_HEAD=0
UPLOAD_PUBLIC_KEY=1
TAG=""
GH_TIMEOUT_SECS="${FORGE_PUBLISH_GH_TIMEOUT_SECS:-30}"

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

usage() {
  cat <<'EOF'
Usage:
  scripts/publish-release.sh <tag> [--signing-key <path>] [--allow-non-head] [--no-public-key-upload]

Environment:
  FORGE_RELEASE_SIGNING_KEY   Default signing key path for scripts/package-release.sh --sign
  FORGE_RELEASE_REPOSITORY    GitHub repository slug (owner/repo). Defaults to git remote origin.
EOF
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --signing-key)
        shift
        [ "$#" -gt 0 ] || die "--signing-key requires a path"
        SIGNING_KEY="$1"
        ;;
      --allow-non-head)
        ALLOW_NON_HEAD=1
        ;;
      --no-public-key-upload)
        UPLOAD_PUBLIC_KEY=0
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      --*)
        die "unknown argument: $1"
        ;;
      *)
        [ -z "$TAG" ] || die "tag already provided: $TAG"
        TAG="$1"
        ;;
    esac
    shift
  done
}

require_clean_tree() {
  local status
  status="$(git status --short --untracked-files=normal 2>/dev/null)" || die "git status failed"
  [ -z "$status" ] || die "git tree must be clean before publishing a release"
}

resolve_repository() {
  if [ -n "$REPOSITORY" ]; then
    printf '%s\n' "$REPOSITORY"
    return 0
  fi
  local remote
  remote="$(git remote get-url origin 2>/dev/null || true)"
  [ -n "$remote" ] || die "could not determine GitHub repository; set FORGE_RELEASE_REPOSITORY=owner/repo"
  python3 - "$remote" <<'PY'
import re
import sys

remote = sys.argv[1].strip()
match = re.search(r'github\.com[:/](?P<slug>[^/]+/[^/.]+?)(?:\.git)?$', remote)
if not match:
    raise SystemExit(1)
print(match.group('slug'))
PY
}

verify_tag() {
  local tag_commit head_commit
  git rev-parse --verify "${TAG}^{commit}" >/dev/null 2>&1 || die "tag not found: $TAG"
  tag_commit="$(git rev-parse "${TAG}^{commit}")"
  head_commit="$(git rev-parse HEAD)"
  if [ "$ALLOW_NON_HEAD" -ne 1 ] && [ "$tag_commit" != "$head_commit" ]; then
    die "tag $TAG does not point to HEAD ($head_commit); rerun with --allow-non-head to override"
  fi
}

require_gh() {
  command -v gh >/dev/null 2>&1 || die "gh CLI is required to publish GitHub releases"
  run_with_timeout "$GH_TIMEOUT_SECS" gh auth status >/dev/null \
    || die "gh CLI is not authenticated; run 'gh auth login' and retry"
}

require_signed_bundle() {
  [ -f "$DIST_DIR/release-manifest.json" ] || die "missing release-manifest.json; signed packaging did not complete"
  [ -f "$DIST_DIR/release-manifest.sig" ] || die "missing release-manifest.sig; refusing unsigned publish"
  [ -f "$DIST_DIR/checksums.txt" ] || die "missing checksums.txt; signed packaging did not complete"
  [ -f "$DIST_DIR/release-public-key.pem" ] || die "missing release-public-key.pem; signed packaging did not complete"
  local artifacts=("$DIST_DIR"/forge-*.tar.gz)
  [ -e "${artifacts[0]}" ] || die "missing packaged forge artifact under $DIST_DIR"
}

generate_release_notes() {
  local manifest_path="$DIST_DIR/release-manifest.json"
  local notes_path="$DIST_DIR/RELEASE_NOTES.md"
  python3 - "$manifest_path" "$notes_path" "$TAG" "$(git rev-parse HEAD)" <<'PY'
import json
import sys

manifest_path, notes_path, tag, head_commit = sys.argv[1:5]
manifest = json.load(open(manifest_path, "r", encoding="utf-8"))
schema = manifest["schema_versions"]
artifacts = manifest["artifacts"]

lines = [
    f"# Forge Release {tag}",
    "",
    "## Build Metadata",
    f"- Tag: `{tag}`",
    f"- Git commit: `{head_commit}`",
    f"- Forge version: `{manifest['version']}`",
    f"- Manifest git_commit: `{manifest['git_commit']}`",
    f"- Build timestamp: `{manifest['build_timestamp']}`",
    f"- Dirty tree: `{'true' if manifest['git_dirty'] else 'false'}`",
    "",
    "## Schemas",
    f"- Manifest schema: `{schema['manifest_schema']}`",
    f"- Snapshot schema: `{schema['snapshot_schema']}`",
    f"- Checkpoint schema: `{schema['checkpoint_schema']}`",
    f"- Reconciliation log schema: `{schema['reconciliation_log_schema']}`",
    f"- Storage compatibility: `{schema['storage_compatibility_version']}`",
    "",
    "## Artifacts",
]
for artifact in artifacts:
    lines.append(
        f"- `{artifact['name']}` ({artifact['target_triple']}, sha256 `{artifact['sha256']}`)"
    )
lines.extend(
    [
        "",
        "## Validation Commands",
        "```bash",
        "sha256sum -c checksums.txt",
        "openssl pkeyutl -verify -pubin -inkey release-public-key.pem -sigfile <(base64 -d release-manifest.sig) -rawin -in release-manifest.json",
        f"./install.sh --release {tag}",
        f"forge upgrade plan --release {tag}",
        "```",
        "",
        "## Upgrade Instructions",
        "```bash",
        f"./install.sh --release {tag}",
        f"forge upgrade plan --release {tag}",
        f"forge upgrade apply --release {tag}",
        "forge upgrade rollback",
        "```",
        "",
        "## Security Notes",
        "- Verify the release manifest signature before trusting downloaded artifacts.",
        "- Use `forge upgrade plan` and `forge upgrade apply` for operator-managed upgrades.",
        "- `syncforge` remains development-only and is not part of the operator release path.",
        "",
    ]
)

with open(notes_path, "w", encoding="utf-8") as handle:
    handle.write("\n".join(lines))
PY
  log "wrote $notes_path"
}

publish_release() {
  local repository="$1"
  local notes_path="$DIST_DIR/RELEASE_NOTES.md"
  local assets=(
    "$DIST_DIR"/forge-*.tar.gz
    "$DIST_DIR/release-manifest.json"
    "$DIST_DIR/release-manifest.sig"
    "$DIST_DIR/checksums.txt"
  )
  if [ "$UPLOAD_PUBLIC_KEY" -eq 1 ] && [ -f "$DIST_DIR/release-public-key.pem" ]; then
    assets+=("$DIST_DIR/release-public-key.pem")
  fi

  if run_with_timeout "$GH_TIMEOUT_SECS" gh release view "$TAG" --repo "$repository" >/dev/null; then
    run_with_timeout "$GH_TIMEOUT_SECS" gh release edit "$TAG" --repo "$repository" --notes-file "$notes_path" >/dev/null
  else
    run_with_timeout "$GH_TIMEOUT_SECS" gh release create "$TAG" --repo "$repository" --title "$TAG" --notes-file "$notes_path" >/dev/null
  fi
  run_with_timeout "$GH_TIMEOUT_SECS" gh release upload "$TAG" --repo "$repository" --clobber "${assets[@]}" >/dev/null
  log "published GitHub release $TAG to $repository"
}

main() {
  parse_args "$@"
  [ -n "$TAG" ] || die "tag argument required"
  [ -n "$SIGNING_KEY" ] || die "signing key required; pass --signing-key <path> or set FORGE_RELEASE_SIGNING_KEY"
  require_clean_tree
  verify_tag
  require_gh
  local repository
  repository="$(resolve_repository)" || die "failed to resolve GitHub repository from origin remote"

  (
    cd "$REPO_ROOT"
    scripts/package-release.sh --sign --signing-key "$SIGNING_KEY"
  )
  require_signed_bundle
  generate_release_notes
  publish_release "$repository"
}

main "$@"
