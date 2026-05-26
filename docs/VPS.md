# Forge VPS Guide

This guide documents the current single-node operator path:

```txt
forge daemon
→ HTTP API
→ CLI/API deploy flow
```

It is intentionally aligned to the current implementation, not an aspirational installer.

## Alpha Core Loop v5 Validated (May 2026)

The Forge Alpha Core Loop v5 milestone freezes the durable single-writer control plane on VPS infrastructure.

### Validated Capabilities (v5)

- **Durable Checkpoints**: `convergence_checkpoint.json` restores cache-backed readiness after restart. `readyz_status` and degraded reasons are restored only as cached initial state; the first healthy leader convergence refresh must replace stale recovered readiness before stall detection can degrade again.
- **Control-Plane Snapshots**: `runtime_snapshot.json`, `route_snapshot.json`, and `dependency_snapshot.json` support diagnostics with bounded retention.
- **Persistent Node Identity**: Stable `node_id`, metadata, boot timestamp, and capabilities survive daemon restart.
- **Operational Journal**: `operations.jsonl` records leadership, degradation, route, deploy/restore, and GC events.
- **Lease-Based Single Writer**: `leader_lease.json` fences mutating work to one active leader and advances `lease_epoch` on takeover.
- **Follower Read-Only Mode**: Followers serve cache-backed reads only and never mutate shared control-plane state.
- **Split-Brain Detection Scaffolding**: `cluster_nodes.json` tracks heartbeat observations and degraded signals such as `split_brain_suspected`.
- **Deterministic Replay Recovery**: Startup phases are explicit and replay is bounded, resumable, leader-only, and quarantine-aware.
- **Cache-Backed Request Paths**: `/readyz` and `/metrics` remain bounded and do not perform fleet scans.
- **Validated Live Checks**: local `/readyz` around `8ms`; `forge bench leader` and `forge bench convergence` around `0.23ms` p95; daemon restart returns to `leader_active`.

v5.1 note: stale checkpointed `convergence_stalled` readiness no longer survives healthy restart, upgrade apply, or upgrade rollback handoff. Healthy leader recovery refreshes `convergence_last_success_unix`, clears stale degraded cache, and keeps `convergence_failures_total` at `0` when no fresh failure occurred.

## Alpha Core Loop v2 Validated (May 2026)

The Forge Alpha Core Loop v2 milestone formalizes the second validated operational maturity milestone for the Forge platform. This milestone freezes the core orchestration loop after extensive validation of progressive lifecycles, lifecycle persistence, retention/GC, immutable environment snapshots, and convergence-driven runtime truth alignment.

### Validated Capabilities (v2)

- **Progressive Deployment Lifecycle**: Deterministic state transitions from `queued` through `promoted`.
- **Lifecycle Persistence**: Full per-generation lifecycle state tracking and recovery.
- **Retention & GC**: Rollback-safe generation preservation with automatic cleanup of expired artifacts.
- **Immutable Env Snapshots**: Fully resolved and sealed runtime environment snapshots per generation.
- **Diagnostics & Logs**: Bounded, secret-redacted deployment logs and deep-inspection diagnostics.
- **Secret Lifecycle**: Immutable secret snapshots with historical restoration during rollback.
- **Probe Stability Semantics**: Hysteresis-aware health probing with flapping detection and stability windows.
- **Convergence & Runtime Truth**: Continuous repair of routing and container state toward the promoted truth.

## Alpha Core Loop v1 Validated (May 2026)

The Forge Alpha Core Loop v1 milestone formalizes the first validated end-to-end Forge platform baseline after successful live staging and production deployments on VPS infrastructure.

### Validated Capabilities

- **forge login**: Mac CLI login to remote Forge server.
- **forge project add --repo**: Project registration from GitHub repository.
- **git-backed deploy by ref**: Source-controlled deployment from branches or tags.
- **Environment targets**: Staging and production deployment workflows.
- **Generated environment domains**: Automatic derivation of staging/production domains.
- **Immutable source checkout**: Server-side source resolution and cache management.
- **Managed Docker runtime network**: Isolated container networks with Forge-managed lifecycles.
- **Runtime validation and health probing**: TCP reachability and HTTP health check enforcement.
- **Route activation and convergence**: Atomic Caddy route updates following successful validation.
- **forge status**: Lightweight runtime and environment summary for operators.
- **forge diagnose**: Deep inspection of runtime truth and failure reasons.
- **forge env**: Inspection of generation-scoped runtime environment variables.
- **Runtime env snapshots**: Authoritative, redacted snapshots of the effective runtime environment.
- **Rollback**: Atomic restoration of the previous healthy generation and its specific metadata.
- **Authoritative pointers**: Deterministic current/previous pointer semantics.
- **Runtime metadata injection**: Automatic injection of Forge-scoped context (Project ID, Generation, etc.).
- **Route drift repair**: Continuous convergence of routing state toward the authoritative generation.
- **Deterministic recovery**: Reliable reconstruction of runtime state after daemon or host restarts.

### Validated Deployment Example

```bash
# 1. Login to your Forge server
forge login https://forge.example.com

# 2. Register a project from a GitHub repository
forge project add \
  --repo https://github.com/example/repo.git

# 3. Deploy to staging from the main branch
forge deploy my-app staging --ref main

# 4. Inspect status and domains
forge status my-app staging
# Staging domain: staging-my-app.example.com
# Production domain: my-app.example.com

# 5. Inspect runtime environment and diagnostics
forge env my-app staging
forge diagnose my-app staging

# 6. Rollback if needed
forge rollback my-app staging
```

## 2. Prerequisites (Docker & Caddy)

Forge does not install Docker or Caddy for you. Install them using your distribution's package manager.

### Install Docker

```bash
apt-get update
apt-get install -y docker.io
systemctl enable --now docker
```

### Install Caddy

```bash
apt-get update
apt-get install -y caddy
systemctl enable --now caddy
```

### Configure Caddy Admin API

Forge expects the Caddy admin API at `http://127.0.0.1:2019`.

Example `/etc/caddy/Caddyfile`:

```caddyfile
{
	admin 127.0.0.1:2019
}

:80 {
	respond "Not Found" 404
}
```

Forge writes its own managed fallback route and machine-detectable markers through the Caddy admin
API. Keep the bootstrap Caddyfile fail-closed instead of embedding a stale manual fallback body.

Restart Caddy: `systemctl restart caddy`.

---

## 3. Conservative Installation

For Linux hosts with systemd, use the provided conservative installer:

```bash
./install.sh --release <tag>
```

The installer is **idempotent** and safe:
- Installs `forge` to `/usr/local/bin`.
- Creates `/etc/forge/forge.conf` and `/etc/forge/forge.env` if missing.
- Prepares `/var/lib/forge` for storage.
- Installs the systemd unit `forge.service`.

---

## 4. Host Directory & Permissions

While `install.sh` creates the storage root, you must ensure your application checkout is accessible:

```bash
# Example project directory
mkdir -p /srv/forge/sample-http-app
chown -R forge:forge /var/lib/forge
```

### Critical Permission Rules
- **Storage**: `/var/lib/forge` (storage_root) MUST be owned by the `forge` service user.
- **Project**: The `WorkingDirectory` (e.g., `/srv/forge/sample-http-app`) must be readable and traversable by the `forge` service user.

---

## 5. Initialize Your Project

Go to your application directory and initialize `forge.yml`:

```bash
cd /srv/forge/sample-http-app
forge init
```

Forge strictly validates `forge.yml`. Unsupported fields are rejected.

---

## 6. Configure Forge Environment

Update `/etc/forge/forge.env` with your master key:

```bash
FORGE_MASTER_KEY=<64 hex characters>
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019
FORGE_CADDY_PUBLIC_URL=https://api.forge.example.com
FORGE_APPS_DOMAIN=forge.example.com
```

`FORGE_MASTER_KEY` is required for secrets support.
`FORGE_APPS_DOMAIN` is required only if you want Forge to generate project base domains when `forge project add` omits `--domain`.

Future derived app domains require wildcard DNS aimed at the VPS:

```txt
*.forge.example.com -> <your VPS public IP>
```

Example future domains:

- `api.forge.example.com`
- `api-k7x9q2.forge.example.com` (collision fallback)
- `staging-api-k7x9q2.forge.example.com`
- `development-api-k7x9q2.forge.example.com`

Forge web login is part of the human operator control surface and requires these env vars:

```bash
FORGE_GITHUB_OAUTH_CLIENT_ID=...
FORGE_GITHUB_OAUTH_CLIENT_SECRET=...
FORGE_PUBLIC_URL=https://forge.example.com
FORGE_SESSION_SECRET=...
FORGE_CLI_TOKEN_SECRET=...
ALLOW_NEW_REGISTRATION=false
```

`FORGE_PUBLIC_URL` must be the public Forge origin used by operators and `forge login`.
`FORGE_CLI_TOKEN_SECRET` signs CLI bearer tokens issued after browser approval.
`ALLOW_NEW_REGISTRATION` controls whether GitHub OAuth may create a new Forge user. Default is `false`.

`/login` starts the GitHub OAuth flow, `/app` requires the resulting session cookie, `/login/cli?code=...` serves the CLI approval page, and `/api/cli-login/*` drives the short-lived browser approval flow used by `forge login`.
The static control-plane assets under `web/` are served by Forge itself, so the same process remains authoritative for auth, session validation, and protected page delivery.
CLI commands and bearer-token API auth remain available for automation and operator usage. Web actions are not a separate deployment engine; they flow through the same API and queue.

Bootstrap a new self-hosted install like this:

1. Set `ALLOW_NEW_REGISTRATION=true`.
2. Log in once with the owner GitHub account.
3. Set `ALLOW_NEW_REGISTRATION=false`.
4. Restart Forge.

```bash
sudo editor /etc/forge/forge.env

ALLOW_NEW_REGISTRATION=false

sudo systemctl restart forge
```

When registration is closed, existing users can still log in. Unknown GitHub users are rejected with `Registration is closed`. This is registration control only, not RBAC, teams, invites, billing, or org policy.

---

## 7. Run Diagnostics

Before starting the service, verify your environment:

```bash
FORGE_CONFIG=/etc/forge/forge.conf \
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019 \
FORGE_MASTER_KEY=<64 hex characters> \
forge doctor
```

---

## 8. Start the Forge Daemon

```bash
systemctl daemon-reload
systemctl enable --now forge
```

### Manual Deployment Note
`forge deploy <project> <environment>` now resolves source from the registered project repository using the project's `default_branch`, or `--ref <ref>` when provided. Forge requires `git` to be installed on the server, reuses a repository cache under `/var/lib/forge/repositories/<project_id>/`, and deploys from immutable checkouts under `/var/lib/forge/source-checkouts/<project_id>/<commit_sha>/`. `--from` remains available for explicit local-path deploys.

---

## 9. Verify Readiness

```bash
curl http://127.0.0.1:8080/healthz
curl -s http://127.0.0.1:8080/readyz | jq
curl -s http://127.0.0.1:8080/metrics | jq
forge control-plane leader
forge control-plane lease
forge --url http://127.0.0.1:8080 bench leader
forge --url http://127.0.0.1:8080 bench convergence
forge --url http://127.0.0.1:8080 bench diagnostics
forge --url http://127.0.0.1:8080 bench snapshots
```

Semantics:

- `/healthz`: process liveness only
- `/readyz`: control-plane readiness only
- `forge status`: lightweight operational inspection
- `forge diagnose`: deep diagnostics for operators

`/readyz` serves cached convergence state. It must not perform synchronous Docker scans, Caddy scans, route reconciliation, generation reconciliation, or environment-wide diagnostics on the request path.

`/metrics` separates active readiness from historical convergence observability. Historical counters such as `convergence_failures_total` remain monotonic, while active blockers are exposed through `readiness_status`, `readiness_reason`, `convergence_active_failure`, and `convergence_active_failure_reason`.

Checkpoint-restored `/readyz` is cached startup context, not final truth. Operators may briefly see restored degraded reasons such as `convergence_stalled`, but once startup reaches `leader_active` with replay complete and healthy convergence domains, the next cache refresh must recompute readiness from fresh leader state and clear the stale marker.

Readiness derives from cached control-plane inputs such as storage accessibility, queue health, Docker reachability, Caddy admin reachability, unresolved fatal markers, and convergence freshness. Environment-level health belongs to diagnostics, not readiness.

Forge is still single-writer. The new multi-node work is preparatory only. The lease is a safety primitive, not HA consensus:

- the active leader refreshes `control_plane/leader_lease.json`
- only the lease owner may reconcile shared control-plane state
- follower nodes serve cached reads only, including cached readiness and metrics, without mutating shared control-plane state
- lease takeover is allowed only after expiry and advances a monotonic `lease_epoch`
- mutating APIs require the active leader
- the filesystem-backed lease is not safe for true multi-writer distributed storage unless all nodes share a filesystem with correct atomic semantics

Leadership-specific degraded readiness reasons now include:

- `leadership uncertain`
- `convergence ownership lost`
- `lease stale`
- `checkpoint epoch mismatch`

Performance targets:

- local `/readyz`: under 250ms
- public `/readyz` TTFB: under 1s
- stale readiness cache: degrade immediately

Example degraded response:

```json
{
  "status": "degraded",
  "reason": "readiness cache stale"
}
```

Observed validation:

```bash
forge control-plane leader
forge control-plane lease
forge --url http://127.0.0.1:18080 bench leader
time curl -s http://127.0.0.1:18080/readyz >/dev/null
# ~13ms

curl -sk -o /dev/null \
  -w 'ttfb=%{time_starttransfer} total=%{time_total}\n' \
  https://forge.anggaprytn.com/readyz
# ttfb=0.028 total=0.028
```

The previous implementation coupled readiness to synchronous fleet-wide diagnostics and exhibited pathological 48s to 150s latency.

---

## 10. Deploy the Sample App

Set the CLI client environment for bearer-token auth, or use browser approval:

```bash
export FORGE_URL=http://127.0.0.1:8080
export FORGE_TOKEN=replace-with-the-bearer_token-from-forge.conf
forge login https://forge.example.com
```

Enqueue the deploy:

```bash
forge deploy api production
forge deploy api production --ref main
forge deploy api production --from /srv/forge/sample-http-app
forge events
```

Stateful workflow examples:

```bash
forge backup create api production
forge backup list api production
forge backup inspect backup-1
forge backup restore backup-1
forge diagnose api production
```

After restore, `forge diagnose api production` should show restore lineage, including the backup ID, source generation, and restored Docker volume names for stateful services.

Project registry examples:

```bash
forge project add --repo https://github.com/example/api.git
forge project add api --repo https://github.com/example/api.git
forge project add api --repo https://github.com/example/api.git --domain api.example.com
forge project list
forge project show api
```

`project_id` is optional when `--repo` is present. Forge infers and normalizes it from the repository basename, then applies the same safe ID validation. This registry metadata is also the source of truth for deploy-by-ref source resolution.

Cleanup and orphan recovery outcomes are emitted into the same event stream:

```bash
forge events | rg 'ORPHANED_|CLEANUP_'
```

---

## Troubleshooting VPS Deployments

Probe guidance:

- Use `/healthz` for liveness probes.
- Use `/readyz` for load balancer and readiness probes.
- Use `forge status` for operational overview.
- Use `forge diagnose` for deep debugging.
- Do not use `/readyz` as fleet health inspection or per-project monitoring.
- Do not couple readiness probes to expensive reconciliation work.

- **Caddy server ID**: Ensure Caddy is configured with server ID `"forge"`.
- **Port Conflicts**: If port 8080 is taken, update `api_bind` in `forge.conf` and `FORGE_URL`.
- **API Visibility**: Keep the API bound to `localhost` (127.0.0.1) for security.

## Token And Secret Hygiene

- `bearer_token` in `forge.conf` is the bootstrap/admin credential. Prefer CLI tokens for day-to-day remote operation.
- Manage CLI tokens with `forge token list`, `forge token create --name <name>`, and `forge token revoke <token_id>`.
- Forge stores only token hashes server-side and shows token plaintext once at creation time.
- Redaction covers `Authorization` headers, bearer tokens, Forge master keys, OAuth client secrets, GitHub tokens, and sensitive app values in logs and diagnostics.

## CLI Token Secret Rotation

1. Set `FORGE_CLI_TOKEN_SECRET_CURRENT` to the new secret.
2. Set `FORGE_CLI_TOKEN_SECRET_PREVIOUS` to the old secret during the migration window.
3. Restart Forge.
4. Have every operator run `forge login <server_url>` again.
5. Remove `FORGE_CLI_TOKEN_SECRET_PREVIOUS` after the old tokens are retired.

## Backup Handling

- Backups may contain sensitive application data.
- Backups are not encrypted yet.
- Protect `/var/lib/forge/backups` with restrictive filesystem permissions and host access controls.

## Upgrade Preflight

- Use `forge version` to capture runtime build identity, target triple, and schema/storage compatibility versions.
- Use `forge doctor upgrade` before upgrades. It is read-only and checks storage readability, schema compatibility, Docker, Caddy, write access, and Linux `systemd` sanity.
- Build release artifacts with `scripts/package-release.sh --sign --signing-key <path>` whenever a signing key is available. Development-only packages must use `scripts/package-release.sh --unsigned`.
- Publish a tagged GitHub release with `scripts/publish-release.sh <tag> --signing-key <path>`. It requires a clean tree, verifies the tag, writes `dist/RELEASE_NOTES.md`, and uploads the signed bundle through `gh`.
- Signed packaging emits `dist/release-manifest.json`, `dist/release-manifest.sig`, and `dist/release-public-key.pem`. Configure the operator node with `release_public_key_path=/etc/forge/release-public-key.pem` or `FORGE_RELEASE_PUBLIC_KEY=/path/to/release-public-key.pem`.
- Install a pinned release with `./install.sh --release <tag>` after the public key is in place.
- Use `forge upgrade plan --release <tag>` before swapping binaries.
- Use `forge upgrade apply --release <tag>` for the actual upgrade.
- If you need an offline path, keep the local artifact form: `forge upgrade plan --artifact dist/forge-<version>-linux-amd64.tar.gz --manifest dist/release-manifest.json --signature dist/release-manifest.sig`.
- If no public key is configured, Forge fails closed unless `--allow-unsigned` is passed explicitly. Keep `--allow-unsigned` for development-only or emergency testing paths.
- `install.sh` also fails closed for release downloads unless `--allow-unsigned-release` is passed explicitly.
- If a release was built from a dirty worktree, Forge rejects it unless `--allow-dirty-artifact` is passed explicitly.
- Use `forge upgrade rollback` for emergency binary restore from `/usr/local/bin/forge.previous`.
- Verify the manifest signature, use `forge upgrade plan/apply`, and treat `syncforge` as development-only; release artifacts are the preferred production path.
- Threat model: signatures provide tamper evidence for the manifest and release tarballs listed in it. They do not sandbox the artifact binary.
