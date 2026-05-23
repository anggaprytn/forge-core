# Forge VPS Guide

This guide documents the current single-node operator path:

```txt
forge daemon
→ HTTP API
→ CLI/API deploy flow
```

It is intentionally aligned to the current implementation, not an aspirational installer.

## Alpha Core Loop v4 Validated (May 2026)

The Forge Alpha Core Loop v4 milestone freezes the current single-node stateful orchestration loop on VPS infrastructure with persisted runtime policy and degraded-runtime operator signals.

### Validated Capabilities (v4)

- **Per-Service CPU/Memory/Restart Policy**: Runtime policy persists per service and round-trips through status, diagnostics, rollback, and convergence.
- **Rollback Restores Historical Runtime Policy**: VPS rollback restores the exact historical runtime policy of the rollback target.
- **Convergence Repairs Runtime Policy Drift**: Manual Docker-side policy drift is detected and repaired back to promoted truth.
- **OOM/Crash-Loop/Restart-Storm Promotion Gates**: Warmup refuses to promote unstable services.
- **Termination Diagnostics**: Diagnose/status expose exit reason, signal, OOM state, restart count, and tails when available.
- **Runtime Usage Snapshots**: Active services expose captured CPU and memory usage snapshots.
- **Non-Fatal Route Repair Failures**: Route repair issues degrade readiness without falsely claiming full readiness.
- **Readyz Active Degradation Semantics**: `/readyz` may return `degraded` with active repair reasons while `/healthz` remains live.
- **Clean Diagnostics API Repair Fields**: Current unresolved runtime policy and volume repair events remain visible; healthy historical noise is suppressed.
- **Multi-Service Stateful Baseline**: v3 topology, state, backup/restore, and restore-lineage guarantees remain validated.

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
- **forge status**: Project and environment health and runtime monitoring.
- **forge diagnose**: Deep inspection of runtime state and failure reasons.
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
	respond "caddy ready" 200
}
```

Restart Caddy: `systemctl restart caddy`.

---

## 3. Conservative Installation

For Linux hosts with systemd, use the provided conservative installer:

```bash
./install.sh
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
```

`FORGE_PUBLIC_URL` must be the public Forge origin used by operators and `forge login`.
`FORGE_CLI_TOKEN_SECRET` signs CLI bearer tokens issued after browser approval.

`/login` starts the GitHub OAuth flow, `/app` requires the resulting session cookie, `/login/cli?code=...` serves the CLI approval page, and `/api/cli-login/*` drives the short-lived browser approval flow used by `forge login`.
The static control-plane assets under `web/` are served by Forge itself, so the same process remains authoritative for auth, session validation, and protected page delivery.
CLI commands and bearer-token API auth remain available for automation and operator usage. Web actions are not a separate deployment engine; they flow through the same API and queue.

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
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
```

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

- **Caddy server ID**: Ensure Caddy is configured with server ID `"forge"`.
- **Port Conflicts**: If port 8080 is taken, update `api_bind` in `forge.conf` and `FORGE_URL`.
- **API Visibility**: Keep the API bound to `localhost` (127.0.0.1) for security.
