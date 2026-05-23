# Forge Usage Guide

This document explains how to use Forge as an operator and developer.

Forge is designed for deterministic deployment and runtime convergence of AI-generated applications.

---

# Core Deployment Model

Forge does not treat deployment as:

```txt
start container → done
```

Forge deploys in strict order:

```txt
candidate
→ validated
→ finalized
→ activated
→ promoted
```

A deployment becomes successful only after runtime validation and route activation succeed.

---

# Alpha Product Semantics

## Binary Model

- `forge` is the operator/client CLI.
- `forged` is the planned server/runtime authority binary name.
- The current implementation may still use one binary temporarily.
- This is product direction for the next alpha phase, not an immediate migration requirement.

## Control-Plane Model

- Forge server owns deployment queueing, source resolution, snapshots, convergence, routes, rollback, and restart recovery.
- CLI is a stateless operator/client surface.
- Web is a visibility/control surface for humans.
- API is the automation surface.
- CLI, API, and web operations must all converge into the same deployment queue and deployment pipeline.

## Source Model

- Canonical long-term deploy source is `git repository + ref`.
- Local `--from <path>` remains supported as alpha/dev mode.
- Upload sources are not canonical product semantics.
- Source acquisition resolves into a local immutable source checkout.
- The deployment pipeline consumes that resolved source path.
- Forge does not maintain a separate Git deployment FSM.

## Source Revision Identity Chain

```txt
repository
→ ref
→ commit_sha
→ source_checkout
→ generation
→ image_ref
→ snapshot
→ route activation
```

## Environments And Domains

- Alpha supports only `development`, `staging`, and `production`.
- Default branch mapping is `development -> development`, `staging -> staging`, `production -> main`.
- Planned alpha domain derivation is:
  `production -> <base_domain>`, `staging -> staging-<base_domain>`, `development -> development-<base_domain>`.
- Custom environments and custom per-environment domains are deferred.

## Project Registry

Forge stores a server-owned project registry under the existing storage root. This slice records repository metadata and a stable `base_domain` only.

Commands:

```bash
forge project add --repo https://github.com/example/api.git
forge project add api --repo https://github.com/example/api.git
forge project add api --repo https://github.com/example/api.git --branch development
forge project add api --repo https://github.com/example/api.git --domain api.example.com
forge project list
forge project show api
```

Rules:

- `--branch` defaults to `main`.
- `project_id` is optional when `--repo` is provided. Forge infers it from the repository basename, normalizes it, and validates the resulting ID using the existing safe project ID rules.
- If `--domain` is provided, Forge stores it as an explicit `base_domain`.
- If `--domain` is omitted on first creation, Forge first tries `<project_id>.<FORGE_APPS_DOMAIN>`.
- If that clean domain is already used by another project, Forge falls back to `<project_id>-<shortid>.<FORGE_APPS_DOMAIN>`.
- Generated domains stay stable after the first creation and are not regenerated when `repo_url` or `default_branch` changes.
- `FORGE_APPS_DOMAIN` is required only when Forge must generate a domain.
- Repository URLs with embedded HTTP credentials such as `https://token@github.com/...` are rejected.

Planned derived domains are documented product semantics only in this slice:

- `production -> <base_domain>`
- `staging -> staging-<base_domain>`
- `development -> development-<base_domain>`

Deploy-by-ref uses this registry metadata now. Forge clones or fetches repositories into `<storage_root>/repositories/<project_id>/`, resolves the requested ref or default branch to an immutable commit SHA, and reuses immutable checkouts from `<storage_root>/source-checkouts/<project_id>/<commit_sha>/`.

## Web Role

- Web is first a visibility/control plane, not the primary deployment engine.
- Initial web scope is login, projects, environments, current/previous generation visibility, and events/logs/diagnostics.
- Deployment execution still goes through the same API, queue, and FSM.

---

# Operator Workflow Examples

### Basic Deployment
Deploy a project to the production environment.
```bash
forge deploy my-app production
```

### Inspect Runtime Status
Check the health and active generation of an environment.
```bash
forge status my-app production
```

`forge status` is the lightweight operational summary surface. It includes effective runtime policy and, when available, captured runtime usage snapshots for the active service set.

### Diagnose Failures
View detailed diagnostic information for a specific deployment or environment.
```bash
forge diagnose my-app production
```

`forge diagnose` is the deep diagnostics surface. It reports runtime truth, termination details such as restart count, exit code, signal, OOM state, termination reason, and unresolved runtime-policy or volume-repair events when the environment is degraded.

After a backup restore, `forge diagnose` reports active lineage similar to:

```txt
Active Restore: backup=backup-1 source_generation=3 hook_succeeded=true
restored_volume=forge-my-app-production-restore-gen-4-vol-redis
Backup Restore Events:
- restored backup backup-1 into gen-4
```

### View Deployment History
See a history of recent deployments for a project.
```bash
forge history my-app production
```

### List All Deployments
List recent deployment attempts across the system.
```bash
forge deployments
```

### Manage Secrets
List and modify runtime environment secrets.
```bash
forge secrets list my-app production
forge secrets set my-app production API_KEY sk_live_...
forge secrets unset my-app production OBSOLETE_KEY
```

### Environment Diff
Compare the environment variables between the current and a candidate generation.
```bash
forge env diff my-app production
```

### Atomic Rollback
Restore the previous healthy generation immediately.
```bash
forge rollback my-app production
```

### Backup Lifecycle
Create, inspect, and restore a persistent-volume backup.
```bash
forge backup create my-app production
forge backup list my-app production
forge backup inspect backup-1
forge backup restore backup-1
```

### Garbage Collection Dry-Run
Preview which generations would be removed by the GC.
```bash
forge gc --dry-run
```

---

# Requirements

Minimum runtime requirements:

- Linux host or compatible environment
- Docker daemon
- Caddy with admin API enabled
- Git installed
- Rust toolchain (for local development)
- GitHub webhook access (optional)

---

# Installation

## Conservative Installer (Linux)

For Linux hosts with systemd, use the provided conservative installer:

```bash
./install.sh
```

The installer is **idempotent** and performs the following:
- Installs the `forge` binary to `/usr/local/bin`.
- Creates `/etc/forge/forge.conf` and `/etc/forge/forge.env` if they do not exist.
- Prepares the storage root at `/var/lib/forge`.
- Installs the systemd unit `forge.service` (does not enable or start it automatically).

**What the installer does NOT do:**
- It does **not** install Docker or Caddy.
- It does **not** modify your firewall or Nginx configuration.
- It does **not** expose the Forge API publicly (remains localhost-bound by default).

---

# Manifest Examples

## Multi-Service App With Internal Redis

```yaml
version: 1
name: my-app
type: web

build:
  dockerfile: Dockerfile
  context: .

services:
  redis:
    runtime:
      image: redis:7
    state:
      volume: redis-data
      mount_path: /data
      retention: persistent
      pre_backup_command: redis-cli SAVE
    expose: false
  api:
    build:
      dockerfile: Dockerfile
      context: .
    runtime:
      port: 3000
      cpu:
        limit: "1.5"
      memory:
        limit_mb: 512
      restart:
        policy: on-failure
        max_retries: 5
      healthcheck:
        path: /health
        expected_status: 200
      depends_on:
        - redis
    expose: true
```

Runtime policy fields under `runtime` are persisted per service. Forge uses them as rollback truth and convergence repair targets:

- `runtime.cpu.limit`
- `runtime.memory.limit_mb`
- `runtime.restart.policy`
- `runtime.restart.max_retries`

Promotion is blocked if warmup detects OOM kills, crash loops, restart storms, or unstable required dependencies.

Notes:

- `redis` is an internal service and is not publicly routed.
- `api` can reach Redis over the Forge-managed internal service alias.
- `retention: persistent` keeps the Redis volume across deploys, rollback, and GC.
- `pre_backup_command: redis-cli SAVE` flushes Redis state before backup archive creation.

## Ephemeral Volume Variant

```yaml
services:
  redis:
    runtime:
      image: redis:7
    state:
      volume: redis-cache
      mount_path: /data
      retention: ephemeral
    expose: false
```

Use `ephemeral` only when the service state is generation-scoped and safe to discard after the generation is no longer rollback-safe.

---

# Starting Forge

## Start Daemon

```bash
forge daemon
```

Daemon startup reads:

- `FORGE_CONFIG` or `./forge.conf`
- `FORGE_CADDY_ADMIN_URL` or `http://127.0.0.1:2019`
- `FORGE_CADDY_PUBLIC_URL` or `http://127.0.0.1`

Optional flags:

```bash
forge --config /etc/forge/forge.conf \
  --caddy-admin-url http://127.0.0.1:2019 \
  --caddy-public-url https://app.example.com \
  daemon
```

Use `FORGE_CADDY_PUBLIC_URL` or `--caddy-public-url` when route activation must be verified through a non-localhost public address.

Or via systemd:

```bash
systemctl start forge
```

### Permissions and Paths

- **Storage Root**: The service user must own `/var/lib/forge` (or your configured `storage_root`).
- **WorkingDirectory**: The service user must have read and execute (traversal) permissions for the `WorkingDirectory` defined in the systemd unit.
- **Deploy Source Resolution**: `forge deploy <project> <environment>` resolves the registered project's `default_branch` by default. Use `forge deploy <project> <environment> --ref <ref>` to override the ref, or `forge deploy --from <path> <project> <environment>` for an explicit local source checkout.

---

## Verify Readiness

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
curl http://localhost:8080/
curl -X POST http://localhost:8080/api/cli-login/start
forge doctor
```

Expected:

```txt
ok
ready
```

`/metrics` returns Prometheus text exposition for operational visibility.
`/` returns the embedded `web/index.html` landing page.
The alpha web UI is intentionally framework-free: plain HTML, CSS, and vanilla JS served directly by Forge with no frontend build step.
CLI login uses a short-lived browser approval flow:

```bash
forge login https://forge.example.com
```

Forge creates a pending CLI login, opens or prints `/login/cli?code=...`, reuses the existing GitHub web session for approval, and stores the resulting token in `~/.config/forge/config.toml`. `FORGE_URL` and `FORGE_TOKEN` override the saved config when present.

Endpoint semantics:

- `/healthz`: process liveness only
- `/readyz`: control-plane readiness only
- `forge status`: lightweight runtime and environment summary
- `forge diagnose`: deep diagnostics for operators and debugging

`/readyz` is cache-backed. The convergence loop computes readiness asynchronously, and the request path serves cached truth in bounded time. It must not perform synchronous Docker scans, Caddy scans, route reconciliation, generation reconciliation, or environment-wide diagnostics.

If the readiness cache is stale, Forge should fail fast with degraded status:

```json
{
  "status": "degraded",
  "reason": "readiness cache stale"
}
```

---

# Project Structure

Typical repository:

```txt
my-app/
├── Dockerfile
├── forge.yml
└── src/
```

## Bootstrap A Config

The fastest way to get started with Forge is to initialize a project configuration in your current directory:

```bash
forge init
```

This generates a deterministic `forge.yml` file. This command:
- does not require `FORGE_URL` or `FORGE_TOKEN`
- is intentionally minimal to reduce onboarding friction
- refuses to overwrite an existing `forge.yml` unless you pass `--force`

### Validated forge.yml Fields

Forge strictly validates `forge.yml`. Unsupported or unknown fields are rejected intentionally.

```yaml
version: 1
name: api
type: web

build:
  dockerfile: Dockerfile
  context: .

services:
  api:
    runtime:
      port: 3000
      healthcheck:
        path: /health
        expected_status: 200
    expose: true
```

| Field | Purpose |
|-------|---------|
| `version` | Manifest schema version. |
| `name` | Project identifier used in CLI and routing. |
| `type` | Project type (currently only `web` is supported). |
| `build.dockerfile` | Path to the Dockerfile (relative to context). |
| `build.context` | Docker build context path (relative to `forge.yml`). |
| `services` | Service map for exposed and internal services. |
| `services.<id>.runtime.port` | Internal port the service binds to. |
| `services.<id>.runtime.healthcheck.path` | HTTP path for health validation. |
| `services.<id>.runtime.healthcheck.expected_status` | Expected success status for health check. |

### Overwriting Configuration

If you need to reset your configuration, use the `--force` flag:

```bash
forge init --force
```

---

# Basic Deployment Flow

## 1. Initialize and Deploy

A minimal getting-started flow:

```bash
forge init
forge deploy api production
forge deploy api production --ref main
forge deploy api production --ref release-2026-05
forge deploy api production --from /path/to/project
```

Deploy-by-ref fetches the registered repository into the server-side cache, resolves an immutable commit SHA, and then uses the checkout's `forge.yml` for the deployment. `--from` bypasses Git resolution and deploys directly from the provided local path.

---

# Basic Deployment Flow

## 1. Push Code

```bash
git push origin main
```

---

## 2. GitHub Webhook Triggers Forge

Forge will:

- verify webhook signature
- dedupe delivery
- fetch exact commit
- load manifest from exact commit
- map branch → environment
- enqueue deployment

Default branch mapping in alpha:

- `development -> development`
- `staging -> staging`
- `main -> production`

---

## 3. Deployment Executes

Forge performs:

```txt
build
→ create candidate generation
→ start container
→ validate runtime
→ finalize snapshot
→ activate route
→ promote current
```

---

# Manual Deployment

## Deploy via CLI

```bash
forge deploy api production
forge deploy api production --ref main
```

## CLI Login

```bash
forge login https://forge.example.com
forge whoami
forge logout
```

`forge whoami` reports the resolved server URL and whether the current credentials appear authenticated. `forge logout` removes the saved local token without removing `FORGE_URL` or `FORGE_TOKEN` from the environment.

---

# Alpha Core Loop v2 Validated (May 2026)

The Forge Alpha Core Loop v2 milestone formalizes the second validated operational maturity milestone for the Forge platform.

### Validated Capabilities

- **Progressive Deployment Lifecycle**: Deterministic state transitions from `queued` through `promoted`.
- **Lifecycle Persistence**: Full per-generation lifecycle state tracking and recovery.
- **Retention & GC**: Rollback-safe generation preservation with automatic cleanup of expired artifacts.
- **Immutable Env Snapshots**: Fully resolved and sealed runtime environment snapshots per generation.
- **Diagnostics & Logs**: Bounded, secret-redacted deployment logs and deep-inspection diagnostics.
- **Secret Lifecycle**: Immutable secret snapshots with historical restoration during rollback.
- **Probe Stability Semantics**: Hysteresis-aware health probing with flapping detection and stability windows.
- **Convergence & Runtime Truth**: Continuous repair of routing and container state toward the promoted truth.

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

### Known Alpha Constraints
- Single-node deployments only.
- Single-service web apps (one service per project).
- No preview environments or automated DNS.
- No web-based deployment trigger (API/CLI only).
- No RBAC or team management.

---

## Deploy via API

```bash
curl -X POST http://localhost:8080/deployments \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "api",
    "environment": "production"
  }'
```

---

# Deployment Status

## CLI

```bash
forge status <deployment_id>
```

---

## API

```bash
curl http://localhost:8080/deployments/<deployment_id>
```

---

# Deployment Logs

Forge persists a bounded, redacted diagnostic log excerpt for each deployment.

## API

```bash
curl http://localhost:8080/logs/<deployment_id>
```

The response contains recent log lines only.

Rules:

- secret values are redacted before persistence and delivery
- logs are retained as bounded diagnostic excerpts, not as unbounded streams
- Forge does not expose `docker logs -f`, SSE, or websocket log tails

---

# Events

Forge emits append-only deployment events.

## CLI

```bash
forge events
```

Cleanup outcomes are included in the same stream. To surface orphan and tombstone activity:

```bash
forge events | rg 'ORPHANED_|CLEANUP_'
```

Expected cleanup event types include `ORPHANED_CONTAINER_REMOVED`, `ORPHANED_CONTAINER_TOMBSTONED`, `ORPHANED_ROUTE_REMOVED`, `ORPHANED_ROUTE_TOMBSTONED`, `CLEANUP_RETRY_SUCCEEDED`, and `CLEANUP_RETRY_TOMBSTONED`.

---

## API

```bash
curl http://localhost:8080/events
```

---

# Metrics

Forge exposes a minimal Prometheus-compatible metrics endpoint:

```bash
curl http://localhost:8080/metrics
```

Current metrics:

- `forge_deployments_total`
- `forge_deployments_failed_total`
- `forge_deployments_rollback_total`
- `forge_queue_depth`

`forge_queue_depth` reports the current number of queued deployments waiting to run.

---

# Doctor

Forge includes a read-only local diagnostics command:

```bash
forge doctor
```

Doctor reads:

- `FORGE_CONFIG` or `./forge.conf`
- `FORGE_CADDY_ADMIN_URL` or `http://127.0.0.1:2019`

Doctor checks:

- Docker reachable
- Caddy admin API reachable
- storage root permission state
- `FORGE_MASTER_KEY` presence/format
- queue root exists
- snapshot root exists
- API token configured
- metrics endpoint reachable

Example output:

```txt
[OK] Docker reachable
[OK] Caddy admin API reachable
[WARN] FORGE_MASTER_KEY missing
```

For systemd installs, run doctor with the same config path and Caddy admin URL used by the service:

```bash
FORGE_CONFIG=/etc/forge/forge.conf \
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019 \
FORGE_MASTER_KEY=<64 hex characters> \
forge doctor
```

---

# Rollback

Rollback restores the previous healthy finalized generation.

Hard boundary:

- rollback restores topology, not database history
- restore is the stateful recovery workflow when persistent data must be replayed

## CLI

```bash
forge rollback api production
```

---

## API

```bash
curl -X POST http://localhost:8080/deployments \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "api",
    "environment": "production",
    "intent": "rollback"
  }'
```

---

# Secrets

Secrets are API-managed only.

Never place secret values in:

- forge.yml
- git
- logs
- diagnostics

---

## Set Secret

```bash
forge secrets set api production DATABASE_URL postgres://...
```

---

## Secret Injection

Secrets are injected at runtime into the deployment container.

Secrets are automatically redacted from:

- events
- diagnostics
- logs
- API output

---

# Runtime Validation

Forge validates deployments before promotion.

## TCP Validation

Verifies:

```txt
container reachable on declared port
```

---

## HTTP Validation

Verifies:

```txt
application health endpoint returns success
```

Example:

```json
{
  "healthcheck": {
    "path": "/health"
  }
}
```

---

# Runtime Contracts

Forge prevents common AI-generated infrastructure mistakes.

Examples:

- binding to `127.0.0.1`
- wrong port assumptions
- unhealthy startup behavior
- invalid health responses

Bad generations are blocked before promotion.

---

# Current Runtime State

Forge tracks:

- current generation
- previous generation
- runtime state
- convergence health
- deployment history

Important:

```txt
current = intended active generation
```

Routes reconcile toward current.

---

# Runtime Recovery

Forge automatically handles:

- restart reconstruction
- degraded generations
- rollback
- orphan cleanup
- crash recovery

---

# Steady-State Convergence

Forge continuously evaluates active generations.

Lifecycle:

```txt
healthy
→ degraded
→ restart_attempt
→ rollback
→ unavailable
```

---

# Example Failure Flow

```txt
deploy candidate
→ HTTP probe fails
→ generation rejected
→ current unchanged
→ diagnostics preserved
→ failed generation cleaned
```

---

# Docker Usage

Forge manages Docker containers internally.

Forge-managed containers include labels:

```txt
forge.managed=true
forge.project_id=<project>
forge.environment=<environment>
forge.generation=<generation>
```

---

# Caddy Usage

Forge manages only its own Caddy subtree.

Forge route IDs:

```txt
forge:{project_id}:{environment}
```

Forge does not manage unrelated Caddy routes.

---

# Local Development

## Run Unit Tests

```bash
cargo test -q
```

---

## Run Integration Tests

```bash
FORGE_INTEGRATION=1 cargo test -- --nocapture
```

---

## Run Dogfood E2Es

```bash
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

---

# Example Dogfood Flow

Forge has validated:

- AI-generated app deploys
- invalid infra assumptions blocked
- rollback correctness
- secret redaction under failure

Goal:

```txt
AI-generated apps deploy successfully without manual infrastructure repair
```

---

# Common Commands

| Action       | Command                          |
| ------------ | -------------------------------- |
| start daemon | `forge daemon`                   |
| deploy       | `forge deploy [--from PATH] [--ref REF] <project> <env>` |
| rollback     | `forge rollback <project> <env>` |
| status       | `forge status <deployment_id>`   |
| events       | `forge events`                   |
| set secret   | `forge secrets set ...`          |

---

# Common Failure Cases

## HTTP Health Failure

Symptoms:

- deployment rejected
- no route promotion

Fix:

- verify app binds `0.0.0.0`
- verify `/health`
- verify internal port

---

## Missing Secret

Symptoms:

- deployment fails before container start

Fix:

```bash
forge secrets set ...
```

---

## Route Activation Failure

Symptoms:

- snapshot finalized
- current unchanged
- route verification failed

Fix:

- inspect Caddy admin API
- verify Docker network
- verify container reachable

---

# Operational Rules

Never:

- manually edit finalized snapshots
- manually advance current pointer
- enable Docker restart policies
- store secret values in manifests

Always:

- let convergence repair runtime state
- preserve diagnostics
- preserve snapshot immutability

---

# Important Invariant

Never forget:

```txt
running container != successful deployment
```

Successful deployment requires:

```txt
validated runtime
+ finalized snapshot
+ verified route activation
+ promoted current generation
```

That distinction is the foundation of Forge.
