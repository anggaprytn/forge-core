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

# Requirements

Minimum runtime requirements:

- Linux host or compatible environment
- Docker daemon
- Caddy with admin API enabled
- Git installed
- Rust toolchain (for local development)
- GitHub webhook access (optional)

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

Current implementation note:

manual `forge deploy <project> <environment>` builds from the daemon process working directory. For a VPS quickstart, point the service `WorkingDirectory` at the application checkout you want Forge to deploy.

---

## Verify Readiness

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
forge doctor
```

Expected:

```txt
ok
ready
```

`/metrics` returns Prometheus text exposition for operational visibility.

---

# Project Structure

Typical repository:

```txt
my-app/
├── Dockerfile
├── forge.yml
├── forge.project.json
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

### Example forge.yml

```yaml
version: 1
name: api
type: web

build:
  dockerfile: Dockerfile
  context: .

runtime:
  port: 3000
  healthcheck:
    path: /health
    expected_status: 200

invariants:
  - name: health
    path: /health
    expect_status: 200
```

### Overwriting Configuration

If you need to reset your configuration, use the `--force` flag:

```bash
forge init --force
```

Current compatibility note:

- `forge deploy <project> <environment>` loads `forge.yml` from the daemon working directory when present.
- Internal runtime artifacts remain JSON.
- GitHub webhook flows continue to read the committed `forge.project.json`.

---

# Basic Deployment Flow

## 1. Initialize and Deploy

A minimal getting-started flow:

```bash
forge init
forge deploy api production
```

`forge deploy` uses the `name`, build paths, runtime port, and healthcheck from `forge.yml`.

---

# forge.project.json

Example:

```json
{
  "project_id": "api",
  "service_type": "http",
  "dockerfile": "Dockerfile",
  "internal_port": 3000,
  "healthcheck": {
    "path": "/health"
  },
  "environments": {
    "production": {
      "branch": "main",
      "subdomain": "api.example.com"
    }
  }
}
```

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
```

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

- forge.project.json
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
| deploy       | `forge deploy <project> <env>`   |
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
