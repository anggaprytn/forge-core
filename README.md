# Forge

Deterministic runtime convergence platform for AI-generated applications.

Forge is a single-node deployment and orchestration system designed around one core idea:

```txt
running container != successful deployment
```

A deployment is only considered successful after:

```txt
candidate
→ validated
→ finalized
→ activated
→ promoted
```

Forge separates deploy-time eligibility from steady-state correctness, allowing deterministic rollback, restart-safe recovery, and runtime convergence without Kubernetes-scale complexity.

---

# Why Forge Exists

AI-generated applications fail operationally in predictable ways:

- bind to `127.0.0.1`
- expose incorrect ports
- fail health assumptions
- leak secrets into logs
- partially deploy
- leave orphaned runtime state
- require manual infrastructure repair

Forge exists to make generated software operationally convergent.

Goal:

```txt
git push
→ deploy
→ validate
→ recover
→ rollback
```

with minimal human intervention.

---

# Getting Started

- Local alpha loop: [docs/LOCAL_QUICKSTART.md](docs/LOCAL_QUICKSTART.md)
- Linux/systemd host bootstrap: [install.sh](install.sh) (conservative and idempotent)

---

# Alpha Readiness Checklist

Forge has been manually validated on a real VPS with the following:

- [x] **Install**: `install.sh` successfully sets up binary, config, and systemd.
- [x] **Deploy**: `forge deploy` promotes a new generation successfully.
- [x] **Rollback**: `forge rollback` restores the previous healthy generation.
- [x] **Daemon Restart**: `systemctl restart forge` reconstructs state.
- [x] **Caddy Restart**: `systemctl restart caddy` results in route repair.
- [x] **Docker Restart**: `systemctl restart docker` results in container recovery.
- [x] **Host Reboot**: VPS reboot results in full automatic recovery.
- [x] **Route Recovery**: Routing remains stable across runtime churn.
- [x] **Bounded Retention**: Old generations are cleaned up deterministically.
- [x] **Orphan Cleanup**: Orphaned containers/routes are removed or tombstoned.
- [x] **12h Soak**: Daemon remains healthy under idle and active convergence.

---

# Known Constraints (Alpha)

- **Single-node only**: Forge manages exactly one host.
- **Single-service web apps**: Only one HTTP service per project.
- **No stateful DB ownership**: Database volume/state management is not native yet.
- **No multi-service orchestration**: No built-in cross-service dependency management.
- **Manual deploy source**: By default, `forge deploy` builds from the daemon's `WorkingDirectory`; prefer `forge deploy --from <path>` for explicit operator control.
- **API Visibility**: API is localhost-bound by default; do not expose publicly.

---

# Core Principles

## 1. Forge Is Authority

Docker executes.

Caddy routes.

Forge owns orchestration truth.

---

## 2. Deterministic Activation Ordering

Deployments follow strict ordering:

```txt
candidate generation
→ validated generation
→ finalized snapshot
→ route activation verified
→ current pointer update
```

Never the reverse.

---

## 3. Current Pointer Represents Intent

```txt
current
```

is the intended active generation.

Routes reconcile toward current.

Not vice versa.

---

## 4. Immutable Deployment Snapshots

Each deployment generation is immutable and persisted.

Snapshots are rollback artifacts, not runtime guesses.

---

## 5. Runtime Convergence

Forge continuously reconciles:

- snapshots
- routes
- containers
- runtime state
- queue state

toward correctness.

---

# Features

## Deployments

- GitHub webhook deploys
- Exact-commit manifest resolution
- Queue-backed deployment execution
- Deterministic generation allocation
- Restart-safe orchestration

## Runtime Validation

- TCP validation
- HTTP health validation
- Runtime contract enforcement
- Failed-generation isolation

## Recovery

- Automatic rollback
- Restart reconstruction
- Orphan cleanup
- Tombstoning
- Convergence repair loops

## Secrets

- API-managed secrets
- Runtime injection
- Redaction in events and diagnostics
- Secret-safe failure handling

## Observability

- Structured deployment events
- Persisted diagnostics
- Runtime state persistence
- Deployment/event API
- CLI tooling

---

# Architecture

```txt
GitHub Webhook
        ↓
HTTP API
        ↓
Persistent Queue
        ↓
Deployment Executor
        ↓
Docker Runtime
        ↓
Validation
        ↓
Snapshot Finalization
        ↓
Caddy Route Activation
        ↓
Current Pointer Promotion
        ↓
Steady-State Convergence
```

---

# Components

| Component          | Responsibility                   |
| ------------------ | -------------------------------- |
| Forge Core         | Orchestration authority          |
| Docker             | Container execution              |
| Caddy              | HTTP routing                     |
| Snapshot Store     | Immutable deployment state       |
| Queue              | Persistent deployment sequencing |
| Convergence Engine | Runtime repair/recovery          |
| CLI                | Operator interface               |

---

# Runtime Semantics

## Deploy-Time

Determines:

```txt
can this generation become active?
```

## Steady-State

Determines:

```txt
should this generation remain active?
```

These are intentionally separate systems.

---

# CLI

```bash
forge init                                   # Generate forge.yml
forge deploy [--from PATH] <project> <environment> # Deploy using forge.yml
forge status <deployment_id>                 # Check deployment status
forge events                                 # View orchestration events
forge rollback <project> <environment>       # Restore previous healthy generation
forge secrets set <project> <env> <k> <v>    # Set runtime secrets
```

## HTTP Surface

- `GET /` serves a tiny built-in landing page (`Forge Runtime`) so the root path does not 404.
- `GET /login/cli` serves a placeholder Forge CLI login bootstrap page.
- GitHub OAuth, callback handling, session storage, and Forge token issuance for CLI login are not implemented yet.

---

# Example Workflow

## 1. Initialize Project

```bash
forge init
```

This generates a deterministic `forge.yml` in the current directory. `forge.yml` is the primary operator-facing deployment configuration.

## 2. Deploy to Production

```bash
forge deploy api production
forge deploy api production --from /path/to/project
```

Forge reads `forge.yml` from the deploy source root and enqueues a deployment for the `api` project in the `production` environment.

By default, the deploy source is the daemon's `WorkingDirectory`. For predictable manual operations, prefer `forge deploy api production --from /path/to/project`.

## 3. GitHub Webhook (Automated)

For automated flows, pushing to a tracked branch triggers the same deterministic pipeline:

```bash
git push origin main
```

---

# Example Manifest (forge.yml)

Forge strictly validates `forge.yml`. Unsupported or unknown fields are rejected intentionally.

```yaml
version: 1
name: api
type: web # Only single-service web apps supported currently

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

Validated fields:
- `version`: Manifest schema version.
- `name`: Project identifier.
- `type`: Service type (currently `web`).
- `build.dockerfile`: Path to the Dockerfile.
- `build.context`: Docker build context path.
- `runtime.port`: The port the application binds to (0.0.0.0).
- `runtime.healthcheck.path`: Endpoint for HTTP health validation.
- `runtime.healthcheck.expected_status`: Expected HTTP status code.
- `invariants`: Post-activation runtime assertions.

---

# Development

## Run Tests

```bash
cargo test -q
```

## Run Integration Tests

```bash
FORGE_INTEGRATION=1 cargo test -- --nocapture
```

## Run Dogfood E2Es

```bash
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

---

# Status

Current maturity:

```txt
alpha
```

Forge currently proves:

- deterministic deployment ordering
- rollback semantics
- restart-safe recovery
- Docker/Caddy orchestration
- secret redaction
- runtime convergence
- AI-generated app deployment proofs

---

# Non-Goals

Forge intentionally avoids:

- Kubernetes complexity
- distributed scheduling
- multi-region orchestration
- premature multi-service abstractions
- enterprise RBAC sprawl

Forge optimizes for:

```txt
single-node operational correctness
```

first.

---

# Vision

Forge is designed for a future where software generation becomes cheap but operational correctness remains difficult.

The long-term thesis:

```txt
AI-generated applications should converge toward operational correctness automatically.
```

Not just deploy.

Converge.
