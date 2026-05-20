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
forge init
forge deploy <project> <environment>
forge status <deployment_id>
forge events
forge rollback <project> <environment>
forge secrets set <project> <environment> <key> <value>
```

---

# Example Workflow

## 1. Initialize Project

```bash
forge init
```

This generates a deterministic `forge.yml` in the current directory. YAML is the primary user-facing configuration surface for Forge projects.

## 2. Push Code

```bash
git push origin main
```

## 3. GitHub Webhook Triggers Forge

Forge:

- verifies signature
- dedupes delivery
- fetches exact commit
- loads the committed webhook manifest
- maps branch → environment
- enqueues deployment

## 4. Forge Executes Deployment

```txt
build
→ start
→ validate
→ finalize
→ activate
→ promote
```

## 5. Runtime Convergence Maintains Correctness

If deployment degrades:

```txt
restart
→ rollback
→ cleanup
→ recover
```

---

# Example Manifest (forge.yml)

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

`forge deploy <project> <environment>` now loads `forge.yml` from the daemon working directory when that file is present. Internal runtime artifacts and deployment ordering remain unchanged.

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
