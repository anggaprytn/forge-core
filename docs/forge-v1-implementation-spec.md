# Forge v1 Implementation Specification

This document records the implementation-oriented v1 model. For the next alpha phase, product semantics are locked as follows:

- `forge` is the client/operator CLI
- `forged` is the future server/runtime authority binary name
- current implementation may temporarily continue to use one binary
- the server owns queueing, source resolution, snapshots, convergence, routes, rollback, and recovery
- CLI, API, webhook, and web must converge into the same deployment pipeline
- canonical deployment source is `repository + ref`, resolved into an immutable local checkout
- `forge.yml` is the alpha manifest contract
- supported alpha environments are `development`, `staging`, and `production`

## 1. Scope

Forge v1 is a single-node, self-hosted runtime convergence daemon for deploying one containerized project, potentially composed of multiple services, onto one VPS.

Included in v1:

- Rust daemon
- GitHub webhook deploys
- manual API deploys
- local `--from` deploys for alpha/dev mode
- single-service and multi-service deployments on one managed Docker network
- Runtime Contracts v1
- Caddy HTTP routing
- blue-green deployments
- immutable deployment snapshots
- rollback
- backup and restore of persistent Docker volumes
- environment isolation
- encrypted secret management
- single global serialized deployment queue
- TCP and optional HTTP health verification
- startup convergence
- bounded log streaming
- append-only event stream
- restore lineage tracking

Excluded from v1:

- preview environments
- custom environments
- custom per-environment domains
- distributed workers
- Redis/distributed queue
- standalone worker workloads
- UDP workloads
- autoscaling
- RBAC
- teams
- DNS automation
- multi-tenant isolation
- AI auto-remediation

## 2. Core Invariants

Forge v1 MUST enforce these invariants:

- Docker is the execution engine only.
- Forge is the orchestration authority.
- The deployment server is the control-plane authority.
- Docker restart policy, CPU limit, and memory limit are persisted per service and must round-trip through deployment, rollback, and convergence.
- Desired state is defined by the resolved `forge.yml` plus deploy request intent.
- Observed state is defined by Docker/Linux inspection.
- Snapshot state is the immutable rollback artifact.
- Reconstructed state is derived from snapshots, labels, pointers, and runtime inspection.
- SQLite is never authoritative.
- Secrets are never persisted plaintext.
- `current` expresses intended active generation after route activation succeeds.
- `previous` always points to the most recent superseded healthy generation.
- Failed generations never become active pointers.
- Generation numbers are monotonically increasing per `(project_id, environment)` and never reused.
- Only one deployment may execute at a time across the daemon.
- CLI, API, webhook, and web requests must feed the same deployment queue and state machine.

## 3. Environment Model

Fixed environments in alpha:

- `development`
- `staging`
- `production`

Rules:

- environment names are fixed enums
- each environment has an independent generation history
- default branch mappings are:
  - `development -> development`
  - `staging -> staging`
  - `production -> main`
- routing, secrets, snapshots, and rollback history are isolated per environment
- custom environments are not supported in alpha

Planned alpha domain derivation:

- `production -> <base_domain>`
- `staging -> staging-<base_domain>`
- `development -> development-<base_domain>`

## 4. Manifest Model

`forge.yml` is the alpha manifest contract. It is source-rooted, versioned with application code, and resolved from the exact deployment source checkout.

Current implementation note:

manual local `--from` deploys remain valid alpha/dev mode, but the long-term canonical source is `repository + ref`.

### 4.1 Manifest Purpose

The manifest defines stable project configuration:

- project identity
- build defaults
- runtime defaults
- route defaults
- resource defaults
- contract defaults
- secret references
- health defaults

### 4.2 Manifest Schema v1

The current alpha surface is the narrower `forge.yml` contract already used by the runtime and CLI. It remains intentionally small while product semantics are being locked.

### 4.3 Manifest Validation Rules

- manifest version MUST match the supported alpha schema
- project identity MUST be stable after first successful deployment
- runtime port MUST be a valid TCP port
- current alpha project type is `web`, supporting one or more services in a single-node topology
- secret values MUST NOT appear in manifest
- manifest validation remains intentionally narrow and deterministic

## 5. Deploy Request Model

`POST /deployments` expresses ephemeral execution intent.

### 5.1 Allowed Fields

```json
{
  "project_id": "api",
  "environment": "production",
  "commit_sha": "abc123",
  "image_digest": null,
  "intent": "deploy",
  "runtime_override": {},
  "metadata": {
    "reason": "manual redeploy"
  }
}
```

Headers:

- `Authorization: Bearer <token>`
- `Idempotency-Key: <opaque-key>`

Product model note:

the API is the automation surface only. It does not bypass queueing, source resolution, or deployment ordering.

### 5.2 Request Rules

- exactly one of `commit_sha` or `image_digest` may be provided
- if both are provided, reject request
- `project_id` is required
- `environment` is required
- `intent` allowed values:
  - `deploy`
  - `redeploy`
  - `rollback`
- `runtime_override` is optional and narrowly scoped
- runtime override may adjust approved deploy-time values only
- manifest remains the stable project definition

### 5.3 Idempotency

- manual deploys support `Idempotency-Key`
- repeated request with same key and same normalized payload returns same deployment record
- changed payload with reused key is rejected

## 6. Desired State Model

Desired state is materialized at deployment start as a normalized object.

```json
{
  "project_id": "api",
  "environment": "production",
  "source": {
    "repository": "https://github.com/org/api.git",
    "ref": "main",
    "commit_sha": "abc123",
    "source_checkout": "/var/lib/forge/.../source-checkouts/abc123"
  },
  "build": {
    "dockerfile_path": "./Dockerfile",
    "context_path": "."
  },
  "runtime": {
    "service_type": "http",
    "internal_port": 3000,
    "subdomain": "api",
    "resources": {
      "memory_limit_mb": 512,
      "cpu_shares": 1024
    }
  },
  "contract": {
    "version": 1,
    "spec": {}
  },
  "health": {
    "tcp_required": true,
    "http_enabled": true
  }
}
```

Precedence:

- deploy override
- environment override
- project manifest defaults
- Forge inferred defaults
- Forge non-overridable minimums

Source revision identity chain:

```txt
repository
-> ref
-> commit_sha
-> source_checkout
-> generation
-> image_ref
-> snapshot
-> route activation
```

Forge minimums include:

- bind to `0.0.0.0`
- TCP health validation
- Forge-managed labels
- deterministic naming

## 7. Runtime Contract v1

Runtime Contracts are validated at deploy time and continuously in steady state.

### 7.1 v1 Capability Surface

- network bind validation
- internal port validation
- environment variable presence
- resource limit declaration
- graceful shutdown timeout
- HTTP health specification
- required runtime invariants

### 7.2 Non-overridable Forge Minimums

- bind must be externally reachable inside the container namespace
- container must expose required TCP service
- container labels must be present
- Docker restart behavior may be configured per service, but Forge must treat restart storms, OOM kills, and unstable warmup behavior as promotion blockers.

Single-node isolation assumptions:
- Forge relies on Docker cgroup and namespace controls on a single host.
- These controls provide operational containment only. They do not turn Forge into a hardened hostile-tenant platform.
- Resource exhaustion must be reported in runtime metadata (`runtime.json`, `lifecycle.json`, diagnostics) and must block promotion when observed during warmup.

## 8. Filesystem Layout

Canonical root:

```txt
/var/lib/forge/
  projects/
  secrets/
  events/
  indexes/
```

Per project:

```txt
/var/lib/forge/projects/{project_id}/
  project.meta.json
  environments/
    development/
    staging/
    production/
```

Per environment:

```txt
/var/lib/forge/projects/{project_id}/environments/{environment}/
  generation.counter
  current
  previous
  generations/
```

Per generation:

```txt
/var/lib/forge/projects/{project_id}/environments/{environment}/generations/{generation}/
  snapshot.json
  desired_state.json
  runtime_contract.json
  route.json
  build.json
  events.jsonl
  diagnostics/
```

## 9. Generation Allocation

Canonical source:

```txt
/var/lib/forge/projects/{project_id}/environments/{environment}/generation.counter
```

Rules:

- one counter per `(project_id, environment)`
- allocate when deployment begins execution
- file-lock before read-modify-write
- increment atomically
- fsync before release
- never decrement
- never reuse
- gaps allowed after crash/cancel

## 10. Snapshot Model

A generation snapshot is one immutable directory per generation.

### 10.1 Required Files

- `snapshot.json`: top-level immutable summary
- `desired_state.json`: normalized desired state
- `runtime_contract.json`: effective resolved contract
- `route.json`: route target and activation metadata
- `build.json`: source or digest build metadata
- `events.jsonl`: generation-local append-only events
- `diagnostics/`: health failures, logs, probe output

### 10.2 Snapshot Finalization

A generation snapshot becomes immutable after finalize.

Mutable environment pointers live outside the snapshot:

- `current`
- `previous`

Pointer updates MUST use temp write + fsync + atomic rename.

### 10.3 Snapshot Durability

Snapshot persistence is the strict durability boundary.

Requirements:

- temp file write
- fsync file
- fsync directory
- atomic rename
- optional advisory locking
- partial snapshot write is invalid state

## 11. Event Model

Events are append-only and durable, but not the strict sync point for every transition.

### 11.1 Event Envelope

```json
{
  "event_id": "uuid",
  "timestamp": "2026-05-18T12:00:00Z",
  "project_id": "api",
  "environment": "production",
  "generation": 42,
  "deployment_id": "uuid",
  "correlation_id": "uuid",
  "type": "CONTAINER_STARTED",
  "severity": "info",
  "payload": {},
  "redacted": false
}
```

### 11.2 Event Rules

- immutable
- append-only
- JSONL storage
- globally queryable through SQLite index
- replayable for diagnostics only
- snapshots remain rollback authority

### 11.3 Core Event Types

- `DEPLOYMENT_QUEUED`
- `DEPLOYMENT_STARTED`
- `SOURCE_FETCH_STARTED`
- `IMAGE_BUILD_STARTED`
- `IMAGE_BUILD_COMPLETED`
- `CONTAINER_CREATED`
- `CONTAINER_STARTED`
- `TCP_PROBE_FAILED`
- `HTTP_PROBE_FAILED`
- `CONTRACT_VALIDATION_FAILED`
- `ROUTE_UPDATED`
- `ROUTE_ACTIVATION_CONFIRMED`
- `DEPLOYMENT_CONVERGED`
- `DEPLOYMENT_DEGRADED`
- `ROLLBACK_STARTED`
- `ROLLBACK_COMPLETED`
- `DEPLOYMENT_FAILED`
- `CLEANUP_COMPLETED`

## 12. Deployment State Machine

### 12.1 States

- `queued`
- `preparing`
- `building`
- `starting`
- `validating`
- `routing`
- `healthy`
- `degraded`
- `rollback`
- `failed`
- `stopped`

### 12.2 Legal Transitions

```txt
queued -> preparing
preparing -> building
preparing -> starting
building -> starting
building -> failed
starting -> validating
starting -> failed
validating -> routing
validating -> failed
routing -> healthy
routing -> rollback
routing -> failed
healthy -> degraded
healthy -> stopped
degraded -> healthy
degraded -> rollback
degraded -> failed
rollback -> healthy
rollback -> failed
```

### 12.3 Deployment Success Criteria

A deployment is healthy only after:

- container is running
- TCP probe passes
- HTTP probe passes if enabled
- runtime contract passes
- Caddy route is updated if HTTP service
- route activation probe passes if HTTP service
- snapshot finalize succeeds
- `current` pointer update succeeds

## 13. Recovery State Machine

### 13.1 States

- `healthy`
- `degraded`
- `retrying`
- `rollback_candidate`
- `rollback`
- `restored`
- `unavailable`

### 13.2 Steady-State Policy

- probe every 15s
- degraded after 3 consecutive failures
- recovery after 2 consecutive successes
- max 3 state transitions per 5 minutes

### 13.3 Recovery Rules

For degraded active generation:

```txt
3 failed steady-state probes
-> mark degraded
-> restart same generation once
-> probe for 30s
-> if still unhealthy and previous healthy generation exists, rollback route
-> if no previous healthy generation exists, keep route attached if TCP reachable
```

Special cases:

- if HTTP unhealthy but TCP reachable: keep routed, mark degraded
- if container not running after restart: remove route, mark unavailable
- if TCP unreachable after restart: remove route, mark unavailable

## 14. Health Model

### 14.1 Deploy-Time TCP

- retries: 10
- interval: 1s
- timeout: 1s

### 14.2 Deploy-Time HTTP

- optional
- retries: 10
- interval: 2s
- timeout: 5s

### 14.3 Startup Grace

- default: 30s
- configurable

### 14.4 Probe Order for HTTP Services

```txt
container direct TCP probe
-> container direct HTTP probe
-> route activation
-> Caddy-level probe
```

### 14.5 Supported Workload Types

v1 supports only network-addressable TCP services:

- HTTP services
- raw TCP services

Not supported in v1:

- UDP
- non-network workers
- multi-port services
- distributed storage
- database-native snapshots

Single-node Docker volume workloads are supported with two roles:

- `persistent`: survives deploy, rollback, and convergence repair
- `ephemeral`: scoped to a generation and removed by GC after the generation stops being rollback-safe

Rollback restores runtime topology and volume attachment intent only. Forge does not snapshot database contents or rewind persistent data.

Backup and restore semantics:

- backups archive persistent Docker volumes only
- helper containers perform archive and restore operations
- `pre_backup_command` hooks can flush service state before archive
- backups are crash-consistent by default
- restore creates a new generation with newly managed restored volumes
- restore lineage is recorded in generation diagnostics and backup metadata
- no PITR, distributed storage, or automatic quiescing

## 15. Routing Model

### 15.1 HTTP Services

HTTP services route through Caddy to a generation-specific container target on the Forge Docker network.

Example:

```txt
prod-api-gen-42:3000
```

### 15.2 TCP Services

Raw TCP services use deterministic host port allocation.

### 15.3 Caddy Ownership Boundary

Forge manages only a dedicated Forge-owned route subtree.

Forge-owned subtree ID format:

```txt
forge:{project_id}:{environment}
```

Forge never owns or rewrites the full Caddy configuration.

### 15.4 Route Activation Verification

For HTTP services:

- update Forge-owned Caddy subtree
- confirm API success
- probe via Caddy
- only then mark route active

## 16. Port Allocation

Host ports are required only for non-HTTP TCP services.

### 16.1 Port Identity

Unique by:

```txt
(host_port, protocol)
```

In v1 protocol is TCP only.

### 16.2 Port Allocation Records

- canonical: filesystem snapshot
- fast lookup: SQLite index
- runtime reconstruction: Docker labels

### 16.3 Validation

- active listener collision check
- orphan binding detection
- stale allocation cleanup on reconciliation

## 17. Container Labels

All managed resources MUST carry labels:

```txt
forge.managed=true
forge.project_id=<id>
forge.environment=<env>
forge.generation=<n>
forge.contract_version=1
forge.deployment_id=<uuid>
```

These labels support:

- reconstruction
- cleanup
- observability
- reconciliation

## 18. Build Model

### 18.1 Supported Source Types

- source build from Git repository plus ref
- local `--from` source path for alpha/dev mode

### 18.2 Source Build Rules

- BuildKit enabled
- local Docker Engine build
- source acquisition resolves to a local immutable checkout before build execution
- host architecture only in v1
- no multi-arch support
- no remote cache export
- build secrets via BuildKit secret mounts only

### 18.3 Conflict Rule

If both `commit_sha` and `image_digest` are supplied, reject request.

## 19. Secret Model

### 19.1 Scope Precedence

```txt
deployment override
environment
project
global
```

### 19.2 Secret Storage

- encrypted at rest
- AES-256-GCM
- master key from `FORGE_MASTER_KEY`
- no plaintext persistence
- no manifest secret values

### 19.3 Secret Write Path

Secrets are written only through the API.

Manifests contain secret references only.

### 19.4 Runtime Injection

- decrypt in memory only
- inject through Docker API
- redact from logs and events

### 19.5 Secret Redaction Rules

- exact-value redaction applies by default to secrets with length `>= 8`
- secrets shorter than `8` characters are not redacted by exact-value matching unless explicitly marked sensitive
- env var names are not treated as secret values by default

### 19.6 Build Secrets

- mounted only during build
- temp material cleaned immediately after completion/interruption
- secure deletion claim is limited to prompt cleanup and restrictive permissions, not physical media erasure guarantees

## 20. API Security

### 20.1 Auth

- one global bearer token in v1
- single trusted operator model
- no RBAC

## 20.3 Web Role

- web is a human visibility/control surface
- web is not the primary deployment engine
- initial scope is login, projects, environments, current/previous generation visibility, and events/logs/diagnostics
- any web-triggered operation must flow through the same API and queue as every other surface

### 20.2 Webhook Security

- GitHub signature verification required
- GitHub delivery ID replay cache required
- duplicate deliveries ignored within replay window

## 21. Queue Model

- one global active deployment at a time
- FIFO by default
- persisted queue checkpoints
- restart-safe recovery
- queue position visible as `X of Y`

## 22. Startup Convergence Sequence

Boot order:

1. load daemon config
2. validate `FORGE_MASTER_KEY`
3. verify filesystem roots
4. verify Docker availability
5. verify Docker socket
6. verify Caddy API
7. load project metadata and pointers
8. inspect snapshots on disk
9. inspect Docker managed containers by label
10. inspect route subtree from Caddy
11. rebuild reconstructed state
12. rebuild SQLite indexes if needed
13. recover queue checkpoints
14. identify in-flight deployments
15. continue only when safe
16. otherwise mark failed and clean up
17. start health loops
18. start log stream workers
19. accept API/webhook traffic

## 23. Startup Recovery Rules

On restart:

- reconstruct from snapshots, labels, runtime inspection, and pointers
- if an in-flight deployment can safely resume, resume
- if not safely resumable, mark failed
- perform failed-generation cleanup
- preserve diagnostics
- never guess route ownership beyond Forge subtree
- never trust SQLite over runtime plus filesystem

## 24. Rollback Model

Rollback is initiated through `POST /deployments` with `intent=rollback`.

### 24.1 Eligibility

- only healthy retained generations are rollback eligible
- failed generations are diagnostic only
- always preserve `current`
- always preserve `previous`
- retain at most 2 additional failed generations with diagnostics
- remove runtime artifacts for unreferenced generations deterministically before deleting retained metadata

### 24.2 Rollback Procedure

1. resolve `previous` healthy target
2. verify image availability
3. ensure target runtime metadata is valid
4. shift route back to target generation
5. confirm route activation if HTTP
6. set `current` to target
7. set `previous` to most recent superseded healthy generation
8. persist rollback event and snapshot metadata
9. clean failed generation resources as appropriate

## 25. SQLite Role

SQLite is a derived query index only.

Allowed uses:

- dashboard queries
- event search
- queue views
- port lookup acceleration

Disallowed uses:

- generation authority
- rollback authority
- secrets authority
- convergence correctness

SQLite MUST be fully rebuildable from filesystem and runtime.

## 26. Logging and Stream Isolation

- bounded workers per stream
- bounded memory buffer
- throttling
- backpressure
- token-bucket rate limiting
- secret redaction before client delivery

Historical log behavior in v1:

- live streaming supported
- diagnostic snapshots may persist bounded excerpts
- Docker remains source for recent raw logs where available

## 27. Failure Handling

### 27.1 Failed Generation Cleanup

On deployment failure:

- stop failed container
- detach route if attached and invalid
- release transient allocations
- preserve diagnostics
- emit cleanup events
- tombstone resources that cannot be removed immediately
- retry cleanup later via reconciliation

### 27.2 Partial Cleanup Failure

If cleanup cannot fully complete:

- mark tombstoned
- continue reconciliation later
- do not reuse conflicting identities
- do not promote failed generation

## 28. Minimal API Surface v1

- `POST /deployments`
- `GET /deployments/{id}`
- `GET /projects`
- `GET /projects/{id}`
- `GET /projects/{id}/environments/{env}`
- `GET /events`
- `GET /logs/{deployment_id}/stream`
- `POST /secrets`

All responses should use structured machine-readable error codes.

## 29. Recommended Rust Module Layout

```txt
src/
  api/
  auth/
  caddy/
  config/
  contracts/
  deployments/
  docker/
  events/
  health/
  logs/
  ports/
  queue/
  recovery/
  secrets/
  snapshots/
  state/
  storage/
```

## 30. Immediate Next Artifacts

The next concrete deliverables should be:

1. JSON schema for `forge.yml`
2. OpenAPI spec for deployment and secret endpoints
3. snapshot file schemas
4. event type registry
5. deployment FSM table
6. recovery FSM table
7. startup convergence algorithm
8. daemon config schema
