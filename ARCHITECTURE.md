Forge Architecture

Overview

Forge is a deterministic deployment and runtime convergence platform for AI-generated applications.

The architecture is intentionally narrow:

single-node
deterministic
restart-safe
runtime-authoritative

Product direction for the next alpha phase keeps that runtime-authoritative model, while clarifying product surfaces:

- `forge` is the operator/client CLI
- `forged` is the future server/runtime authority binary name
- current implementation may temporarily continue to ship one binary
- the binary split is product taxonomy, not a required code migration in this phase

Forge treats deployment as a state convergence problem, not a container-start problem.

⸻

Core Thesis

Forge is built around one invariant:

running container != successful deployment

A deployment becomes successful only after explicit validation, snapshot finalization, route activation, and convergence promotion.

⸻

High-Level Architecture

                    ┌─────────────────┐
                    │ GitHub Webhook  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Source Resolver │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   HTTP API      │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Persistent Queue│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Deployment FSM  │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌─────────────────┐          ┌─────────────────┐
     │ Docker Runtime  │          │ Probe Runtime   │
     └────────┬────────┘          └────────┬────────┘
              │                             │
              └──────────────┬──────────────┘
                             ▼
                    ┌─────────────────┐
                    │ Snapshot Store  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Caddy Routing   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Current Pointer │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Convergence FSM │
                    └─────────────────┘

⸻

Core Components

Control-plane model:

- Forge server owns deployment queueing, source resolution, immutable snapshots, convergence, routes, rollback, and restart recovery.
- CLI is a stateless operator/client surface.
- Web is a visibility/control surface for humans.
- API is the automation surface.
- CLI, API, and web requests must converge into the same queue and deployment pipeline.

Readiness model:

- `Convergence computes truth. APIs serve cached truth.`
- `/healthz` is liveness only. It verifies the daemon process is running and responding.
- `/readyz` is control-plane readiness only. It serves cached readiness state from background convergence work.
- `forge status` is a lightweight runtime and environment summary.
- `forge diagnose` is deep runtime truth inspection for operators and debugging.
- Environment-wide health investigation belongs to diagnostics, not readiness.

The request path for `/readyz` must remain constant-time. It must never trigger synchronous Docker scans, Caddy scans, generation reconciliation, route reconciliation, or environment-wide diagnostics.

Durability model:

- Forge computes operational truth asynchronously and persists it as durable control-plane state.
- Each environment now carries a bounded, atomic convergence checkpoint.
- Each convergence cycle can emit immutable runtime, route, and dependency snapshots for replayable debugging.
- Node identity is durable and informational only. It does not imply clustering, leader election, or distributed control.
- `control_plane/cluster_nodes.json` persists observed node topology, heartbeat state, lease epochs, and capability hints for future distributed reconciliation work.
- The cluster topology document is advisory coordination state, not consensus membership and not distributed locking.
- An append-only operational journal records durable control-plane events without adding writes to the request path.
- `control_plane/reconciliation_log.jsonl` is the append-only reconciliation intent journal. Intent durability is the boundary before route, promotion, rollback, restore, snapshot, and repair mutations.
- `control_plane/reconciliation_cursor.json` stores bounded replay state including last applied intent, replay position, replay status, and recovered/skipped operations.

Single-writer coordination model:

- Forge remains single-writer. One lease holder is allowed to reconcile shared control-plane state at a time.
- The filesystem lease is advisory coordination, not Raft, not quorum, and not automatic failover consensus.
- Heartbeats and split-brain signals are used for detection and degraded readiness only.
- Request paths remain cache-backed and must never depend on live cross-node communication.
- Followers never replay intents. Replay is leader-only and correctness-biased: unsafe or destructive intents degrade readiness instead of running automatically.

Replay model:

- Recovery starts from durable intent order, not implicit runtime inspection order.
- Replay resumes only operations classified `replay_safe` or `idempotent`.
- Operations classified `requires_operator_intervention` or `destructive` are surfaced through readiness, metrics, and CLI diagnostics and remain pending until explicitly handled.
- Replay is resumable and bounded by cursor progress. Forge prefers degraded recovery over aggressive automatic mutation.

Convergence domains:

- routing reconciliation
- runtime/container reconciliation
- retention reconciliation
- backup reconciliation
- metrics refresh
- dependency probing

These domains are intentionally isolated so a degraded subsystem can mark its own domain degraded without collapsing the rest of the control plane.

Previous readiness behavior coupled probe handling to full fleet diagnostics. Under scale, that produced pathological latency in the 48s to 150s range. The current model breaks that coupling.

1. HTTP API

The control-plane entrypoint.

Operator path:

```txt
forge daemon
→ HTTP API
→ CLI/API deploy flow
```

Responsibilities:
• deployment requests
• rollback requests
• webhook ingestion
• event access
• status queries
• deployment diagnostic log access
• secret management

The API is intentionally thin.

Business logic lives in the executor and convergence engine.

Health surface note:

- `/healthz` should always be a cheap process-level response.
- `/readyz` should fail fast from cached control-plane state.
- Load balancers and uptime probes must not depend on deep runtime inspection.

Current implementation note:

manual deploy requests execute against the daemon process working directory unless the deploy source is provided through the GitHub webhook path.

Product direction note:

the long-term canonical source model is `repository + ref -> commit_sha -> immutable local checkout`, with local `--from` remaining an alpha/dev-mode operator path.

⸻

2. Persistent Queue

Forge processes deployments through a durable queue.

Properties:
• restart-safe
• single active deployment globally
• persistent replay
• deterministic ordering
• idempotent enqueue semantics

Queue state alone is never treated as deployment truth.

All deploy requests, regardless of whether they originate from CLI, API, webhook, or future web actions, converge into this same queue.

⸻

3. Deployment Executor

Responsible for deploy-time correctness.

Responsibilities
• build image
• create generation
• start container
• validate runtime assumptions
• finalize snapshot
• activate route
• advance current pointer

Critical Ordering

candidate
→ validated
→ finalized
→ routed
→ promoted

Never reversed.

⸻

4. Docker Runtime

Docker is execution-only.

Forge retains orchestration authority.

Docker responsibilities:
• image build
• container lifecycle
• runtime inspection

Docker does NOT decide:
• health truth
• rollback
• routing
• deployment success

Restart policy is explicitly disabled.

⸻

5. Probe Runtime

Validation layer.

TCP Validation

Verifies:

container reachable on declared internal port

HTTP Validation

Verifies:

application-level health semantics

Deployments fail closed on probe failure.

⸻

Readiness Architecture

Forge readiness is cache-backed.

- The convergence loop computes readiness asynchronously.
- `/readyz` serves the cached readiness snapshot.
- The handler remains bounded-time even on large fleets.
- If the cache is stale, readiness degrades immediately instead of recomputing on demand.

Example degraded response:

```json
{
  "status": "degraded",
  "reason": "readiness cache stale"
}
```

Cached readiness derives from control-plane convergence state such as:

- storage accessibility
- queue health
- Docker reachability
- Caddy admin reachability
- unresolved fatal control-plane markers
- convergence freshness and cache age

This is intentionally narrower than environment truth. Per-project runtime health, route correctness, and deeper environment inspection are operator diagnostics surfaced through `forge status` and `forge diagnose`.

Operational targets:

- local `/readyz`: under 250ms
- public `/readyz` TTFB: under 1s
- stale readiness cache: return degraded immediately
- readiness handlers: fail fast

Observed validation:

```bash
time curl -s http://127.0.0.1:18080/readyz >/dev/null
# ~13ms

curl -sk -o /dev/null \
  -w 'ttfb=%{time_starttransfer} total=%{time_total}\n' \
  https://forge.anggaprytn.com/readyz
# ttfb=0.028 total=0.028
```

⸻

Snapshot System

Snapshots are immutable deployment artifacts.

Each generation contains:

snapshot.json
runtime_state.json
events.jsonl
diagnostics/
cleanup.json

`diagnostics/` contains redacted, bounded observability artifacts such as failure summaries and persisted deployment log excerpts.

Snapshots are the rollback source of truth.

Source revision identity chain:

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

Generation retention is intentionally bounded.
Forge always preserves `current` and `previous`, keeps only a small recent set of failed generations with diagnostics, and deterministically removes older unreferenced generations plus their Forge-managed runtime artifacts.
Retention cleanup resolves and removes Forge-managed containers and images before deleting generation metadata, and orphaned runtime artifacts are also cleaned by Forge labels when metadata is already gone.

⸻

Pointer Semantics

Forge maintains:

current
previous

current

Represents:

intended active generation

NOT observed route truth.

previous

Most recent superseded healthy generation.

Used for rollback eligibility.

⸻

Routing System

Forge uses Caddy as a routing runtime.

Important constraint:

Forge owns only a dedicated subtree

Forge never manages the entire Caddy config.

⸻

Route Activation Ordering

HTTP services follow:

validated
→ snapshot finalized
→ route activated
→ route verified
→ current advanced

If route activation fails:
• current does not advance

Alpha environment model:

- supported environments: `development`, `staging`, `production`
- default branch mapping: `development -> development`, `staging -> staging`, `production -> main`
- custom environments are intentionally out of scope for alpha

Planned alpha domain derivation:

- `production -> <base_domain>`
- `staging -> staging-<base_domain>`
- `development -> development-<base_domain>`
• failed generation cleaned or tombstoned

⸻

Convergence Engine

The convergence engine handles steady-state correctness.

Deploy-time and steady-state are intentionally separate.

⸻

Deploy-Time Responsibility

Question:

can this generation become active?

⸻

Steady-State Responsibility

Question:

should this generation remain active?

⸻

Steady-State Lifecycle

healthy
→ degraded
→ restart_attempt
→ rollback
→ unavailable

⸻

Restart Recovery

Forge reconstructs runtime state from:

snapshots
runtime inspection
routes
pointers
queue state

NOT queue state alone.

⸻

Runtime Authority Model

Forge

Authoritative for:
• orchestration
• deployment semantics
• rollback
• convergence
• pointer truth

⸻

Docker

Authoritative for:
• container runtime execution

Only.

⸻

Caddy

Authoritative for:
• active HTTP route state

Observed by Forge, not controlling Forge.

⸻

Secrets System

Secrets are API-managed only.

Manifest files contain references, never values.

Secrets are:
• runtime injected
• redacted from events
• redacted from diagnostics
• redacted from logs

⸻

Failure Handling

Forge treats failure handling as part of convergence.

Not an afterthought.

⸻

Failed Deployment Invariants

failed deployment never advances current
failed deployment never leaves active route
failed deployment preserves diagnostics
failed deployment is cleaned or tombstoned

⸻

Tombstones

Used when cleanup cannot fully complete.

Purpose:
• preserve identity
• prevent generation reuse
• aid reconciliation

⸻

Restart Reconstruction

On startup, Forge scans:
• snapshots
• Docker labels
• active routes
• runtime state
• queue state

Then deterministically converges runtime state.

⸻

Runtime Contracts

Runtime contracts define deploy-time expectations:
• bind address
• port reachability
• HTTP health semantics
• service type assumptions

Purpose:

catch bad infrastructure assumptions before promotion

⸻

CLI Architecture

The CLI is intentionally thin.

It wraps the HTTP API only.

No duplicated business logic.

⸻

Storage Layout

Example:

/forge
/projects
/api
/production
/generations
/1
/2
current
previous
runtime_state.json
queue.json

⸻

Event System

Forge emits append-only events.

Examples:
• deployment queued
• validation passed
• route activated
• rollback completed
• cleanup started
• tombstone created

Events are persisted.

⸻

Dogfooding Proofs

Forge validates its operational thesis through E2E proofs:
• AI-generated app deploys first try
• bad infra assumptions blocked
• secrets redacted during failure
• rollback restores prior generation

⸻

Non-Goals

Forge intentionally avoids:
• Kubernetes replacement scope
• distributed orchestration
• cluster scheduling
• enterprise RBAC complexity
• service mesh abstractions
• premature multi-service orchestration

⸻

Design Philosophy

Forge optimizes for:

operational correctness first

not feature breadth.

The long-term thesis:

AI-generated software should converge toward operational correctness automatically.
