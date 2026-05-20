Forge Architecture

Overview

Forge is a deterministic deployment and runtime convergence platform for AI-generated applications.

The architecture is intentionally narrow:

single-node
deterministic
restart-safe
runtime-authoritative

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

Current implementation note:

manual deploy requests execute against the daemon process working directory unless the deploy source is provided through the GitHub webhook path.

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
