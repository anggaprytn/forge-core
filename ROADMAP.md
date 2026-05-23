# Forge Roadmap

Forge is a deterministic convergence runtime for AI-generated applications.

The platform exists to solve a specific problem:

```txt
AI can generate software faster than humans can operate it safely.
```

Forge focuses on making AI-generated systems operationally reliable through deterministic deployment, convergence, rollback, and recovery semantics.

The roadmap prioritizes:

```txt
operational correctness
→ deterministic convergence
→ recovery guarantees
→ AI deployment reliability
```

before scale, orchestration breadth, or enterprise platform complexity.

---

# Current Status

Current stage:

```txt
alpha
```

**Alpha Core Loop v4 Validated (May 2026)**:

The Forge Alpha Core Loop v4 milestone hardens the single-node application orchestration loop with persisted per-service runtime policy, rollback/convergence policy fidelity, runtime usage snapshots, termination diagnostics, and degraded-runtime promotion safety.

### Validated Capabilities (v4)

- **Per-Service CPU/Memory/Restart Policy**: Each service persists its runtime policy in generation metadata.
- **Runtime Policy Persistence**: Stored runtime policy survives restart, status inspection, and diagnostics inspection.
- **Rollback Restores Historical Runtime Policy**: Rollback reinstates the exact CPU, memory, and restart policy captured by the rollback target.
- **Convergence Repairs Runtime Policy Drift**: Drift in stored runtime policy is detected and repaired back to promoted truth.
- **OOM/Crash-Loop/Restart-Storm Promotion Gates**: Warmup refuses promotion when unstable runtime signals are observed.
- **Termination Diagnostics**: Diagnose/status include exit reason, signals, restart count, OOM state, and available tails.
- **Runtime Usage Snapshots**: Status and diagnostics expose captured CPU and memory usage snapshots.
- **Non-Fatal Route Repair Failures**: Route repair failures can degrade readiness without turning the daemon fully unavailable.
- **Readyz Active Degradation Semantics**: `/readyz` reports `degraded` plus reasons when active repairs or failures remain unresolved.
- **Clean Diagnostics API Repair Fields**: Current unresolved repair signals stay visible while healthy historical noise is suppressed.
- **Multi-Service Stateful Baseline Preserved**: v3 topology, volume, backup/restore, and restore-lineage guarantees remain validated.

**Alpha Core Loop v3 Validated (May 2026)**:

- **Multi-Service Topology**: Multiple services per project with deterministic dependency ordering.
- **Per-Service Build/Runtime**: Build and runtime configuration can be declared independently per service.
- **Internal Service DNS Aliases**: Services resolve each other through Forge-managed internal aliases.
- **Per-Service Logs/Status/Diagnostics**: Operator visibility is grouped per service, including restore and volume state.
- **Stateful Service Volumes**: Services can declare attached Docker volumes.
- **Persistent vs Ephemeral Semantics**: Volume retention is explicit and enforced.
- **Stateful Rollback Boundary**: Rollback restores topology and generation truth without pretending to rewind database history.
- **Backup/Restore Primitives**: CLI/API support create, list, inspect, and restore workflows.
- **Helper-Container Volume Archive/Restore**: Backups and restores use Docker helper containers rather than direct host mount assumptions.
- **Backup Hooks**: Service-level `pre_backup_command` hooks allow DB-aware flushes such as `redis-cli SAVE`.
- **Restore Lineage**: Restored generations record backup source, prior generation, and restored volume lineage.
- **GC Safety**: Garbage collection preserves backups and persistent volumes.

**Alpha Core Loop v2 Validated (May 2026)**:

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

**Alpha Core Loop v1 Validated (May 2026)**:

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

### Milestone Summary

Forge Alpha Core Loop v1 proves that git-backed immutable deployments can achieve deterministic runtime convergence with authoritative truth and automatic rollback correctness. It provides a stable foundation for AI-generated applications to converge operationally without manual infrastructure surgery.

- [x] Deterministic deployment ordering
- [x] Restart-safe convergence recovery
- [x] Immutable deployment snapshots
- [x] Rollback semantics
- [x] Docker runtime orchestration
- [x] Caddy routing orchestration
- [x] Persistent deployment queue
- [x] GitHub-triggered deploys
- [x] Secret injection and redaction
- [x] AI-generated application deployment proofs
- **VPS Alpha Milestone (Manual Validation)**:
  - conservative and idempotent `install.sh`
  - systemd-managed daemon lifecycle
  - host reboot / daemon restart recovery
  - routing repair after Caddy/Docker restart
  - deterministic generation cleanup/retention

Current runtime note:

Forge has now frozen the single-node stateful orchestration loop. The next phase should favor UX hardening, recovery depth, and operator ergonomics over broad new runtime scope.

Post-v4 focus:
- operator UX polish for status, diagnose, history, and restore lineage
- broader recovery-path validation around backup/restore and daemon crashes
- human visibility surfaces layered on top of the frozen runtime core

---

# Next Alpha Sequence

Recommended implementation order for the next alpha phase:

## Phase 0 — Harden Operator UX

Goal:

```txt
improve visibility, recovery confidence, and operator ergonomics around the frozen v4 runtime
```

Scope:

- backup/restore UX polish
- status and diagnose improvements for multi-service generations
- clearer volume and retention visibility
- restore lineage inspection and history UX
- auth and doctor command polish

## Phase 1 — Recovery Hardening

Goal:

```txt
prove more crash and restart failure paths around stateful workloads
```

## Phase 2 — Minimal Read-Only Web Visibility

Goal:

```txt
ship a minimal human visibility surface after product semantics and operator flows are aligned
```

Initial web scope:

- login
- projects
- environments
- current and previous generation visibility
- events, logs, and diagnostics

---

# Core Philosophy

Forge optimizes for:

```txt
single-node operational correctness
```

before attempting:

- distributed scheduling
- cluster orchestration
- distributed service graphs beyond one single-node project
- enterprise abstractions
- platform extensibility

The system intentionally prefers:

- deterministic behavior over flexibility
- explicit state over implicit orchestration
- convergence guarantees over deployment speed
- operational simplicity over feature breadth

---

# Phase 0 — Runtime Foundations

Status:

```txt
completed
```

Completed:

- [x] deployment FSM
- [x] convergence FSM
- [x] snapshot model
- [x] rollback model
- [x] pointer semantics
- [x] restart recovery semantics
- [x] queue persistence
- [x] runtime contract model
- [x] operational invariants

Goal achieved:

```txt
deterministic runtime state transitions
```

---

# Phase 1 — Alpha Runtime Core

Status:

```txt
completed
```

Completed:

- [x] HTTP API
- [x] daemon bootstrap
- [x] Docker runtime adapter
- [x] Caddy routing adapter
- [x] deploy-time validation
- [x] rollback executor
- [x] convergence engine
- [x] restart reconstruction
- [x] diagnostics/events
- [x] GitHub webhook deploy path
- [x] CLI
- [x] secret injection and redaction

Goal achieved:

```txt
single-node deployments converge deterministically
```

---

# Phase 2 — Dogfood Validation

Status:

```txt
completed
```

Validated scenarios:

- [x] AI-generated applications deploy successfully first try
- [x] invalid infrastructure assumptions are blocked pre-runtime
- [x] rollback restores previous operational generation
- [x] secret redaction survives deployment failure paths

Validated outcome:

```txt
AI-generated applications can converge operationally
with minimal manual infrastructure repair.
```

---

# Phase 3 — Operational Visibility

Status:

```txt
in progress
```

Goal:

```txt
increase operator trust and runtime observability
```

---

## 3.1 Metrics

Planned:

- [x] Prometheus metrics endpoint
- [x] deployment counters
- [x] rollback counters
- [x] queue depth metrics
- [ ] convergence duration metrics
- [ ] probe failure metrics

Explicit non-goals:

- [ ] metrics-driven orchestration
- [ ] distributed telemetry systems
- [ ] full observability platform scope

---

## 3.2 Bounded Logging

Planned:

- [x] persisted deployment logs
- [x] bounded retention policies
- [x] redacted log delivery
- [x] deployment log API

Explicitly deferred:

- [ ] websocket log streaming
- [ ] unbounded log tailing
- [ ] distributed logging pipelines

---

## 3.3 Operational Diagnostics

Planned:

- [x] `forge doctor`
- [x] Docker diagnostics
- [x] Caddy diagnostics
- [x] storage diagnostics
- [x] environment validation
- [ ] recovery recommendations

Goal achieved:

---

# Phase 3.4 — Installation And VPS Packaging

Status:

```txt
completed
```

Completed in this slice:

- [x] example `forge.conf`
- [x] systemd service unit
- [x] validated `forge.yml` deployment flow (`forge init` -> `forge deploy`)
- [x] VPS operator runbook for daemon/API deploy flow
- [x] VPS alpha milestone manual validation (12h soak, host reboot recovery)

Current limitation documented explicitly:

- [x] manual CLI deploys currently build from the daemon working directory

Still needed:

- [ ] installer/package distribution
- [ ] multi-project deploy source selection
- [ ] webhook-first VPS onboarding guide

Acceptance status:

- [x] dogfood integration suite is green on daemon/VPS startup validation
- [x] manual validation pass: deploy, rollback, restart, reboot, route recovery
- [x] `forge.yml` manifest correctly drives build/runtime configuration

```txt
operators can diagnose runtime failures quickly and deterministically
```

---

# Phase 4 — Runtime Hardening

Goal:

```txt
survive operational chaos deterministically
```

Planned:

- [ ] crash-during-build recovery
- [ ] crash-during-validation recovery
- [ ] crash-during-route-activation recovery
- [ ] crash-during-rollback recovery
- [ ] orphaned resource cleanup
- [ ] tombstone retry convergence
- [ ] disk pressure handling
- [ ] Docker unavailable recovery
- [ ] Caddy unavailable recovery
- [ ] long-running soak tests

Target outcome:

```txt
runtime recovery becomes boring and predictable
```

---

# Phase 5 — Installation & Operator Experience

Goal:

```txt
reduce operational friction
```

Planned:

- [x] daemon CLI entrypoint
- [x] `forge init`
- [ ] local development setup
- [ ] VPS installation guides
- [ ] GitHub webhook setup
- [ ] Caddy setup automation
- [ ] example manifests
- [ ] production readiness checklist

Success criteria:

```txt
a single operator can deploy reliably without deep infrastructure expertise
```

---

# Phase 6 — Minimal Operations Dashboard

Goal:

```txt
visual operational clarity
```

Planned:

- [ ] deployment history
- [ ] active generation state
- [ ] event timelines
- [ ] diagnostics viewer
- [ ] rollback controls
- [ ] secret reference visibility

Intentionally avoided:

- [ ] enterprise analytics
- [ ] RBAC systems
- [ ] team/org management
- [ ] orchestration topology graphs
- [ ] platform-style abstraction layers

---

# Phase 7 — AI Runtime Tooling

Goal:

```txt
teach AI systems how deployment environments behave
```

Planned:

- [ ] runtime contract export
- [ ] AI-readable deployment context
- [ ] deployment contract validation
- [ ] Cursor/Claude context generation
- [ ] generated-app preflight validation

Long-term target:

```txt
AI agents generate operationally deployable applications first try
```

---

# Phase 8 — Multi-Service Runtime

Status:

```txt
completed in alpha core loop v3
```

Validated in v3:

- [x] service dependency ordering
- [x] internal service discovery
- [x] worker workloads
- [x] multi-container applications

---

# Phase 9 — Stateful Workloads

Status:

```txt
completed in alpha core loop v3 for single-node Docker volumes
```

Validated in v3:

- [x] persistent volume semantics
- [x] ephemeral volume semantics
- [x] state-aware rollback constraints
- [x] backup/restore primitives
- [x] restore lineage

Still out of scope:

- [ ] PITR
- [ ] distributed storage
- [ ] automatic quiescing
- [ ] database-native snapshot orchestration

---

# Phase 10 — Distributed Runtime

Status:

```txt
far future
```

Potential future scope:

- [ ] multi-node scheduling
- [ ] distributed convergence
- [ ] replicated queues
- [ ] leader election
- [ ] cluster recovery semantics

Forge is intentionally avoiding premature convergence toward Kubernetes-style complexity.

---

# Explicit Non-Goals

Forge is intentionally not optimizing for:

- [ ] Kubernetes replacement scope
- [ ] enterprise platform sprawl
- [ ] service mesh architectures
- [ ] plugin ecosystems
- [ ] workflow orchestration engines
- [ ] low-code abstractions
- [ ] multi-cloud abstraction layers
- [ ] generalized distributed schedulers
- [ ] custom environments
- [ ] custom per-environment domains
- [ ] preview environments
- [x] multi-service orchestration in alpha
- [ ] RBAC or teams
- [ ] DNS automation
- [ ] distributed or database-native stateful storage ownership
- [ ] a web deploy button as the primary product surface

The system is optimized for:

```txt
operational convergence reliability
```

not infrastructure maximalism.

---

# Success Criteria

Forge succeeds if:

```txt
AI-generated applications
deploy
recover
rollback
and converge operationally
without manual infrastructure surgery
```

---

# Long-Term Thesis

Forge is not fundamentally a deployment platform.

The deeper thesis is:

```txt
AI-generated software should converge toward operational correctness automatically.
```

Deployment is only one layer.

Convergence is the actual product.
