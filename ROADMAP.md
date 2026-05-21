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

Forge already contains many alpha runtime pieces. The next phase is not broad feature expansion first; it is product semantics alignment and command taxonomy hardening.

---

# Next Alpha Sequence

Recommended implementation order for the next alpha phase:

## Phase 0 — Lock Product Semantics

Goal:

```txt
align product model, source model, environment model, and command taxonomy before more implementation breadth
```

Scope:

- document `forge` as client/operator CLI
- document `forged` as the future server/runtime authority binary name
- lock the control-plane model across CLI, API, and web
- lock git-first source semantics and the source revision identity chain
- lock fixed alpha environments and derived domain semantics

## Phase 1 — `forged` Server Command Taxonomy

Goal:

```txt
separate server/runtime authority taxonomy from client/operator taxonomy without forcing an immediate binary split
```

## Phase 2 — `forge` Client Auth And Diagnostics

Goal:

```txt
stabilize operator identity and health workflows
```

Planned focus:

- `forge login`
- `forge whoami`
- `forge logout`
- `forge doctor`

## Phase 3 — `forge.yml` Manifest Contract

Goal:

```txt
lock the alpha manifest surface before deeper source and deploy work
```

## Phase 4 — Git-Backed Source Acquisition

Goal:

```txt
make repository plus ref the canonical deploy source path
```

## Phase 5 — Deploy By Git Ref

Goal:

```txt
ensure manual, API, webhook, and future web flows all resolve through the same git-ref deployment pipeline
```

## Phase 6 — Derived Domain Routing

Goal:

```txt
standardize alpha environment-to-domain derivation
```

## Phase 7 — Status, Events, And Diagnostics UX

Goal:

```txt
improve visibility without moving orchestration authority out of the server
```

## Phase 8 — Rollback UX

Goal:

```txt
make rollback intent and visibility clearer while preserving existing rollback authority semantics
```

## Phase 9 — Minimal Read-Only Web Visibility

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
- multi-service graphs
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
single-service deployments converge deterministically
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
intentionally deferred
```

Potential future scope:

- [ ] service dependency ordering
- [ ] internal service discovery
- [ ] worker workloads
- [ ] multi-container applications

Blocked until:

```txt
single-service convergence semantics are fully hardened
```

---

# Phase 9 — Stateful Workloads

Status:

```txt
research phase
```

Potential future scope:

- [ ] persistent volume semantics
- [ ] state-aware rollback constraints
- [ ] snapshot-aware persistence policies
- [ ] recovery-safe data workloads

Not part of alpha scope.

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
- [ ] multi-service orchestration in alpha
- [ ] RBAC or teams
- [ ] DNS automation
- [ ] stateful database ownership
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
