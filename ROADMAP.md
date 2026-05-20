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

Validated capabilities:

- deterministic deployment ordering
- restart-safe convergence recovery
- immutable deployment snapshots
- rollback semantics
- Docker runtime orchestration
- Caddy routing orchestration
- persistent deployment queue
- GitHub-triggered deploys
- secret injection and redaction
- AI-generated application deployment proofs

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
- [ ] `forge init`
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
