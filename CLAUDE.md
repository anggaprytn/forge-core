# Claude Instructions for Forge

You are working on Forge, a deterministic runtime convergence platform for AI-generated applications.

Forge is infrastructure software.

Correctness matters more than speed.

---

# Project Identity

Forge is not a Docker dashboard.

Forge is runtime convergence software.

Core invariant:

```txt
running container != successful deployment
```

A deployment succeeds only after:

```txt
candidate
→ validated
→ finalized
→ activated
→ promoted
```

Never weaken this ordering.

---

# Current Baseline

Forge has reached CLI-level alpha core.

Implemented:

- Rust runtime daemon
- HTTP API
- CLI wrapper
- persistent queue
- Docker adapter
- Caddy adapter
- deploy-time validation
- immutable snapshots
- current/previous pointers
- rollback
- convergence engine
- GitHub webhook path
- secret injection/redaction
- events/diagnostics
- dogfood E2E proofs

Current focus:

```txt
operational visibility
runtime hardening
dogfood validation
```

Not new platform breadth.

---

# Critical Invariants

Before modifying code, read:

- `INVARIANTS.md`
- `ARCHITECTURE.md`
- `OPERATIONS.md`
- `TODO.md`

Do not violate:

```txt
candidate → validated → finalized → activated → promoted
```

```txt
current pointer expresses intended active generation
```

```txt
routes reconcile toward current
```

```txt
failed generations never become active
```

```txt
secrets are never persisted or delivered plaintext
```

---

# Authority Boundaries

Forge owns orchestration authority.

- Docker is execution-only.
- Caddy is routing-only.
- CLI is API wrapper only.
- HTTP handlers are thin.

Do not move orchestration logic into:

- Docker adapter
- Caddy adapter
- CLI
- HTTP handlers

---

# What Not To Do

Do not:

- perform broad refactors
- redesign traits unless explicitly requested
- change convergence semantics casually
- modify pointer semantics
- change rollback ordering
- introduce unbounded streams
- add dashboard work unless explicitly requested
- add multi-service orchestration
- add RBAC, teams, preview envs, distributed workers, or Kubernetes-like abstractions

---

# Required Test Gates

Before claiming completion:

```bash
cargo test -q
```

For runtime-sensitive changes:

```bash
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

If touching Docker/Caddy integration:

```bash
FORGE_INTEGRATION=1 cargo test -- --nocapture
```

If tests fail:

```txt
stop and report the failure
```

Do not continue adding features.

---

# Patch Discipline

Prefer small patches.

Good tasks:

```txt
Add GET /metrics only
Add forge doctor only
Add bounded log endpoint only
Fix one invariant test
```

Bad tasks:

```txt
Improve observability
Refactor runtime
Clean up architecture
Overhaul deployment flow
```

Rule:

```txt
one concern per patch
```

---

# Agent Safety Rules

Before editing:

1. Identify exact files required.
2. State what will not change.
3. Preserve runtime invariants.
4. Keep the diff small.

After editing:

```bash
git diff --stat
cargo test -q
```

For dogfood-sensitive work:

```bash
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

---

# Current Recommended Next Slices

Preferred order:

1. `GET /metrics`
2. bounded persisted logs
3. `forge doctor`
4. real dogfood apps
5. runtime hardening
6. minimal dashboard later

Do not start with UI.

---

# Metrics Slice Rules

If implementing metrics:

Allowed:

- metrics registry
- Prometheus text output
- `GET /metrics`
- counters for deployments, failures, rollbacks, probe failures, queue depth

Forbidden:

- changing convergence semantics
- changing Docker/Caddy traits
- adding log streaming
- adding doctor command
- adding dashboard

---

# Logs Slice Rules

If implementing logs:

Allowed:

- bounded persisted log excerpts
- redaction before persistence
- `GET /logs/:deployment_id`

Forbidden:

- raw `docker logs -f`
- SSE streaming initially
- unbounded buffers
- logs as runtime authority

Rule:

```txt
logs are observability only
```

---

# Runtime Semantics

Deploy-time answers:

```txt
can this generation become active?
```

Steady-state answers:

```txt
should this generation remain active?
```

Keep these separate.

---

# Snapshot Semantics

Snapshots are immutable rollback artifacts.

Never:

- mutate finalized snapshots
- finalize snapshots before validation
- update current before activation succeeds

---

# Pointer Semantics

Definitions:

```txt
current  = intended active generation
previous = most recent superseded healthy generation
```

If route and current diverge:

```txt
reconcile route toward current when current is valid
```

Do not make route truth override pointer intent.

---

# Secret Semantics

Secret values must never appear in:

- manifests
- events
- diagnostics
- logs
- API responses
- CLI output

Secret names may appear.

Always redact before persistence or delivery.

---

# Rollback Semantics

Rollback order:

```txt
resolve target
→ verify target
→ activate target route
→ verify activation
→ update current
→ emit event
```

Never update current before route activation succeeds.

---

# Failure Semantics

Failed deployments must:

- not advance current
- not become previous
- not leave active routes
- preserve diagnostics
- cleanup or tombstone failed resources

Rule:

```txt
failure handling is part of convergence
```

---

# Final Reminder

Forge is already a validated alpha core.

At this stage:

```txt
preserving correctness > shipping broad features
```

If uncertain:

```txt
choose the smaller change
```
