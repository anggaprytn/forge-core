# Forge TODO

Current baseline: Alpha Core Loop v3 is validated.
Goal: harden the frozen single-node stateful runtime without reopening core scope.

---

# Frozen Milestone

Validated and frozen in Alpha Core Loop v3:

- [x] Multi-service topology
- [x] Per-service build/runtime
- [x] Internal service DNS aliases
- [x] Per-service logs/status/diagnostics
- [x] Stateful service volumes
- [x] Persistent vs ephemeral volume semantics
- [x] Stateful rollback boundary
- [x] Backup/restore primitives
- [x] Helper-container Docker volume archive/restore
- [x] Backup hooks such as `redis-cli SAVE`
- [x] Restore lineage
- [x] Restored primary service truth
- [x] GC preserves backups and persistent volumes

# Next Work

## Operator UX

- [ ] Improve `forge diagnose` restore lineage readability
- [ ] Improve `forge history` / backup history cross-linking
- [ ] Improve per-service status/log formatting
- [ ] Improve restore safety messaging in CLI and API output

## Recovery Hardening

- [ ] Crash during backup creation recovery
- [ ] Crash during backup restore recovery
- [ ] Docker unavailable recovery for restore paths
- [ ] Caddy unavailable recovery during restore promotion

## Auth And Operator Flows

- [ ] Confirm `forge whoami`
- [ ] Confirm `forge logout`
- [ ] Expand `forge doctor` stateful workload checks

## Web Visibility

- [ ] Login
- [ ] Projects
- [ ] Environments
- [ ] Current/previous generation visibility
- [ ] Events/logs/diagnostics
- [ ] Backup and restore lineage visibility

# Current Completed Baseline

- [x] **Alpha Core Loop v3 Validated (May 2026)**
- [x] **Alpha Core Loop v2 Validated (May 2026)**
- [x] **Alpha Core Loop v1 Validated (May 2026)**
- [x] Core architecture defined
- [x] Implementation spec defined
- [x] Storage primitives
- [x] Immutable generation snapshots
- [x] Atomic current/previous pointers
- [x] Generation allocator
- [x] Persistent queue
- [x] Daemon bootstrap skeleton
- [x] HTTP API
- [x] CLI wrapper
- [x] Docker runtime adapter
- [x] Caddy routing adapter
- [x] Deploy-time TCP/HTTP validation
- [x] Snapshot finalization
- [x] Route activation
- [x] Rollback semantics
- [x] Steady-state convergence engine
- [x] Events and diagnostics
- [x] Secret injection and redaction
- [x] GitHub webhook trigger path
- [x] Dogfood E2E proofs
- [x] Phase 1 (Stabilize Alpha Baseline)
- [x] Phase 2 (Operational Visibility)
- [x] Phase 5 (Installation UX) - *Partially completed (validated installer/VPS guide)*

---

# Phase 1: Stabilize Current Alpha Baseline

- [x] Commit clean CLI baseline
- [x] Verify all tests pass

```bash
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

- [x] Remove or silence harmless warnings
- [x] Ensure `README.md` matches actual current state
- [x] Ensure `ARCHITECTURE.md` matches actual current state
- [x] Add this `TODO.md`

---

# Phase 2: Operational Visibility

Do this in narrow slices.

## 2.1 Metrics

- [x] Add minimal metrics registry
- [x] Expose `GET /metrics`
- [x] Output Prometheus text format

Track:

- [x] `deployments_total`
- [x] `failed_deployments_total`
- [x] `rollback_total`
- [x] `queue_depth`
- [ ] `probe_failures`
- [ ] `convergence_transitions`

### Tests

- [x] `metrics_endpoint_exposes_prometheus_text`
- [ ] `metrics_increment_on_deploy_failure`
- [x] `metrics_report_queue_depth`

### Rules

- No convergence semantic changes
- No Docker/Caddy trait changes
- No logs/SSE in this slice

---

## 2.2 Bounded Logs

- [x] Add bounded persisted deployment log excerpts
- [x] Expose `GET /logs/:deployment_id`
- [x] Redact secret values before persistence/delivery
- [x] Enforce max retained log size

### Tests

- [x] `logs_endpoint_redacts_secret_values`
- [x] `logs_endpoint_is_bounded`
- [x] `failed_deploy_logs_preserve_diagnostic_context`

### Rules

- No `docker logs -f`
- No SSE yet
- No unbounded streaming

---

## 2.3 Doctor Command

- [x] Add `forge doctor`
- [x] Check Docker availability
- [x] Check Caddy availability
- [x] Check storage root
- [x] Check `FORGE_MASTER_KEY`
- [x] Return clear diagnostic output

### Tests

- [x] `doctor_reports_docker_unavailable`
- [x] `doctor_reports_caddy_unavailable`
- [x] `doctor_reports_missing_master_key`

---

# Phase 3: Real Dogfooding

Goal: validate product thesis, not add features.

- [ ] Generate 5 AI-created sample apps
- [ ] Add `forge.yml`
- [ ] Deploy via GitHub webhook
- [ ] Confirm route live
- [ ] Confirm events visible
- [ ] Confirm rollback works

Track:

- First deploy success rate
- Manual infra fixes required
- Failure reasons
- Missing contract assumptions

### Success Target

> AI-generated app deploys with near-zero manual infrastructure repair.

---

# Phase 4: Runtime Hardening

- [ ] Crash during build recovery
- [ ] Crash during validation recovery
- [ ] Crash during route activation recovery
- [ ] Crash during rollback recovery
- [x] Orphaned container cleanup
- [x] Orphaned route cleanup
- [x] Tombstone retry loop
- [x] Disk pressure handling
- [ ] Docker unavailable recovery
- [ ] Caddy unavailable recovery

### Tests

- [ ] `crash_during_build_recovers`
- [ ] `crash_during_route_activation_recovers`
- [x] `orphaned_container_is_removed`
- [ ] `orphaned_route_is_removed`
- [x] `orphaned_route_is_removed`
- [x] `cleanup_retry_eventually_succeeds`

---

# Phase 5: Installation UX

- [x] Wire `forge daemon` to the existing HTTP/daemon runtime path
- [x] Add `forge init`
- [ ] Generate basic `forge.yml`
- [x] Generate local example config
- [ ] Add install instructions
- [ ] Add local development quickstart
- [x] Add VPS setup guide
- [x] Validate VPS alpha milestone (12h soak)
- [ ] Add GitHub webhook setup guide
- [ ] Add Caddy setup automation

---

# Phase 6: Minimal Dashboard

Only after CLI and dogfood workflow are stable.

Dashboard should show:

- [ ] Projects
- [ ] Environments
- [ ] Active generation
- [ ] Deployment history
- [ ] Events
- [ ] Diagnostics
- [ ] Rollback button
- [ ] Secret references, not values

Do **NOT** build:

- Analytics
- Multi-service visual graph
- RBAC
- Team management
- Preview environment UI

---

# Phase 7: AI Runtime Contract UX

- [ ] `forge contract export`
- [ ] `forge contract validate`
- [ ] `forge context claude`
- [ ] `forge context cursor`
- [ ] Generate AI-ready runtime rules
- [ ] Validate generated app against runtime contract before deploy

### Success Target

> AI agent can read Forge context and generate deployable app on first try.

---

# Deferred Explicitly

Do **NOT** build yet:

- Kubernetes support
- Multi-node orchestration
- Distributed queue
- RBAC
- Teams
- Preview environments
- Persistent volumes
- UDP workloads
- Worker workloads
- Service mesh
- Plugin system
- AI auto-remediation

---

# Agent Safety Rules

Before accepting any agent patch:

```bash
git diff --stat
git diff
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

Reject patch if it:

- Changes convergence semantics unexpectedly
- Changes pointer semantics
- Changes Docker/Caddy trait boundaries unnecessarily
- Adds broad refactors
- Introduces unbounded logs/streams
- Changes deployment activation ordering
- Weakens rollback invariants

---

# Core Invariant

```text
candidate
→ validated
→ finalized
→ activated
→ promoted
```

Never break this.
