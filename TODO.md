# Forge TODO

Current baseline: CLI implemented.
Goal: reach usable alpha without scope creep.

---

# Current Completed Baseline

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

---

# Phase 1: Stabilize Current Alpha Baseline

- [ ] Commit clean CLI baseline
- [x] Verify all tests pass

```bash
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

- [ ] Remove or silence harmless warnings
- [ ] Ensure `README.md` matches actual current state
- [ ] Ensure `ARCHITECTURE.md` matches actual current state
- [ ] Add this `TODO.md`

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
- [ ] Add `forge.project.json`
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
- [ ] Orphaned container cleanup
- [ ] Orphaned route cleanup
- [ ] Tombstone retry loop
- [ ] Disk pressure handling
- [ ] Docker unavailable recovery
- [ ] Caddy unavailable recovery

### Tests

- [ ] `crash_during_build_recovers`
- [ ] `crash_during_route_activation_recovers`
- [ ] `orphaned_container_is_tombstoned`
- [ ] `orphaned_route_is_removed`
- [ ] `cleanup_retry_eventually_succeeds`

---

# Phase 5: Installation UX

- [x] Wire `forge daemon` to the existing HTTP/daemon runtime path
- [ ] Add `forge init`
- [ ] Generate basic `forge.project.json`
- [x] Generate local example config
- [ ] Add install instructions
- [ ] Add local development quickstart
- [x] Add VPS setup guide
- [ ] Add GitHub webhook setup guide
- [ ] Add Caddy setup guide

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
