# Forge Invariants

Forge is built around **deterministic runtime convergence**.

These invariants are more important than features.
If an implementation violates these rules, the implementation is wrong.

---

## 1. Core Runtime Invariant

```txt
running container != successful deployment
```

A running container only proves that a candidate runtime artifact exists.

A deployment is successful only after:

```txt
candidate
→ validated
→ finalized
→ activated
→ promoted
```

---

## 2. Activation Ordering

Forge must never promote a generation before validation and activation complete.

### Correct Order

```txt
candidate generation
→ TCP/HTTP validation passes
→ snapshot finalized
→ route activation verified
→ current pointer updated
```

### Invalid Order

```txt
candidate generation
→ current pointer updated
→ validation later
```

This must never happen.

---

## 3. Current Pointer Semantics

`current` represents the intended active generation.

```txt
current = control-plane intent
```

Routes reconcile toward `current`.

`current` must not blindly follow observed route state.

---

## 4. Previous Pointer Semantics

`previous` points to the most recent superseded healthy generation.

`previous` exists for rollback eligibility.

Failed generations must never become `previous`.

---

## 5. Route Semantics

For HTTP services:

```txt
Caddy route target == current generation
```

If route state and `current` diverge, Forge reconciles routes toward `current` when `current` is valid.

Rules:

- Forge must mutate only its owned Caddy subtree
- Forge must never rewrite unrelated Caddy config

---

## 6. Snapshot Invariants

Deployment snapshots are immutable once finalized.

A finalized snapshot represents a valid rollback artifact.

A snapshot must not be finalized unless:

- container created
- container started
- TCP probe passed
- HTTP probe passed if enabled
- runtime contract passed
- route activation passed for routed HTTP services

Partial snapshots are invalid.

---

## 7. Snapshot Durability

Snapshot writes must be crash-safe.

Required persistence sequence:

```txt
write temp file
→ fsync file
→ fsync parent directory
→ atomic rename
→ fsync parent directory
```

Pointer updates must use the same durability guarantees.

---

## 8. Generation Invariants

Generation numbers are monotonically increasing per:

```txt
(project_id, environment)
```

Rules:

- generation numbers are never reused
- gaps are allowed
- failed or cancelled generations still consume generation numbers

---

## 9. Deployment Queue Invariants

Forge v1 allows exactly one active deployment globally.

Queue state must survive daemon restart.

Queue state is not deployment truth.

Deployment truth comes from:

- finalized snapshots
- pointers
- runtime inspection
- route inspection
- events

---

## 10. Deploy-Time vs Steady-State Separation

Deploy-time answers:

```txt
can this generation become active?
```

Steady-state convergence answers:

```txt
should this generation remain active?
```

These concerns must remain separate.

Do not merge deploy-time validation with steady-state recovery logic.

---

## 11. Failed Deployment Invariants

Failed deployments must never:

- advance `current`
- become `previous`
- leave active routes behind
- become rollback-eligible
- erase diagnostics

Failed deployments must:

- preserve failure reason
- emit failure event
- attempt cleanup
- write tombstone/cleanup state if cleanup is incomplete

---

## 12. Cleanup Invariants

Cleanup must be explicit.

### If cleanup succeeds

- container removed
- route removed if attached
- cleanup state marked complete

### If cleanup fails

- tombstone created
- identity blocked from reuse
- reconciliation retries later

Forge must never reuse conflicting runtime identities.

---

## 13. Rollback Invariants

Rollback targets must be previous healthy finalized generations.

Rollback order:

```txt
resolve rollback target
→ verify target exists
→ activate target route
→ verify route activation
→ update current pointer
→ emit rollback event
```

`current` must not update before rollback route activation succeeds.

Stateful rollback semantics:

- rollback restores runtime topology, not database history
- Forge does not snapshot database history during rollback
- persistent volumes are operator-owned durability and must be reattached without rewrite
- immutable generations do not imply immutable data
- ephemeral generation-scoped volumes may disappear after GC once the generation is no longer rollback-safe

Stateful backup and restore semantics:

- backups are operator-triggered snapshots of persistent Docker volumes only
- backups are crash-consistent unless hooks are configured; Forge does not quiesce databases automatically
- DB-consistent backups require explicit service-level `pre_backup_command` hooks
- backups are not WAL, PITR, or incremental history
- restore always creates a new runtime generation with new managed volumes
- restore never rewrites historical generations or mutates existing persistent volumes in place
- restore is not rollback; rollback keeps topology semantics only and does not restore DB history
- Forge remains single-node and Docker-volume only for stateful workloads

Runtime policy invariants:

- per-service CPU limit, memory limit, and restart policy are part of the immutable runtime artifact
- rollback restores the historical runtime policy of the selected generation without recomputing policy from current config drift
- convergence repairs observed runtime policy drift back to promoted truth
- promotion must fail closed when warmup detects OOM kills, crash loops, restart storms, or unstable required dependencies
- degraded readiness may report active repair failures while basic liveness remains healthy

Durable single-writer control-plane invariants:

- only the active leader may mutate shared runtime or control-plane state
- follower nodes are read-only and serve cached truth only
- replay requires valid lease ownership
- every replay mutation is lease-fenced by current owner and `lease_epoch`
- destructive replay is blocked unless explicitly safe
- corrupted intents are quarantined
- readiness remains bounded during replay
- convergence computes operational truth asynchronously
- APIs serve cached truth

Startup and replay invariants:

- startup phases are explicit: `booting`, `replaying`, `leader_acquiring`, `follower`, `leader_active`, `degraded`
- replay cannot run without valid lease ownership
- convergence does not start before replay stabilizes
- followers never replay
- replay aborts on lease loss
- replay is bounded and resumable
- checkpoint and snapshot owner mismatches degrade readiness instead of being silently repaired
- split-brain handling is detection and degradation only, not automatic distributed recovery

---

## 14. Restart Recovery Invariants

On daemon restart, Forge reconstructs state from runtime truth and persisted artifacts.

Sources of truth:

- snapshots
- current pointer
- previous pointer
- Docker labels
- Caddy route subtree
- `runtime_state.json`
- queue records

SQLite or derived indexes must never be required for correctness.

---

## 15. Runtime Authority Invariants

Forge owns orchestration semantics.

```txt
Docker = execution-only
Caddy  = routing-only
```

Docker must not decide:

- deployment health
- rollback
- promotion
- convergence
- route truth

Caddy must not decide:

- service health
- rollback
- promotion
- convergence

---

## 16. Docker Invariants

Forge-managed containers must include labels:

```txt
forge.managed=true
forge.project_id=<project>
forge.environment=<environment>
forge.generation=<generation>
forge.deployment_id=<deployment_id>
```

Docker restart policy must match the persisted per-generation runtime policy for each Forge-managed container.

Forge owns promotion decisions. A container may restart according to its configured Docker policy, but Forge must not promote or continue treating that generation as healthy when warmup observes restart storms, OOM kills, or unstable dependency chains.

---

## 17. Caddy Invariants

Forge owns only routes whose IDs match:

```txt
forge:{project_id}:{environment}
```

Rules:

- Forge must never mutate non-Forge Caddy routes
- Caddy active upstream health checks must remain disabled

Forge is the health authority.

---

## 18. Health Invariants

Rules:

- TCP validation is mandatory
- HTTP validation is mandatory for HTTP services unless explicitly disabled
- successful deployment healthcheck is required before activation
- steady-state failure thresholds must be explicit and deterministic

---

## 19. Secret Invariants

Secret values must never appear in:

- manifest files
- events
- diagnostics
- logs
- HTTP responses
- CLI output

Secret names may appear.

Secret values must only be written through the API.

Manifest files may reference secrets but must never contain secret values.

---

## 20. Redaction Invariants

Redaction must happen before persistence or delivery.

Applies to:

- events
- diagnostics
- logs
- API responses
- CLI output

Short values may require explicit sensitivity marking to avoid over-redaction.

---

## 21. Event Invariants

Events are append-only.

Events are diagnostic history, not rollback authority.

Rollback authority comes from finalized snapshots.

Events must be redacted before persistence.

---

## 22. Diagnostics Invariants

Diagnostics must survive failed cleanup.

Diagnostics must explain:

- failure stage
- error code
- probe failure
- cleanup status
- tombstone status if applicable

Diagnostics must never expose secret values.

---

## 23. Metrics Invariants

Metrics are observability only.

Metrics must never:

- become runtime authority
- drive convergence decisions

---

## 24. Log Invariants

Logs are observability artifacts only.

Rules:

- logs must be bounded
- logs must be redacted before persistence or delivery
- unbounded streaming is forbidden unless backpressure and memory limits are explicit

---

## 25. Manifest Invariants

The effective deployment manifest is loaded from the resolved source checkout for the deployment.

For the current alpha surface, that manifest is `forge.yml`.

Long-term canonical deploy source is:

```txt
repository + ref
→ commit_sha
→ source_checkout
```

Manifest values define project configuration.

Deployment request values define execution intent.

Secret values must never appear in manifests.

`--from <path>` remains an alpha/dev-mode source input and must still resolve to an immutable local source path before the deployment pipeline consumes it.

---

## 26. GitHub Webhook Invariants

Webhook deployments must verify:

- GitHub signature
- delivery ID replay protection
- exact commit resolution
- branch-to-environment mapping

Duplicate delivery IDs must not enqueue duplicate deployments.

---

## 27. API Invariants

The API must remain thin.

Business logic belongs in:

- deployment executor
- convergence engine
- storage layer
- runtime adapters

API handlers must not duplicate orchestration logic.

The API is the automation surface and must feed the same deployment queue and state machine used by CLI, webhook, and web.

---

## 28. CLI Invariants

The CLI is a thin HTTP wrapper.

Rules:

- no business logic in CLI
- all operations go through the API

The CLI is a stateless operator/client surface, not orchestration authority.

`forge` is the product-facing CLI name.

`forged` is the planned future server/runtime authority binary name, but that product taxonomy does not require an immediate binary split in code.

## 28.1 Web Surface Invariants

The web surface is visibility and control for humans, not a separate deployment engine.

Initial scope is:

- login
- projects
- environments
- current and previous generation visibility
- events, logs, and diagnostics

Web-triggered operations, when present, must flow through the same API, queue, and deployment FSM as every other surface.

---

## 29. SQLite / Index Invariants

SQLite, if present, is derived state only.

SQLite must be rebuildable from:

- filesystem snapshots
- events
- runtime inspection
- pointers

SQLite must never be required for deployment correctness.

---

## 30. Agent Safety Invariants

AI agents must not modify:

- activation ordering
- pointer semantics
- rollback ordering
- Docker/Caddy authority boundaries
- snapshot durability rules
- secret redaction rules

unless explicitly instructed.

Before accepting agent-generated code, run:

```bash
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

---

# Non-Negotiable Summary

These must always remain true:

```txt
running container != successful deployment

candidate
→ validated
→ finalized
→ activated
→ promoted

current pointer expresses intended active generation

routes reconcile toward current

failed generations never become active

secrets are never persisted or delivered plaintext

Forge owns orchestration authority
```

Additional hardening invariants:

- CLI token plaintext is never persisted server-side after issuance.
- Revoked CLI tokens must fail authentication.
- Authorization headers and other sensitive credentials must be redacted before persistence or delivery.
- Backup metadata must warn that backup artifacts may contain sensitive application data and are not encrypted yet.
- `forge doctor upgrade` performs read-only compatibility checks and must not mutate control-plane state.
