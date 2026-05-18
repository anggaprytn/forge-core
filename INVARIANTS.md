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

Docker restart policy must be disabled for Forge-managed containers.

Forge owns restart decisions.

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

`forge.project.json` is loaded from the exact commit being deployed.

Manifest values define project configuration.

Deployment request values define execution intent.

Secret values must never appear in manifests.

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

---

## 28. CLI Invariants

The CLI is a thin HTTP wrapper.

Rules:

- no business logic in CLI
- all operations go through the API

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
