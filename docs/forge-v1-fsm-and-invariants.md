# Forge v1 FSM And Invariants

## 1. Purpose

This document freezes the executable control semantics for Forge v1.

It defines:

- deployment FSM
- recovery FSM
- convergence FSM
- hard invariants
- invariant test matrix
- snapshot atomicity test matrix

This document is normative for daemon behavior.

## 2. State Model Terms

- Desired state: manifest plus deploy request after normalization.
- Observed state: Docker, Linux, and Caddy runtime inspection.
- Snapshot state: immutable generation artifact on disk.
- Reconstructed state: operational model rebuilt from snapshots, pointers, labels, and runtime inspection.

## 3. Deployment FSM

### 3.1 States

- `queued`
- `preparing`
- `building`
- `starting`
- `validating`
- `routing`
- `healthy`
- `degraded`
- `rollback`
- `failed`
- `stopped`

### 3.2 State Semantics

| State | Meaning |
| --- | --- |
| `queued` | Request accepted and persisted but not yet executing. |
| `preparing` | Manifest, source, generation allocation, and desired-state materialization in progress. |
| `building` | Image build or image resolution in progress. |
| `starting` | Container creation and startup in progress. |
| `validating` | TCP, HTTP, and contract checks in progress before route activation. |
| `routing` | Route update and activation verification in progress. |
| `healthy` | Generation is active, validated, and eligible as rollback source. |
| `degraded` | Active generation remains partially serviceable but violates steady-state health policy. |
| `rollback` | Forge is shifting active routing intent to a previous healthy generation. |
| `failed` | Generation failed deployment or unrecoverable recovery. |
| `stopped` | Generation intentionally stopped and not active. |

### 3.3 Legal Transitions

| From | To | Condition |
| --- | --- | --- |
| `queued` | `preparing` | Global queue grants execution slot. |
| `preparing` | `building` | Source build required. |
| `preparing` | `starting` | Prebuilt image digest deployment. |
| `preparing` | `failed` | Manifest invalid, generation allocation failed, source fetch failed, or desired-state normalization failed. |
| `building` | `starting` | Image build succeeded. |
| `building` | `failed` | Image build failed or timed out. |
| `starting` | `validating` | Container running and startup grace entered. |
| `starting` | `failed` | Container could not be created or started. |
| `validating` | `routing` | Validation succeeded. |
| `validating` | `failed` | TCP probe failed, HTTP probe failed when enabled, or contract invalid. |
| `routing` | `healthy` | Route activation verified, snapshot finalized, and `current` updated. |
| `routing` | `rollback` | New route partially applied and previous healthy generation must be restored. |
| `routing` | `failed` | Route activation failed and no restorable healthy path exists. |
| `healthy` | `degraded` | Steady-state failure threshold crossed. |
| `healthy` | `stopped` | Operator stop or cleanup on supersession. |
| `degraded` | `healthy` | Recovery threshold satisfied without rollback. |
| `degraded` | `rollback` | Restart attempt failed and previous healthy generation exists. |
| `degraded` | `failed` | Generation unavailable and no previous healthy generation exists. |
| `rollback` | `healthy` | Previous healthy generation restored and route activation confirmed. |
| `rollback` | `failed` | Rollback target unavailable or route restoration failed. |

### 3.4 Forbidden Transitions

The daemon MUST reject or quarantine any transition not listed above.

Examples:

- `queued -> healthy`
- `building -> routing`
- `failed -> healthy`
- `stopped -> healthy`

### 3.5 Deployment Completion Conditions

A deployment reaches `healthy` only if all of the following hold:

- manifest validated
- desired state persisted
- generation allocated and persisted
- image built or resolved
- container running
- TCP probe passed
- HTTP probe passed when enabled
- runtime contract passed
- route activation passed for HTTP services
- snapshot finalized
- `current` pointer updated atomically

## 4. Recovery FSM

### 4.1 States

- `healthy`
- `degraded`
- `retrying`
- `rollback_candidate`
- `rollback`
- `restored`
- `unavailable`

### 4.2 State Semantics

| State | Meaning |
| --- | --- |
| `healthy` | Active generation satisfies steady-state health policy. |
| `degraded` | Generation remains routed but has crossed steady-state failure threshold while still partially reachable. |
| `retrying` | Forge is performing the single restart attempt for the active generation. |
| `rollback_candidate` | Restart failed and a previous healthy generation is eligible to resume routing. |
| `rollback` | Route shift back to previous healthy generation is in progress. |
| `restored` | Previous healthy generation is active again after rollback. |
| `unavailable` | No healthy routed generation is currently reachable. |

### 4.3 Legal Transitions

| From | To | Condition |
| --- | --- | --- |
| `healthy` | `degraded` | 3 consecutive steady-state probe failures. |
| `degraded` | `healthy` | 2 consecutive successful probes without restart. |
| `degraded` | `retrying` | Restart attempt initiated. |
| `retrying` | `healthy` | Same generation passes recovery window. |
| `retrying` | `rollback_candidate` | Recovery window failed and previous healthy generation exists. |
| `retrying` | `degraded` | Recovery window failed but TCP still reachable and no rollback target exists. |
| `retrying` | `unavailable` | Container not running or TCP unreachable after restart. |
| `rollback_candidate` | `rollback` | Rollback operation started. |
| `rollback` | `restored` | Previous healthy generation routed and verified. |
| `rollback` | `unavailable` | Rollback target failed activation. |
| `restored` | `healthy` | Recovery bookkeeping completed. |

### 4.4 Recovery Policy

- probe every `15s`
- failure threshold `3`
- recovery threshold `2`
- restart attempts per degradation event: `1`
- restart recovery window: `30s`
- flapping cap: `3` state transitions per `5 min`

### 4.5 Reachability Rules

- HTTP unhealthy and TCP reachable: keep route attached and mark degraded.
- TCP unreachable after restart: remove route and mark unavailable.
- container not running after restart: remove route and mark unavailable.
- previous healthy generation exists: prefer rollback over prolonged degradation.

## 5. Convergence FSM

The convergence FSM governs daemon-wide reconciliation independent of a single deployment request.

### 5.1 States

- `booting`
- `dependency_wait`
- `reconstructing`
- `idle`
- `reconciling`
- `suspended`
- `error`

### 5.2 State Semantics

| State | Meaning |
| --- | --- |
| `booting` | Process start and config load in progress. |
| `dependency_wait` | Docker, filesystem roots, Caddy, or master key not yet available. |
| `reconstructing` | Snapshots, pointers, labels, routes, and queue state are being rebuilt into memory. |
| `idle` | Runtime model is valid and no active reconciliation task is running. |
| `reconciling` | Deployment execution, cleanup, or drift repair is in progress. |
| `suspended` | Convergence intentionally paused due to dependency loss or unsafe state. |
| `error` | Internal unrecoverable error requiring operator action or process restart. |

### 5.3 Legal Transitions

| From | To | Condition |
| --- | --- | --- |
| `booting` | `dependency_wait` | Required dependency unavailable. |
| `booting` | `reconstructing` | Required dependencies available. |
| `dependency_wait` | `reconstructing` | Dependencies recovered. |
| `reconstructing` | `idle` | State reconstruction successful. |
| `reconstructing` | `error` | Reconstruction contradiction or corruption beyond safe recovery. |
| `idle` | `reconciling` | Queue item, health event, cleanup task, or drift event exists. |
| `reconciling` | `idle` | Reconciliation completed safely. |
| `reconciling` | `suspended` | Required dependency lost during reconciliation. |
| `suspended` | `reconstructing` | Dependencies recovered and state must be rebuilt. |
| any | `error` | Invariant violation with no safe automated resolution. |

### 5.4 Convergence Rules

- SQLite rebuild must never block correctness reconstruction.
- Route intent must always be inferred from pointers plus Forge-owned Caddy subtree.
- Reconciliation must be serialized globally in v1.
- Dependency loss suspends new deploy execution before mutating active state further.

## 6. Hard Invariants

### 6.1 Routing Invariants

- Route never points to a generation that failed deploy-time validation.
- `current` must resolve to exactly one healthy or degraded active generation.
- `previous` must resolve to the most recent superseded healthy generation or be absent.
- Failed generations must never become route targets.
- HTTP route subtree ID must equal `forge:{project_id}:{environment}`.

### 6.2 Snapshot Invariants

- Every finalized generation directory must contain `snapshot.json`.
- Finalized snapshots are immutable.
- Partial snapshot writes are invalid and must not be treated as finalized generations.
- Pointer updates must never occur before snapshot finalization.
- `current` must never reference a non-finalized generation.

### 6.3 Generation Invariants

- Generation numbers are monotonically increasing per `(project_id, environment)`.
- Generation numbers are never reused.
- Gaps are allowed and must not be repaired.
- At most one generation per `(project_id, environment)` may be the routed active generation.

### 6.4 Health Invariants

- A generation cannot be marked `healthy` unless TCP validation has passed.
- A generation with `service_type=http` cannot be marked `healthy` unless HTTP validation has passed when enabled.
- Unavailable generations must not remain routed.
- Rollback targets must be previously healthy generations only.

### 6.5 Queue Invariants

- At most one deployment is executing across the daemon.
- Queue order is deterministic for accepted requests.
- Dequeue must not occur before generation execution ownership is exclusive.

### 6.6 Secrets Invariants

- Secret values never appear in persisted manifest files.
- Secret values never appear plaintext in snapshot metadata.
- Exact-value log redaction applies only to secrets with length `>= 8` unless explicitly marked sensitive.

## 7. Invariant Test Matrix

### 7.1 Routing And Pointer Tests

| Test ID | Scenario | Expected Result |
| --- | --- | --- |
| `INV-ROUTE-001` | New generation passes validation and route cutover completes | `current` points to new generation and route target matches it |
| `INV-ROUTE-002` | New generation fails validation before routing | route remains on prior `current` generation |
| `INV-ROUTE-003` | Route activation fails after subtree update | route restored or marked failed, `current` unchanged |
| `INV-ROUTE-004` | Degraded generation with TCP reachable and no rollback target | route remains attached and state becomes `degraded` |
| `INV-ROUTE-005` | Generation becomes TCP unreachable after restart | route removed and state becomes `unavailable` |

### 7.2 Snapshot And Pointer Tests

| Test ID | Scenario | Expected Result |
| --- | --- | --- |
| `INV-SNAP-001` | Finalize succeeds | snapshot immutable and pointer write allowed |
| `INV-SNAP-002` | Pointer write attempted before finalize | operation rejected |
| `INV-SNAP-003` | `current` points to missing generation | reconstruction enters safe error handling |
| `INV-SNAP-004` | `previous` points to failed generation | invariant violation and pointer repair required |

### 7.3 Generation Tests

| Test ID | Scenario | Expected Result |
| --- | --- | --- |
| `INV-GEN-001` | Two deploys allocate sequential generations | second generation number greater than first |
| `INV-GEN-002` | Crash after generation allocation before finalize | next deploy gets new generation number, gap preserved |
| `INV-GEN-003` | Concurrent allocation attempts | only one succeeds at a time due to file lock |

### 7.4 Rollback Tests

| Test ID | Scenario | Expected Result |
| --- | --- | --- |
| `INV-RB-001` | Healthy previous generation exists after sustained failure | rollback target selected and route restored |
| `INV-RB-002` | Only failed generations exist in history | rollback rejected |
| `INV-RB-003` | `previous` missing but older healthy snapshots exist | reconstruction recomputes candidate and preserves pointer semantics |

## 8. Snapshot Atomicity Test Matrix

### 8.1 Crash Windows

| Test ID | Crash Window | Expected Recovery |
| --- | --- | --- |
| `ATOMIC-001` | crash during `snapshot.json` temp write | generation ignored as non-finalized |
| `ATOMIC-002` | crash after file write before file fsync | generation ignored unless finalize marker proves durability |
| `ATOMIC-003` | crash after file fsync before rename | temp file ignored, finalized generation absent |
| `ATOMIC-004` | crash after rename before directory fsync | recovery treats generation as suspicious and revalidates finalize completeness |
| `ATOMIC-005` | crash after snapshot finalize before `current` swap | old `current` remains authoritative |
| `ATOMIC-006` | crash during `current` temp write | old `current` remains authoritative |
| `ATOMIC-007` | crash after `current` rename before directory fsync | recovery rechecks route subtree and snapshot finalize before trusting pointer |
| `ATOMIC-008` | route shifted but `current` stale | reconstruction detects divergence and repairs toward route plus finalized snapshot truth |

### 8.2 Atomicity Requirements

- snapshot finalize and pointer swap are separate phases
- pointer swap must be atomic
- route activation without durable pointer update must be recoverable
- stale pointers must be repairable from finalized snapshot plus route inspection

## 9. Implementation Guidance

### 9.1 Recommended Code Structure

- `state/fsm.rs`: enums, transition guards, invariant-aware state mutations
- `state/invariants.rs`: invariant checks and repairable violation classification
- `snapshots/writer.rs`: temp-write, fsync, rename, finalize logic
- `snapshots/pointers.rs`: atomic `current` and `previous` updates
- `recovery/engine.rs`: steady-state recovery FSM
- `deployments/executor.rs`: deployment FSM orchestration

### 9.2 Error Classification

Every transition failure should classify into one of:

- retryable
- rollbackable
- cleanup_required
- invariant_violation
- operator_action_required

## 10. Immediate Next Tests

Before feature expansion, implement tests for:

1. deployment transition legality
2. recovery transition legality
3. convergence transition legality
4. generation allocation monotonicity
5. snapshot finalize atomicity
6. pointer swap atomicity
7. route never targeting failed generation
8. rollback target always healthy
