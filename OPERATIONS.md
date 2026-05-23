# Forge Operations Guide

This document describes how to operate, debug, recover, and maintain a Forge runtime safely.

Forge is designed around:

- deterministic convergence
- operational correctness
- explicit orchestration authority

Assumptions:

- single-node deployment
- Docker runtime
- Caddy routing
- filesystem-backed snapshots
- Forge as orchestration authority

---

# Core Operational Philosophy

Forge is not:

```txt
start container
→ hope it works
```

Forge is:

```txt
converge runtime toward correctness
```

Operational decisions must preserve invariants.

Never bypass Forge orchestration semantics manually unless performing disaster recovery.

---

# Alpha Core Loop v4 Validated (May 2026)

The Forge Alpha Core Loop v4 milestone freezes the current single-node stateful orchestration model with persisted runtime policy, cache-backed readiness, and degraded-runtime repair semantics.

### Validated Capabilities

- **Per-Service Runtime Policy**: CPU, memory, and restart policy persist per service.
- **Rollback Policy Fidelity**: Rollback restores the exact historical runtime policy of the selected generation.
- **Convergence Policy Repair**: Runtime policy drift is detected and repaired back to promoted truth.
- **Promotion Safety Gates**: OOM kills, crash loops, restart storms, and unstable required dependencies block promotion.
- **Termination Diagnostics**: Diagnose/status surface termination reason, exit code, signal, restart count, OOM state, and tails when available.
- **Runtime Usage Snapshots**: Operators can inspect captured CPU/memory usage snapshots for services.
- **Cache-Backed Readiness**: Convergence computes readiness asynchronously and `/readyz` serves cached control-plane truth.
- **Readyz Degradation Semantics**: `/readyz` can return `degraded` with repair reasons while `/healthz` remains `ok`.
- **Non-Fatal Route Repair Failures**: Route repair issues surface as degraded readiness and repair work, not silent success.
- **Clean Repair Visibility**: Diagnostics expose current unresolved repair fields separately from historical repair noise.
- **Multi-Service Stateful Baseline**: Multi-service topology, stateful volumes, backup/restore, restore lineage, and GC safety remain validated.

---

# Alpha Core Loop v2 Validated (May 2026)

The Forge Alpha Core Loop v2 milestone formalizes the second validated operational maturity milestone for the Forge platform. This milestone freezes the core orchestration loop after extensive validation of progressive lifecycles, lifecycle persistence, retention/GC, immutable environment snapshots, and convergence-driven runtime truth alignment.

### Validated Capabilities

- **Progressive Deployment Lifecycle**: Deterministic state transitions from `queued` through `promoted`.
- **Lifecycle Persistence**: Full per-generation lifecycle state tracking and recovery.
- **Retention & GC**: Rollback-safe generation preservation with automatic cleanup of expired artifacts.
- **Immutable Env Snapshots**: Fully resolved and sealed runtime environment snapshots per generation.
- **Diagnostics & Logs**: Bounded, secret-redacted deployment logs and deep-inspection diagnostics.
- **Secret Lifecycle**: Immutable secret snapshots with historical restoration during rollback.
- **Probe Stability Semantics**: Hysteresis-aware health probing with flapping detection and stability windows.
- **Convergence & Runtime Truth**: Continuous repair of routing and container state toward the promoted truth.

---

# Validated Runtime Semantics

### Architecture Truth

Forge is:
- **Deterministic single-node deployment orchestration**: Designed for absolute correctness on one host.
- **Immutable generation runtime system**: Every deployment is a frozen artifact.
- **Convergence-driven control plane**: Continuous repair of runtime state toward intended truth.
- **Route-verifying deployment engine**: Zero-downtime promotions backed by out-of-band verification.

Forge is **NOT** yet:
- **Distributed scheduler**: Does not manage clusters or multi-node placement.
- **Kubernetes replacement**: Focuses on single-node simplicity, not enterprise sprawl.
- **Multi-node orchestrator**: No cross-host workload awareness.
- **Service mesh**: No mTLS, sidecars, or complex traffic shaping.
- **Autoscaling platform**: Scaling is currently manual or vertical only.
- **PITR engine**: No WAL shipping, PITR, or incremental restore chain.
- **Distributed storage system**: Stateful support is Docker-volume only on one node.

### Progressive Deployment Lifecycle
Forge enforces a strict, linear state machine for every deployment:
`queued → building → starting → warming → validating → promoted`.
A generation must successfully pass every gate before traffic is allowed to reach it.

### Lifecycle Persistence
Every deployment's lifecycle is persisted in `lifecycle.json` within the generation directory. This allows the Forge daemon to resume or fail in-flight deployments deterministically after a restart, ensuring no generation is left in an undefined state.

### Promotion Gates
Promotion is guarded by three primary gates:
1.  **Warmup**: TCP reachability and initial HTTP health probes.
2.  **Validation**: A stability window where the container must remain healthy for a minimum uptime.
3.  **Route Verification**: Final confirmation that the routing layer (Caddy) has correctly activated the new target before the deployment is marked as promoted.

### Warmup Semantics
During the `warming` phase, Forge executes high-frequency probes. A generation enters the `validating` state only after achieving a required streak of consecutive successful probes.

### Route Verification Gates
After Caddy routes are updated, Forge performs an out-of-band verification to ensure the public-facing route actually reaches the new generation's internal IP. This prevents "route shadowing" or misconfiguration from resulting in a successful deployment that is actually unreachable.

### Probe History Persistence
Probe results are recorded in `probe_history.json` for each generation. This history is used to calculate success rates, detect flapping, and provide a diagnostic trail for failing deployments.

### Retention and GC
Forge distinguishes between **Lifecycle State** and **Retention Role**. 
- **GC Never removes rollback-safe generations**: The generation marked as `rollback_target` is protected from GC even if it is old.
- **Diagnostic Tail**: A small number of recent `failed` generations are retained to allow for post-mortem analysis.

### Runtime Env Snapshots
The `runtime_env_snapshot.json` is the authoritative record of the environment variables used for a generation. It is created before the container starts and is treated as immutable once finalized.

### Secret Lifecycle Semantics
- **Finalized snapshots are immutable**: Secrets used during a deployment are "locked" into that generation's snapshot.
- **Rollback restores historical runtime env**: Rolling back to a previous generation restores the exact secret values that were active when that generation was first promoted.
- **Secrets only affect future deploys**: Changing a secret value via `forge secrets set` does not affect currently running generations until a redeploy or convergence-triggered restart occurs.

### Stateful Runtime Semantics

- **Persistent volumes survive deploy/rollback/GC boundaries** until explicitly removed by the operator.
- **Ephemeral volumes are generation-scoped** and may be collected after the generation is no longer rollback-safe.
- **Backup scope is persistent volumes only**.
- **Backups are crash-consistent by default**; application-consistent backups require hooks.
- **Restore creates a new generation** with new managed volumes and new runtime truth.
- **Restore does not mutate existing persistent volumes in place**.
- **Rollback is not restore**; it reuses runtime topology semantics and does not recover database history.

### Convergence and Runtime Truth
Forge does not assume its internal metadata matches reality. It performs "Runtime Truth" repair:
- **Container Inspection**: Inspects live Docker labels to verify if the running container matches the intended generation.
- **Route Inspection**: Queries the Caddy admin API to ensure routes point to the correct internal IPs.
- **Deterministic Repair**: If drift is detected (e.g., container IP change after Docker restart), Forge automatically repairs the route or restarts the container to align with the `promoted` pointer.

### Readiness Architecture

Forge readiness is cache-backed.

- The convergence loop computes readiness state asynchronously.
- `/readyz` serves the cached readiness state.
- The request path is constant-time and fail-fast.
- Stale cache state returns degraded immediately.

Architectural principle:

`Convergence computes truth. APIs serve cached truth.`

Readiness no longer triggers synchronous fleet scans, Docker enumeration, Caddy enumeration, generation reconciliation, or environment-wide status recomputation on the request path.

The old design coupled readiness to fleet-wide diagnostics. In practice that created pathological 48s to 150s latency. Readiness is now separated from deep inspection.

Readiness is derived from cached convergence state such as:

- storage accessibility
- queue health
- Docker reachability
- Caddy admin reachability
- unresolved fatal control-plane markers
- convergence freshness and cache age

Environment-level health belongs to diagnostics, not readiness.

---

# Operational Observability

Forge now exposes cache-backed control-plane observability through `/readyz`, `/metrics`, and `forge bench`.

### Convergence Model

- A background convergence refresh loop computes cached control-plane truth.
- Request paths do not perform live Docker scans, live Caddy scans, or fleet-wide reconciliation.
- Every convergence cycle is time-bounded and records its duration, last success, last failure, and failure count.

### Cache-Backed APIs

- `/readyz` returns cached control-plane readiness only.
- `/metrics` returns cached JSON diagnostics and counters in constant time.
- Request handlers read cached state only; Docker and Caddy probing happens in the background loop.

### Metrics and Diagnostics

`/metrics` includes:

- `convergence_loop_duration_ms`
- `convergence_last_success_unix`
- `convergence_last_failure_unix`
- `convergence_failures_total`
- `readiness_cache_age_ms`
- `readyz_requests_total`
- `readyz_latency_ms`
- `readyz_degraded_total`
- `docker_probe_latency_ms`
- `caddy_probe_latency_ms`
- dependency breaker state, failure count, last success, next retry time, and last error

### Degraded Mode Semantics

- Docker or Caddy outages degrade readiness but do not block API responses.
- When a dependency repeatedly fails, its circuit breaker opens and the loop backs off exponentially.
- Readiness continues to serve cached degraded state while the loop waits for the next retry budget.
- Recovery automatically closes the breaker after a successful probe.

### Readiness Cache Staleness

- If the readiness cache ages past the freshness threshold, `/readyz` returns `degraded` immediately.
- If convergence has not completed successfully within the stall threshold, readiness reports `convergence stalled`.
- APIs remain responsive even when convergence is stale or dependencies are unhealthy.

Example:

```json
{
  "status": "degraded",
  "reason": "convergence stalled",
  "last_success_unix": 1779522000
}
```

### Troubleshooting Flow

To identify stalled convergence:

1. Check `/readyz` for `reason: "convergence stalled"` or `reason: "readiness cache stale"`.
2. Check `/metrics` for `convergence_last_success_unix`, `convergence_last_failure_unix`, and `convergence_failures_total`.
3. If cache age keeps rising, inspect the daemon process and background refresh loop.

To identify Docker degradation:

1. Check `/metrics.docker.breaker.state`.
2. Check `/metrics.docker.breaker.last_error`.
3. Confirm `docker_probe_latency_ms` and whether `next_retry_unix` is in the future.

To identify Caddy degradation:

1. Check `/metrics.caddy.breaker.state`.
2. Check `/metrics.caddy.breaker.last_error`.
3. Confirm `caddy_probe_latency_ms` and whether `next_retry_unix` is in the future.

Expected behavior during dependency outages:

- deployments may stall or degrade
- readiness becomes `degraded`
- `/metrics` continues to respond quickly from cache
- operator diagnostics remain available
- recovery happens automatically once the dependency is restored

### Bench Utilities

Use the local benchmark helpers to catch regressions:

```bash
forge --url http://127.0.0.1:18080 bench readyz
forge --url http://127.0.0.1:18080 bench convergence
```

These report latency, throughput, cache age, and cached convergence duration. `lock_wait_ms` is reported as `n/a` because the benchmarked endpoints are intentionally cache-backed and do not wait on live reconciliation work.

---

# Deployment Lifecycle States

Forge generations move through these authoritative states:

| State | Description |
| :--- | :--- |
| `queued` | Deployment is waiting in the persistent queue for processing. |
| `building` | The container image is being built from source. |
| `starting` | The container is being created and started on the managed network. |
| `warming` | Initial health probes are running to ensure the process is ready. |
| `validating` | Stability window; container must maintain health for N consecutive seconds. |
| `promoted` | Active, healthy, and receiving live traffic. |
| `rollback` | This generation is being actively restored following a failure of a newer generation. |
| `failed` | Deployment failed a validation gate and was stopped. |
| `gc_eligible` | Marked for removal by the garbage collector. |

---

# Retention Roles

A generation's **Retention Role** determines its protection from the Garbage Collector:

- **current**: The currently promoted generation receiving traffic. (Protected)
- **rollback_target**: The last known healthy generation before `current`. (Protected)
- **retained**: Older generations or failed generations kept for a short diagnostic window.
- **gc_eligible**: Generations that can be safely deleted to reclaim disk space.

---

# Probe Stability Semantics

Forge uses `probe_history.json` to implement robust stability tracking:

- **Hysteresis**: Prevents rapid state oscillations by requiring a "streak" of successes before recovery.
- **Flapping Detection**: Monitors for alternating success/failure patterns that indicate an unstable runtime.
- **Stability Windows**: Enforces a minimum "quiet period" where a generation must be perfectly healthy before promotion.
- **Transient vs. Critical**: Single transient probe failures do not trigger an immediate rollback but are recorded in the history tail.

---

# Operational Invariants

Explicit rules that the Forge engine must never violate:

1.  **Finalized generations are immutable**: Once a snapshot is finalized, its binary, config, and env are locked.
2.  **Rollback never recomputes runtime state**: It restores the exact `resolved_runtime.json` from the target generation.
3.  **Convergence repairs drift toward promoted truth**: The engine always aligns the runtime (Docker/Caddy) with the `promoted` pointer.
4.  **Route activation must match validated runtime target**: Never route traffic to a generation that hasn't passed its validation gates.
5.  **GC never removes rollback-safe generations**: `current` and `rollback_target` are sacred.
6.  **Runtime truth is container-inspected, not metadata-assumed**: Trust the Docker API over the internal cache.

---

### Alpha Readiness Checklist

- [x] **Install**: `install.sh` is conservative and idempotent.
- [x] **Deploy**: `forge deploy api production` promotes a new generation.
- [x] **Rollback**: `forge rollback api production` restores the previous generation.
- [x] **Restart Forge**: `systemctl restart forge` reconstructs state.
- [x] **Restart Caddy**: `systemctl restart caddy` results in route repair.
- [x] **Restart Docker**: `systemctl restart docker` results in container IP churn repair.
- [x] **Reboot VPS**: Host reboot results in full automatic recovery.
- [x] **Retention**: Old generations and metadata are cleaned up deterministically.
- [x] **Orphans**: Orphaned containers and routes are removed or tombstoned.
- [x] **12h Soak**: Runtime remains stable under soak.

## install.sh behavior

The `install.sh` script is designed to be safe and non-destructive:
- It installs the binary and systemd unit.
- It prepares the storage root at `/var/lib/forge`.
- It creates default config/env files if they are missing.
- It **does not** install Docker or Caddy.
- It **does not** modify firewall or Nginx rules.
- It **does not** expose the API publicly.

## Required Environment & Config

The following environment variables and configuration values are required for VPS operations:

| Key                      | Purpose                                      | Source                 |
| ------------------------ | -------------------------------------------- | ---------------------- |
| `FORGE_CONFIG`           | Path to `forge.conf`                         | Env or CLI flag        |
| `FORGE_MASTER_KEY`       | 64-hex char key for secret encryption        | Env or `forge.env`     |
| `FORGE_CADDY_ADMIN_URL`  | Caddy admin API (default: localhost:2019)    | Env or `forge.env`     |
| `FORGE_CADDY_PUBLIC_URL` | Public URL for route verification            | Env or `forge.env`     |
| `FORGE_URL`              | Forge API address for CLI                    | CLI Env                |
| `FORGE_TOKEN`            | `bearer_token` from `forge.conf` for CLI     | CLI Env                |

## Operational Permissions

- **Storage**: The service user MUST own `/var/lib/forge` (or your configured `storage_root`).
- **WorkingDirectory**: Must be readable and executable (traversable) by the service user.
- **Manual Deploys**: Build from the daemon's `WorkingDirectory`. Point the service `WorkingDirectory` at the application checkout you want Forge to deploy.

---

# Known Constraints (Alpha)

- **Single-node only**: Forge manages one host at a time.
- **Docker-volume only state**: No distributed storage backend.
- **Daemon WorkingDirectory**: Manual `forge deploy` builds from the daemon's `WorkingDirectory`.
- **Backups are not quiesced automatically**: Use `pre_backup_command` where needed.
- **No PITR**: Restore is full backup replay into a new generation, not point-in-time rewind.
- **Public API**: Should remain bound to `localhost` behind Nginx/CLI unless intentionally exposed.

---

# Cleanup and Retention

Forge automatically manages disk space and resource leaks:
- **Current/Previous**: Always preserved for stability and rollback.
- **Generations**: Old generation metadata is bounded.
- **Runtime artifacts**: Old containers and images are removed deterministically.
- **Events**: Cleanup outcomes are visible through `forge events`.

---

# Troubleshooting Notes

- **Caddy Server ID**: Forge expects the Caddy server ID to be `"forge"`.
- **Route Shadowing**: Ensure `forge:ready` does not shadow intended active routes.
- **Port Conflicts**: Port `8080` may conflict with other services; use `18080` or similar if needed.
- **CLI Connectivity**: CLI needs both `FORGE_URL` and `FORGE_TOKEN` exported.

---

# Runtime Model

Forge manages:

- deployments
- rollback
- routing
- snapshots
- convergence
- runtime recovery

Responsibilities:

```txt
Forge  = orchestration authority
Docker = execution runtime
Caddy  = traffic routing
```

Forge owns orchestration truth.

---

# Runtime Directories

Example layout:

```txt
/forge
  /projects
    /api
      /production
        current
        previous
        runtime_state.json
        queue.json
        /generations
          /1
          /2
```

---

# Important Runtime Files

| File                 | Purpose                            |
| -------------------- | ---------------------------------- |
| `current`            | Intended active generation         |
| `previous`           | Last healthy superseded generation |
| `runtime_state.json` | Steady-state convergence state     |
| `queue.json`         | Persistent deployment queue        |
| `snapshot.json`      | Immutable deployment artifact      |
| `events.jsonl`       | Append-only deployment events      |
| `diagnostics/`       | Failure diagnostics                |
| `cleanup.json`       | Cleanup/tombstone status           |

---

# Deployment Lifecycle

Forge deploys in strict order:

```txt
candidate
→ validated
→ finalized
→ activated
→ promoted
```

Rules:

- never manually advance `current`
- never manually edit snapshots

---

# Normal Operations

## Start Forge

CLI:

```bash
forge daemon
```

systemd:

```bash
systemctl start forge
```

---

## Liveness And Readiness

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

Semantics:

- `/healthz`: process liveness only. Verifies the daemon is running and responding. Keep it lightweight.
- `/readyz`: control-plane readiness only. Verifies critical dependencies and cached convergence state. It is not fleet health or environment diagnostics.
- `forge status`: lightweight operational summary for a project or environment.
- `forge diagnose`: deep runtime truth inspection for operators and debugging.

Performance targets:

- local `/readyz`: under 250ms
- public `/readyz` TTFB: under 1s
- stale readiness cache: return degraded immediately
- readiness handlers: bounded-time and fail-fast

Example degraded response:

```json
{
  "status": "degraded",
  "reason": "readiness cache stale"
}
```

Observed validation:

```bash
time curl -s http://127.0.0.1:18080/readyz >/dev/null
# ~13ms

curl -sk -o /dev/null \
  -w 'ttfb=%{time_starttransfer} total=%{time_total}\n' \
  https://forge.anggaprytn.com/readyz
# ttfb=0.028 total=0.028
```

---

## Deploy Application

CLI:

```bash
forge deploy api production
```

GitHub webhook flow:

```txt
git push
→ webhook
→ deploy
```

---

## Check Deployment Status

```bash
forge status <deployment_id>
```

Use `forge status` for a lightweight operational view. It summarizes runtime and environment state without turning readiness probes into deep inspection.

---

## View Events

```bash
forge events
```

---

## Manual Rollback

```bash
forge rollback api production
```

Rollback restores the previous healthy finalized generation.

## Troubleshooting Guidance

Use:

- `/healthz` for liveness probes
- `/readyz` for control-plane readiness probes
- `forge status` for operational overview
- `forge diagnose` for deep debugging

Do not:

- use `/readyz` as fleet health inspection
- use readiness endpoints for per-project monitoring
- couple load balancer probes to expensive reconciliation work

## Manual Backup And Restore

```bash
forge backup create api production
forge backup list api production
forge backup inspect <backup_id>
forge backup restore <backup_id>
```

Example state hook:

```yaml
services:
  redis:
    state:
      volume: redis-data
      mount_path: /data
      retention: persistent
      pre_backup_command: redis-cli SAVE
```

Operator invariants:

- backups snapshot persistent Docker volumes only
- backups are crash-consistent only; Forge does not coordinate database quiescing
- DB-consistent backups require app or service `pre_backup_command` hooks
- backups are not PITR or incremental history
- restore creates a new runtime generation and new managed volumes
- rollback and restore are different semantics
- rollback does not restore database history

---

# Deployment Failure Operations

## Deployment Failed Before Promotion

Expected behavior:

- `current` unchanged
- failed generation cleaned or tombstoned
- diagnostics preserved
- events recorded

Inspect:

```txt
diagnostics/
events.jsonl
cleanup.json
```

---

## Common Failure Causes

| Failure                   | Meaning                    |
| ------------------------- | -------------------------- |
| `tcp_unreachable`         | container not reachable    |
| `http_unhealthy`          | health endpoint failed     |
| `route_activation_failed` | Caddy activation failed    |
| `secret_missing`          | required secret missing    |
| `cleanup_failed`          | runtime cleanup incomplete |

---

# Secret Operations

Secrets are API-managed only.

Never place secret values in:

- `forge.project.json`
- git
- diagnostics
- logs

---

## Set Secret

```bash
forge secrets set api production DATABASE_URL postgres://...
```

---

## Secret Failure Expectations

Forge must:

- redact secret values
- preserve secret names
- fail before container start if required secret missing

---

# Restart Recovery

Forge restart recovery is deterministic.

On startup, Forge reconstructs runtime state from:

- snapshots
- Docker labels
- routes
- `runtime_state.json`
- pointers
- queue state

---

## Expected Restart Behavior

If deployment was in-flight, Forge must either:

```txt
resume safely
```

or:

```txt
fail
→ cleanup deterministically
```

No undefined partial deployment state should remain.

---

# Crash Recovery

## Crash During Deployment

Expected:

- orphaned candidate cleaned or tombstoned
- `current` unchanged
- routes reconciled
- diagnostics preserved

---

## Crash During Route Activation

Expected:

- `current` not advanced
- route repaired toward `current`
- failed generation cleaned or tombstoned

---

## Crash During Rollback

Expected:

- `current` reconstructed from finalized snapshot
- routes reconciled toward `current`
- rollback retried safely

---

# Convergence Operations

Forge continuously reconciles runtime state.

---

## Steady-State Lifecycle

```txt
healthy
→ degraded
→ restart_attempt
→ rollback
→ unavailable
```

---

## Health Failure Behavior

Expected behavior:

- generation marked degraded
- one restart attempt
- rollback if restart fails and previous healthy exists
- unavailable if recovery impossible

---

# Route Operations

Forge owns only routes matching:

```txt
forge:{project_id}:{environment}
```

Never manually edit Forge-managed routes unless performing disaster recovery.

---

## Validate Active Route

Inspect:

- `current` pointer
- active Caddy route
- `runtime_state.json`

These should converge.

---

# Docker Operations

Forge-managed containers must contain labels:

```txt
forge.managed=true
forge.project_id=<project>
forge.environment=<environment>
forge.generation=<generation>
```

---

## Inspect Containers

```bash
docker ps
docker inspect <container>
```

---

## Important Constraint

Docker restart policy is part of the persisted runtime policy.

Forge still owns deployment promotion semantics, but container restart behavior may be configured per service as `no`, `always`, `on-failure`, or `unless-stopped`. Convergence treats drift in that policy as repairable runtime drift and recreates the container to restore the stored policy.

Single-node isolation boundaries:
- Forge depends on Docker for CPU and memory enforcement on one host.
- This is not a security-grade tenant boundary. Co-located workloads still share the same kernel, daemon, disks, and operator trust domain.
- OOM during warmup, restart storms, or repeated unstable probes block promotion and surface through `forge diagnose`.

---

# Caddy Operations

Forge manages only its dedicated subtree.

Never replace full Caddy config manually while Forge is running.

---

## Validate Route State

Inspect:

```bash
curl localhost:2019/config/
```

Verify:

- Forge route subtree exists
- target container correct
- no orphaned routes

---

# Tombstones

Tombstones exist when cleanup cannot complete safely.

Purpose:

- block identity reuse
- preserve reconciliation context
- enable retry cleanup

Do not delete tombstones blindly.

---

# Manual Cleanup

Perform manual cleanup only if convergence cannot repair automatically.

---

## Remove Orphan Container

Verify:

- not `current`
- not `previous`
- no finalized snapshot
- no active queue entry

Then:

```bash
docker rm -f <container>
```

---

## Remove Orphan Route

Verify the route is not the current target.

Remove only the specific Forge subtree route.

---

# Backup Strategy

Back up:

```txt
/forge/projects
```

Critical artifacts:

- snapshots
- pointers
- `runtime_state.json`
- events
- diagnostics

These are sufficient for recovery.

---

# Disaster Recovery

## Full Runtime Reconstruction

Required artifacts:

- snapshots
- Docker runtime
- Caddy routes

Forge can reconstruct convergence state from these.

---

## Restore Procedure

1. Restore `/forge/projects`
2. Restore Docker images if available
3. Restore Caddy runtime
4. Start Forge
5. Allow convergence to reconcile runtime

---

# Testing Operations

## Unit Tests

```bash
cargo test -q
```

---

## Integration Tests

```bash
FORGE_INTEGRATION=1 cargo test -- --nocapture
```

---

## Dogfood E2E

```bash
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

These validate:

- generated app deploy
- bad infra assumption rejection
- secret redaction
- rollback correctness

---

# Agent Safety Operations

Before accepting AI-agent-generated patches:

```bash
git diff --stat
git diff
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

Reject patches that:

- alter convergence semantics
- weaken rollback ordering
- change pointer authority incorrectly
- introduce unbounded logs
- bypass snapshot finalization
- bypass validation before promotion

---

# Operational Red Flags

Immediate investigation required if:

- `current` points to non-finalized generation
- active route diverges permanently from `current`
- failed generation becomes rollback target
- secret value appears in diagnostics/logs/events
- generation numbers reused
- orphaned routes accumulate
- Docker restart policy enabled
- convergence loop oscillates continuously

---

# Operational Non-Goals

Forge intentionally does not optimize for:

- distributed scheduling
- multi-node clustering
- Kubernetes replacement
- service mesh orchestration
- enterprise RBAC

Forge optimizes for:

```txt
single-node operational correctness
```

first.

---

# Most Important Operational Invariant

Never violate:

```txt
candidate
→ validated
→ finalized
→ activated
→ promoted
```

Everything else depends on this remaining true.
