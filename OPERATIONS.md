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

### Convergence and Runtime Truth
Forge does not assume its internal metadata matches reality. It performs "Runtime Truth" repair:
- **Container Inspection**: Inspects live Docker labels to verify if the running container matches the intended generation.
- **Route Inspection**: Queries the Caddy admin API to ensure routes point to the correct internal IPs.
- **Deterministic Repair**: If drift is detected (e.g., container IP change after Docker restart), Forge automatically repairs the route or restarts the container to align with the `promoted` pointer.

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
- **Single-service HTTP**: Only one HTTP service per project generation is supported.
- **Daemon WorkingDirectory**: Manual `forge deploy` builds from the daemon's `WorkingDirectory`.
- **Stateful DB**: No native stateful database ownership or volume management yet.
- **Orchestration**: No multi-service application orchestration yet.
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

## Check Health

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
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

## Manual Backup And Restore

```bash
forge backup create api production
forge backup list api production
forge backup inspect <backup_id>
forge backup restore <backup_id>
```

Operator invariants:

- backups snapshot persistent Docker volumes only
- backups are crash-consistent only; Forge does not coordinate database quiescing
- backups are not PITR or incremental history
- restore creates a new runtime generation and new managed volumes
- rollback and restore are different semantics

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

Docker restart policy must remain disabled.

Forge owns restart semantics.

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
