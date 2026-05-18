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

```txt id="k0p0m7"
start container
→ hope it works
```

Forge is:

```txt id="n7s4ql"
converge runtime toward correctness
```

Operational decisions must preserve invariants.

Never bypass Forge orchestration semantics manually unless performing disaster recovery.

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

```txt id="xukmcv"
Forge  = orchestration authority
Docker = execution runtime
Caddy  = traffic routing
```

Forge owns orchestration truth.

---

# Runtime Directories

Example layout:

```txt id="vme1z5"
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

```txt id="s6moyv"
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

```bash id="t2yy5d"
forge daemon
```

systemd:

```bash id="v0lmlm"
systemctl start forge
```

---

## Check Health

```bash id="8q7l5d"
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

---

## Deploy Application

CLI:

```bash id="t7yepp"
forge deploy api production
```

GitHub webhook flow:

```txt id="ux5jq5"
git push
→ webhook
→ deploy
```

---

## Check Deployment Status

```bash id="mnktw6"
forge status <deployment_id>
```

---

## View Events

```bash id="8hbd5n"
forge events
```

---

## Manual Rollback

```bash id="f7pn1l"
forge rollback api production
```

Rollback restores the previous healthy finalized generation.

---

# Deployment Failure Operations

## Deployment Failed Before Promotion

Expected behavior:

- `current` unchanged
- failed generation cleaned or tombstoned
- diagnostics preserved
- events recorded

Inspect:

```txt id="j8ywpa"
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

```bash id="97t7aj"
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

```txt id="jlwmrr"
resume safely
```

or:

```txt id="8zhgxh"
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

```txt id="k9qfct"
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

```txt id="vq2meq"
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

```txt id="xylwtf"
forge.managed=true
forge.project_id=<project>
forge.environment=<environment>
forge.generation=<generation>
```

---

## Inspect Containers

```bash id="sjx6cn"
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

```bash id="k8u57q"
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

```bash id="1zdyfg"
docker rm -f <container>
```

---

## Remove Orphan Route

Verify the route is not the current target.

Remove only the specific Forge subtree route.

---

# Backup Strategy

Back up:

```txt id="3zcxd4"
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

```bash id="1k3svk"
cargo test -q
```

---

## Integration Tests

```bash id="hoy6uo"
FORGE_INTEGRATION=1 cargo test -- --nocapture
```

---

## Dogfood E2E

```bash id="mk09t4"
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

```bash id="l4mxgw"
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

```txt id="gkq2wz"
single-node operational correctness
```

first.

---

# Most Important Operational Invariant

Never violate:

```txt id="x6hz8r"
candidate
→ validated
→ finalized
→ activated
→ promoted
```

Everything else depends on this remaining true.
