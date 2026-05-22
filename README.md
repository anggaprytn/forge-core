![Forge Cover](https://testing-1355450658.cos.ap-jakarta.myqcloud.com/forge-cover.webp)

# Forge

### Deterministic Convergence for AI-Generated Software.

AI can write code in seconds, but operationalizing it remains the "last mile" failure point. Infrastructure hallucinations, port mismatches, and silent crashes often break the link between code and a live URL.

Forge is the safety layer. It ensures that AI-generated applications don't just "run"—they **converge** to a healthy, routable state.

---

## The Vision
Forge treats deployment as a continuous state machine, not a one-time event. It provides the rigorous, fail-closed infrastructure needed for AI agents to deploy, validate, and self-correct their own software autonomously.

- **Deterministic Correctness:** A running container is not a successful deployment. Forge only promotes traffic once internal reachability and health invariants are strictly met.
- **AI-Native Diagnostics:** When a deployment fails, Forge generates high-signal, secret-redacted diagnostic artifacts designed for AI agents to read and fix.
- **Atomic Reliability:** Built in Rust with a "filesystem-as-database" approach. Crash-safe, restart-safe, and zero-downtime by design.

---

## Technical Grounding

### 1. Simple Configuration (`forge.yml`)
Forge Alpha Core Loop v4 extends the single-node multi-service model with per-service runtime policy persistence, Docker restart policy mapping, warmup crash gating, and basic runtime isolation signals.

```yaml
version: 1
name: forge-api
type: web

build:
  context: .
  dockerfile: Dockerfile

services:
  redis:
    runtime:
      image: redis:7
    state:
      volume: redis-data
      mount_path: /data
      retention: persistent
      pre_backup_command: redis-cli SAVE
    expose: false
  api:
    build:
      context: .
      dockerfile: Dockerfile
    runtime:
      port: 8080
      healthcheck:
        path: /health
        expected_status: 200
      depends_on:
        - redis
    expose: true
```

### 2. AI-Native Diagnostic Primitive
Forge captures failure context into a structured JSON payload. This allows AI agents to understand exactly *why* a deployment failed without human intervention.

```json
{
  "service_id": "api-service",
  "status": "failed",
  "failure_reason": "HTTP health check failed: expected 200, got 500",
  "diagnostics": {
    "tcp_reachable": true,
    "container_ip": "172.18.0.5",
    "logs_tail": [
      "ERROR: Failed to connect to database at 10.0.0.5:5432",
      "FATAL: Runtime initialization failed."
    ]
  }
}
```

---

## How it Works
Forge follows a rigid lifecycle: `Candidate → Validated → Finalized → Activated → Promoted`.

1. **Build:** Forge packages the AI-generated code into an optimized image.
2. **Stage:** The container starts in an isolated network for validation.
3. **Validate:** Exhaustive TCP and HTTP probes verify the app is actually ready.
4. **Promote:** Only healthy generations receive traffic via atomic Caddy route updates.

---

## Quick Start

### Installation
For quick evaluation:
```bash
curl -sSL https://raw.githubusercontent.com/anggaprytn/forge-core/main/install.sh | bash
```

For deterministic production builds:
```bash
cargo install forge-core
```

### Initializing & Deploying
```bash
forge init
forge deploy <project_id> production --from ./
```

---

## Operational Reality
Forge is intentionally narrow to remain bulletproof. It is a single-node orchestrator designed for vertical scale on VPS or bare metal. It optimizes for **operational calm** over feature breadth.

Isolation and tenancy notes:
- Forge relies on Docker cgroups and namespace isolation on a single node. This is an operational isolation boundary, not a security-grade multi-tenant sandbox.
- CPU and memory limits are enforced through Docker `HostConfig` and are persisted per generation so rollback and convergence restore the exact historical policy.
- Promotion now blocks when warmup detects OOM kills, restart storms, unstable health behavior, or unstable required dependencies.
- Resource exhaustion is handled as a degraded runtime event. Forge records OOM/crash metadata and refuses silent promotion of degraded containers.

- **Single-Node Authority:** No Kubernetes complexity. Just deterministic execution.
- **Stateful Alpha Scope:** Docker-volume backed stateful services are supported on one host with backup/restore primitives.
- **Secret-Safe:** Automated redaction across logs, events, and diagnostics.

### Alpha Core Loop v3 Validated

Forge Alpha Core Loop v3 freezes the stateful single-node orchestration milestone.

- **Multi-Service Topology:** One project can declare multiple services with dependency ordering and per-service build/runtime settings.
- **Internal Service DNS:** Services resolve each other by Forge-managed aliases inside the project network.
- **Per-Service Diagnostics:** Status, logs, and diagnostics are reported per service, including volume state and restore lineage.
- **Stateful Volumes:** Services can declare Docker volumes with `persistent` or `ephemeral` retention semantics.
- **Rollback Boundary:** Rollback restores topology and historical runtime/env truth, not database history.
- **Backup/Restore Primitives:** Forge can create, list, inspect, and restore backups for persistent volumes.
- **Helper-Container Archive Flow:** Docker helper containers archive and restore volume contents without host mountpoint assumptions.
- **Backup Hooks:** Services can run pre-backup hooks such as `redis-cli SAVE`.
- **Restore Lineage:** Restored generations preserve source backup lineage and report it in `forge diagnose`.
- **GC Safety:** Garbage collection preserves backups and persistent volumes.

### Hard Invariants

- rollback restores topology, not database history
- restore creates a new generation
- restore does not mutate existing persistent volumes in-place
- backups are crash-consistent unless hooks are configured
- Forge is still single-node and Docker-volume only
- no PITR, no distributed storage, no automatic quiescing

---

## Status
Forge is in **Alpha**. Alpha Core Loop v3 is the current frozen orchestration milestone for single-node stateful deployments.

[Roadmap](./ROADMAP.md) | [Architecture](./ARCHITECTURE.md) | [Invariants](./INVARIANTS.md)

---

## License
MIT License. Built for the era of agentic software.
