![Forge Cover](https://testing-1355450658.cos.ap-jakarta.myqcloud.com/forge-cover.webp)

# Forge

**Your AI wrote the app. Now it won't deploy.**

It bound to `127.0.0.1`. Wrong port. No health check.
The container is "running" — but nothing works.

Forge fixes this.

> I got tired of manually setting up DNS, nginx, and Docker every time I deployed a new AI-generated app. Then I realized the problem was deeper — the apps themselves were broken in predictable ways. So I built the safety layer that should have existed already.

Forge is a single-node orchestration and deployment system designed around one core invariant: **a running container does not equal a successful deployment.**

Built in Rust, Forge treats deployment not as a transient event, but as a continuous state convergence problem. It enforces strict operational correctness, ensuring that AI-generated software doesn't just "deploy," but actually _converges_ to a healthy, routable state—or safely rolls back.

---

## The "Last Mile" Problem

Large Language Models (LLMs) can generate functional application code in seconds, but operationalizing that code remains a fragile, error-prone task. AI agents frequently hallucinate infrastructure requirements, leading to:

- Binding to `127.0.0.1` inside containers instead of `0.0.0.0`.
- Exposing incorrect internal ports to the host.
- Failing to implement required health-check endpoints.
- Silently crashing on the very first incoming request.

Existing orchestration solutions are mismatched for this problem:

- **Kubernetes** is architecturally misaligned for single-node, fast-iteration agentic workflows. It introduces massive overhead to solve problems AI agents don't have yet.
- **Traditional PaaS (Dokku, CapRover)** optimize for human operators, hiding the low-level infrastructure state that AI agents desperately need to debug failures.
- **Serverless** abstracts away the runtime, breaking apps that rely on specific container behaviors or background processes.

## The Forge Philosophy

Forge acts as the **"safety rails" for AI engineers**. It provides a rigorous, fail-closed infrastructure layer that replaces the "deploy and pray" model with a deterministic state machine: `Candidate → Validated → Finalized → Activated → Promoted`.

If an AI agent generates a broken deployment, Forge catches it _before_ traffic routing shifts, tears down the broken generation, and produces a mathematically clean, **secret-redacted diagnostic artifact** designed specifically for the AI to read, understand, and autonomously self-correct.

---

## Key Capabilities

### 1. Invariant-Driven Convergence

Forge does not trust its underlying runtimes (Docker/Caddy). Instead, it maintains absolute authority via an immutable on-disk state. If the daemon crashes or the host reboots, Forge reconstructs its world-view entirely from atomic filesystem pointers and snapshot logs, reconciling the system back to the intended active generation.

### 2. Zero-Downtime Atomic Promotions

A generation only becomes active if it passes exhaustive lifecycle validation. Forge explicitly tests TCP reachability and HTTP health checks on the isolated container network before signaling Caddy to update routes. **Failed generations never receive traffic.**

### 3. AI-Native Diagnostic Artifacts

When a deployment fails, Forge captures a deterministic "black box" diagnostic payload. This includes container exit codes, network topologies, reachability notes, and log tails—while rigorously **redacting all injected secrets**. This provides AI agents with high-signal context for debugging without leaking credentials.

### 4. Filesystem-as-Database

Forge eschews heavy relational databases. All state—event logs, deployment snapshots, routing pointers—is persisted via atomic filesystem operations (`fsync` + `rename`). This guarantees crash-safety and zero state corruption without the operational overhead of managing PostgreSQL or SQLite.

---

## Architecture

Forge is intentionally narrow in scope. It optimizes for **operational correctness first, not feature breadth**.

- **Control Plane:** Rust-based daemon running a deterministic convergence loop.
- **Execution:** Docker (via CLI bridging) for container lifecycle management.
- **Routing:** Caddy (via API) for dynamic, hitless reverse-proxying.
- **Storage:** Atomic filesystem directories representing discrete deployment generations.

### The Deployment Pipeline

1. **Allocate:** A new monotonically increasing generation ID is provisioned.
2. **Build:** The container image is built (via Dockerfile).
3. **Stage:** The container is started on a dedicated Forge Docker network.
4. **Validate:** TCP and HTTP probes run against the container's internal IP.
5. **Finalize:** If healthy, the snapshot is marked as immutable.
6. **Activate:** Caddy routes are updated to point to the new generation.
7. **Promote:** The active pointer is atomically swapped, completing the deploy.

---

## Quick Start

### Installation

Run the bootstrap script to install the Forge CLI and daemon:

```bash
curl -sSL https://raw.githubusercontent.com/anggaprytn/forge-core/main/install.sh | bash
```

### Initializing a Project

Navigate to your application directory and initialize a Forge configuration.

```bash
forge init
```

This generates a `forge.yml` file, defining build contexts, exposed ports, and required health invariants.

`forge.yml` can also define source-controlled runtime env values:

```yaml
env:
  API_BASE_URL: https://api.example.com
```

Forge resolves runtime env snapshots with deterministic precedence, from lowest to highest:
`forge.yml` values, project/environment secrets, deploy-time overrides (reserved), Forge-generated vars, then system/runtime reserved vars.

Every finalized generation persists immutable runtime env artifacts:

- `runtime_env_snapshot.json`: safe snapshot metadata, generated Forge vars, non-secret values, and redacted secret-backed keys.
- `resolved_runtime.json`: generation-scoped authoritative restore data used for restart and rollback recovery.

### Deploying

Forge queues the deployment, builds the artifact, and executes the convergence pipeline.

```bash
forge deploy <project_id> <environment> --from ./
```

### Inspecting State

```bash
forge status <deployment_id>
forge events
```

---

## Operational Constraints & Non-Goals

To maintain its strict guarantees, Forge explicitly accepts the following tradeoffs:

- **Single-Node Only:** Forge does not manage multi-node clusters. It is designed for vertical scaling on large, single instances (VPS or bare metal).
- **Stateless Compute:** Forge manages application runtimes, not stateful databases. You must connect your apps to external managed databases (e.g., Supabase, RDS, Neon) or manage volumes out-of-band.
- **Single Service per Project:** Forge currently maps one repository to one exposed service. Complex microservice choreography within a single project is an anti-pattern here.

---

## Status

Forge is currently in **Alpha**.

**Alpha Core Loop v2 Validated (May 2026)**:
The second operational maturity milestone freezes the core orchestration loop after validating:
- Progressive deployment lifecycles (`queued` → `promoted`)
- Persistent lifecycle state and recovery
- Rollback-safe retention and automated GC
- Immutable environment snapshots with secret sealing
- Hysteresis-aware probe stability and flapping detection
- Continuous convergence-driven runtime truth alignment

→ See [ROADMAP.md](./ROADMAP.md) for full milestone details.

---

## Contributing

Built solo. If you care about deterministic systems, correct-by-construction infrastructure, or Rust — PRs and issues welcome.

Read `ARCHITECTURE.md` and `INVARIANTS.md` first. A PR that violates core invariants won't merge.

## License

MIT License. See `LICENSE` for details.
