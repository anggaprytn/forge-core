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
No complex YAML sprawl. Define your build and health invariants in one place.

```yaml
version: 1
name: forge-api
type: web

build:
  context: .
  dockerfile: Dockerfile

runtime:
  port: 8080
  healthcheck:
    path: /health
    expected_status: 200

invariants:
  - name: health
    path: /health
    expect_status: 200
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

- **Single-Node Authority:** No Kubernetes complexity. Just deterministic execution.
- **Stateless Focus:** Optimized for application runtimes; connect to managed data stores.
- **Secret-Safe:** Automated redaction across logs, events, and diagnostics.

---

## Status
Forge is in **Alpha**. It is currently used to bridge the gap between AI generation and production-grade availability.

[Roadmap](./ROADMAP.md) | [Architecture](./ARCHITECTURE.md) | [Invariants](./INVARIANTS.md)

---

## License
MIT License. Built for the era of agentic software.
