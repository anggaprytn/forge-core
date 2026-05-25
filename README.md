![Forge Cover](https://testing-1355450658.cos.ap-jakarta.myqcloud.com/forge-cover3.webp)

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

Forge Alpha Core Loop v5 freezes the durable single-writer control-plane loop on top of the single-node multi-service model. The validated scope now includes lease-based single-writer control, cache-backed readiness and metrics, durable checkpoints, immutable control-plane snapshots, node identity, split-brain detection scaffolding, and deterministic startup/replay recovery.

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

Forge captures failure context into a structured JSON payload. This allows AI agents to understand exactly _why_ a deployment failed without human intervention.

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

### Control-Plane Surfaces

Forge exposes four distinct operator surfaces:

- **`/healthz`**: process liveness only. Verifies the daemon is running and responding. Keep it lightweight.
- **`/readyz`**: control-plane readiness only. Serves cached readiness state derived from asynchronous convergence, including explicit `active_failure` when an unresolved current blocker exists. It is not fleet health inspection.
- **`/readiness/explain`** and **`forge readiness explain`**: read-only operator interpretation of cached readiness, replay, leadership, and historical convergence state. This is an explanation layer, not a repair surface.
- **`/metrics`**: cache-backed control-plane metrics and dependency breaker diagnostics in lightweight JSON. Historical convergence counters remain monotonic observability fields; active readiness blockers are exposed separately through `readiness_status`, `readiness_reason`, and `convergence_active_failure`.
- **`forge status`**: lightweight runtime and environment summary for operators.
- **`forge diagnose`**: deep runtime truth inspection for debugging and incident response.

Architectural principle: `Convergence computes truth. APIs serve cached truth.`

---

## Quick Start

### Installation

For quick evaluation:

```bash
./install.sh --release <tag>
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

### Alpha Core Loop v5 Validated

Forge Alpha Core Loop v5 freezes the durable single-writer control plane for the current single-node stateful runtime.

- **Durable Control-Plane Checkpoints:** `control_plane/convergence_checkpoint.json` restores cached readiness, breaker state, queue depth, and control-plane freshness on warm startup. `readyz_status` and degraded reasons are restored only as cached initial state; after restart, upgrade apply, or upgrade rollback, a fresh healthy leader convergence refresh must overwrite stale `convergence_stalled` state before stall detection runs again. Checkpoints are schema-versioned; stale or corrupt files degrade readiness and are ignored rather than trusted.
- **Immutable Runtime And Route Snapshots:** `control_plane/control_plane_snapshots/` stores `runtime_snapshot.json`, `route_snapshot.json`, and `dependency_snapshot.json` with bounded retention and GC. Operators can use them for diagnostics when live dependencies are unavailable, and corrupted snapshots are skipped and rebuilt later.
- **Persistent Node Identity:** `control_plane/node.json` stores stable `node_id`, node metadata, boot timestamp, and capability hints. Node identity survives daemon restart and is used for leadership attribution and diagnostics only.
- **Operational Journal:** `control_plane/operations.jsonl` is an append-only JSONL journal for leadership changes, convergence degradation, route changes, deployment and restore events, and GC activity. Rotation is bounded and malformed entries are skipped rather than blocking startup.
- **Lease-Based Single Writer:** `control_plane/leader_lease.json` fences the active reconciler. The leader heartbeats a bounded lease, every takeover advances `lease_epoch`, stale leases can be taken over only after expiry, followers stay read-only, and mutating APIs require the local node to be the active leader.
- **Split-Brain Detection Scaffolding:** `control_plane/cluster_nodes.json` stores heartbeat observations, `observed_nodes`, `active_reconcilers`, `lease_epoch_divergence`, owner mismatch signals, and `split_brain_suspected`. This milestone performs detection and degradation only; it does not do automatic distributed repair.
- **Reconciliation Intent Log:** `control_plane/reconciliation_log.jsonl` is the intent-first mutation boundary and `control_plane/reconciliation_cursor.json` tracks replay progress. Intents are classified as `replay_safe`, `idempotent`, `destructive`, or `requires_operator_intervention`; corrupted entries are quarantined.
- **Deterministic Startup And Replay Recovery:** Startup phases are explicit: `booting`, `replaying`, `leader_acquiring`, `follower`, `leader_active`, `degraded`. Replay never runs without valid lease ownership, followers never replay, convergence waits for replay stabilization, lease loss aborts replay, and request paths stay cache-backed while recovery remains bounded and resumable.
- **Cache-Backed Readiness And Metrics:** `/healthz` is process liveness, `/readyz` is cache-backed control-plane readiness, `/metrics` is cache-backed control-plane telemetry, and `forge diagnose` plus `forge status` remain the deep/runtime inspection surfaces. Request paths never perform fleet scans.
- **Operator Readiness Semantics:** Historical convergence counters are monotonic observability fields. Active readiness blockers are represented separately and must not be inferred from stale non-zero `convergence_failures_total` or historical failure timestamps alone.
- **Validated Restart Recovery:** After daemon restart Forge returns to `ready`, `startup_phase=leader_active`, `replay_in_progress=false`, `leader=true`, `follower_mode=false`, and `reconciliation_enabled=true` without reopening synchronous inspection on the request path.

v5.1 note: stale checkpointed readiness no longer self-locks healthy recovery. A degraded cached `/readyz` response may appear briefly during warm startup, but the first healthy leader refresh recomputes readiness from fresh convergence, clears stale `convergence_stalled`, and does not increment `convergence_failures_total` on recovery alone.

Measured live checks for this milestone:

- local `/readyz`: about `8ms`
- `startup_phase`: `leader_active`
- `replay_in_progress`: `false`
- `leader`: `true`
- `follower_mode`: `false`
- `forge bench leader` p95: about `0.23ms`
- `forge bench convergence` p95: about `0.23ms`

Operational benchmarking helpers are available through:

```bash
forge --url http://127.0.0.1:18080 bench readyz
forge --url http://127.0.0.1:18080 bench leader
forge --url http://127.0.0.1:18080 bench convergence
forge --url http://127.0.0.1:18080 bench diagnostics
forge --url http://127.0.0.1:18080 bench snapshots
forge control-plane leader
forge control-plane lease
curl -s http://127.0.0.1:18080/readyz | jq
curl -s http://127.0.0.1:18080/metrics | jq
```

Previous readiness behavior was coupled to synchronous fleet-wide diagnostics, which produced pathological latency in the 48s to 150s range. The current model keeps readiness off the fleet-inspection path and bounded under scale.

### Hard Invariants

- rollback restores topology, not database history
- restore creates a new generation
- restore does not mutate existing persistent volumes in-place
- backups are crash-consistent unless hooks are configured
- Forge is still single-node and Docker-volume only
- current multi-node work is preparatory only: Forge remains single-writer with one active reconciler
- no PITR, no distributed storage, no automatic quiescing

### Strong Non-Goals

- Forge does not implement Raft.
- Forge does not implement distributed consensus.
- Forge does not provide true HA yet.
- The filesystem-backed lease is a single-writer safety primitive, not consensus.
- Multi-node support in this milestone is scaffolding and detection only.
- Split-brain handling is detection and degradation, not automatic distributed recovery.
- Request paths never depend on cross-node communication.

### Reconciliation Replay

- Forge writes intent records before deployment promotion, rollback, route activation, snapshot persistence, backup restore, and other control-plane mutations.
- Startup recovery replays only intents marked replay-safe or idempotent. Destructive or operator-sensitive intents are left pending and surfaced as degraded readiness.
- `forge control-plane replay-status` shows the replay cursor, `forge control-plane intents` lists current intents, `forge control-plane replay --dry-run` is side-effect free, and `forge control-plane replay --resume` requires the active leader.
- Forge uses intent journaling instead of synchronous fleet reconstruction so crash recovery stays deterministic, bounded, and independent from request-path ordering.

---

## Status

Forge is in **Alpha**. Alpha Core Loop v5 is the current frozen milestone for the durable single-writer control plane on single-node stateful deployments.

[Roadmap](./docs/ROADMAP.md) | [Architecture](./docs/ARCHITECTURE.md) | [Invariants](./docs/INVARIANTS.md)

---

## License

MIT License. Built for the era of agentic software.

## Security Hardening

- `forge token list`, `forge token create --name <name>`, and `forge token revoke <token_id>` manage CLI tokens without persisting plaintext server-side.
- Forge stores only token hashes plus metadata: `token_id`, `name`, `created_at`, `last_used_at`, `revoked_at`, `github_login`, and `source`.
- CLI login and `forge token create` show the token only once. Revoked tokens stop authenticating immediately.
- `Authorization` headers, bearer tokens, Forge master keys, OAuth client secrets, and app secrets are redacted from logs, diagnostics, and persisted excerpts.

## Token Rotation

- Preferred rotation env vars are `FORGE_CLI_TOKEN_SECRET_CURRENT` and `FORGE_CLI_TOKEN_SECRET_PREVIOUS`.
- Verification checks the current secret first and the previous secret second. New tokens are always issued with the current secret.
- Rotation flow:
  1. Set `FORGE_CLI_TOKEN_SECRET_CURRENT` to the new secret and `FORGE_CLI_TOKEN_SECRET_PREVIOUS` to the old secret.
  2. Restart Forge and have all CLI users run `forge login <server_url>` again.
  3. Remove `FORGE_CLI_TOKEN_SECRET_PREVIOUS` after the old tokens are no longer needed.

## Bootstrap And Upgrade Hygiene

- `bearer_token` in `forge.conf` remains the bootstrap/admin credential. Prefer CLI tokens for routine remote operation.
- Do not paste the bootstrap bearer token into shell history; use env injection or a protected config file.
- Configure signed upgrade verification with `release_public_key_path=/etc/forge/release-public-key.pem` or `FORGE_RELEASE_PUBLIC_KEY=/path/to/release-public-key.pem`.
- `forge version` reports the runtime version, git commit, build timestamp, target triple, manifest/snapshot/checkpoint/reconciliation schema versions, and storage compatibility version. Missing build metadata is rendered as `unknown`.
- `forge doctor upgrade` is read-only and checks storage readability, checkpoint compatibility, reconciliation log compatibility, backup metadata compatibility, Docker, Caddy, write access, and Linux `systemd` sanity.
- Release artifacts live under `dist/` with `forge-<version>-<platform>.tar.gz`, `release-manifest.json`, optional `release-manifest.sig`, optional `release-public-key.pem`, and `checksums.txt`.
- `release-manifest.json` binds artifact hashes, target triples, build identity, and schema/storage compatibility into one tamper-evident record when signed.
- Build production artifacts with `scripts/package-release.sh --sign --signing-key <path>`. Development-only unsigned packaging requires `scripts/package-release.sh --unsigned`.
- Publish tagged releases with `scripts/publish-release.sh <tag> --signing-key <path>` after the tag is at `HEAD`; it regenerates the signed bundle, writes `dist/RELEASE_NOTES.md`, and uploads the release assets through `gh`.
- Prefer `./install.sh --release <tag>`, `forge upgrade plan --release <tag>`, and `forge upgrade apply --release <tag>` for production/self-hosted operators. Local artifact flags remain available for staged or offline rollouts. `syncforge` remains development-only.
- Unsigned upgrades are rejected unless `--allow-unsigned` is passed explicitly. Dirty manifests are rejected unless `--allow-dirty-artifact` is passed explicitly.
- Release installs verify the signed manifest when `release_public_key_path` or `FORGE_RELEASE_PUBLIC_KEY` is configured; otherwise they fail closed unless `--allow-unsigned-release` is passed explicitly.
- Operators should verify manifest signatures, validate with `forge upgrade plan`, and keep `forge upgrade rollback` as the rollback path for the preserved `forge.previous` binary.
- Signatures provide tamper evidence for release artifacts and manifests. They do not sandbox the extracted binary or replace normal host hardening.

## Backup Safety

- Backup artifacts can contain sensitive application data.
- Backups are not encrypted yet.
- Backup files inherit host filesystem protections and should be treated as sensitive data under `/var/lib/forge/backups`.

## Registration Control

- `ALLOW_NEW_REGISTRATION` controls whether GitHub OAuth may create a new local Forge user.
- Default is `false`.
- Accepted true values: `true`, `1`, `yes`.
- Accepted false values: `false`, `0`, `no`, empty, or unset.
- Existing users can still log in when registration is closed.
- Unknown GitHub users are rejected with `Registration is closed`.
- This is registration control only, not RBAC, teams, org allowlists, or invites.

Recommended self-hosted bootstrap flow:

1. Set `ALLOW_NEW_REGISTRATION=true` in `/etc/forge/forge.env`.
2. Log in once with the owner GitHub account.
3. Set `ALLOW_NEW_REGISTRATION=false`.
4. Restart Forge.

```bash
sudo editor /etc/forge/forge.env

ALLOW_NEW_REGISTRATION=false

sudo systemctl restart forge
```
