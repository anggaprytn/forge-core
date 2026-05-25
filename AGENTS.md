# AGENTS.md

## Project Overview

Forge is a Rust control plane for deterministic deployment and runtime convergence of application containers on a single node. It is not a generic PaaS and it is not a multi-node scheduler. The core product contract is:

`running container != successful deployment`

A deployment is only successful after the full lifecycle completes:

`candidate -> validated -> finalized -> activated -> promoted`

The repository contains one Rust crate (`forge_core`) that builds the `forge` binary. That binary currently serves both operator CLI and daemon/server responsibilities. Product docs describe a future `forged` server binary, but the current codebase still ships a single binary.

## Architecture Summary

- Control-plane authority lives in the daemon and convergence loop, not in HTTP handlers, CLI parsing, Docker adapters, or Caddy adapters.
- The daemon is restart-safe and persists operational truth under `storage_root` using filesystem-backed state and atomic writes.
- Readiness is cache-backed: convergence computes truth asynchronously, `/readyz` and `/metrics` serve cached truth in constant time.
- Docker is the runtime substrate. Caddy is the HTTP routing substrate. Git/GitHub integration exists for source resolution and web login/webhooks.
- The system is explicitly single-writer. Lease-based leadership and split-brain detection scaffolding exist, but this is not consensus and not true HA.

Primary high-risk domains:

- `src/deployments.rs`: deployment FSM, validation, route activation, runtime env snapshotting
- `src/convergence.rs`: steady-state repair, rollback, retention, runtime truth
- `src/daemon.rs`: readiness cache, leadership, replay, convergence refresh, control-plane metrics
- `src/storage.rs`: atomic persistence, locks, pointer semantics, checkpoint/snapshot/journal files
- `src/reconciliation.rs`: intent log and replay safety
- `src/http.rs` and `src/auth.rs`: auth, OAuth, token issuance/verification, idempotency, API surface
- `src/upgrade.rs`, `scripts/package-release.sh`, `scripts/publish-release.sh`, `install.sh`: upgrade and release integrity

## Repository Structure

```text
src/
  main.rs              CLI entrypoint and daemon wiring
  lib.rs               module exports
  http.rs              Axum router, API handlers, web login/app serving
  daemon.rs            daemon lifecycle, readiness cache, leadership, metrics
  deployments.rs       deploy execution pipeline and promotion gates
  convergence.rs       steady-state reconciliation, rollback, GC/retention
  storage.rs           filesystem-backed stores, pointers, checkpoints, journals
  runtime.rs           Docker/routing traits and runtime contracts
  docker.rs            Docker CLI adapter
  caddy.rs             Caddy admin API adapter
  config.rs            `forge.conf` parsing
  secrets.rs           secret encryption and storage
  projects.rs          project registry and domain derivation
  backups.rs           persistent-volume backup and restore flow
  status.rs            status/diagnostics/history/env reporting
  upgrade.rs           signed release upgrade plan/apply/rollback
  ...
tests/
  cli.rs               CLI contract tests
  e2e.rs               dogfood end-to-end control-plane tests
  docker_integration.rs
  caddy_integration.rs
  release.rs           packaging/install/publish/upgrade tests
  integration/common.rs
  fixtures/            sample deploy targets
docs/
  ARCHITECTURE.md
  INVARIANTS.md
  OPERATIONS.md
  USAGE.md
  LOCAL_QUICKSTART.md
deploy/
  forge.conf.example
  forge.service
scripts/
  package-release.sh
  publish-release.sh
schemas/
  JSON schemas for project, deployment request, events, snapshots
web/
  static HTML/CSS/JS embedded by `src/http.rs`
openapi.yaml           HTTP API contract
dist/                  generated release artifacts
target/                Cargo build output
```

## Core Technologies

- Rust edition `2024`
- Axum for HTTP server
- Tokio runtime
- Reqwest with `rustls`
- Docker CLI integration for runtime control
- Caddy admin API integration for routing
- Serde/JSON/YAML
- AES-GCM for secret storage
- Static web assets embedded with `include_str!`

There is no monorepo/workspace structure in this repository. There is no frontend build toolchain visible; `web/` is served as static embedded assets.

## Repository-Specific Invariants

Read `docs/INVARIANTS.md` before changing deploy, convergence, rollback, pointer, snapshot, replay, or routing code.

Non-negotiable rules enforced by docs, code structure, and tests:

- Never advance `current` before validation and route activation succeed.
- `previous` must always point to the most recent superseded healthy generation only.
- Failed generations must never become rollback targets.
- `snapshot.json` is immutable once finalized.
- Queue state is not deployment truth.
- Rollback restores runtime topology, not database history.
- Restore creates a new generation and must not mutate persistent volumes in place.
- `/readyz` must stay constant-time and must not trigger live Docker/Caddy scans or fleet-wide recomputation.
- Request paths must not depend on live cross-node communication.
- Followers are read-only; replay and mutating control-plane work are leader-only.
- Docker/Caddy adapters execute commands; orchestration decisions stay in deploy/convergence/daemon layers.

## Development Workflow

Baseline local prerequisites documented in `docs/LOCAL_QUICKSTART.md` and `docs/CONTRIBUTING.md`:

- Rust/Cargo
- Docker daemon
- Caddy with admin API enabled
- Git
- `curl`

Expected workflow:

1. Read the relevant invariant and architecture docs before touching orchestration code.
2. Keep changes narrow. This codebase explicitly discourages broad cleanup/refactor work.
3. Add or update tests with every behavior change.
4. Run the smallest relevant test slice first, then the broader gates.

No repository-local CI configuration is visible in this checkout. Treat the documented local test gates as the source of truth.

## Build, Run, and Verification

Common commands:

```bash
cargo build
cargo build --release
cargo test -q
FORGE_INTEGRATION=1 cargo test -- --nocapture
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
cargo test --test cli
cargo test --test release
```

Local daemon bring-up follows `docs/LOCAL_QUICKSTART.md`:

```bash
export FORGE_CONFIG="$(pwd)/.local/forge.conf"
export FORGE_MASTER_KEY="<64-hex>"
export FORGE_CADDY_ADMIN_URL="http://127.0.0.1:2019"
export FORGE_CADDY_PUBLIC_URL="http://127.0.0.1"
export FORGE_URL="http://127.0.0.1:18080"
export FORGE_TOKEN="dev-token"

cargo build --release
./target/release/forge doctor
./target/release/forge --config "$FORGE_CONFIG" daemon
```

Use integration tests only when Docker is available. The harness explicitly skips them unless `FORGE_INTEGRATION=1`.

## Environment Configuration

### `forge.conf`

Parsed by `src/config.rs` as simple `key=value` lines.

Required keys:

- `storage_root`
- `api_bind`
- `bearer_token`

Supported optional keys visible in code:

- `release_public_key_path`
- `heartbeat_interval_ms`
- `startup_replay_max_duration_ms`
- `startup_replay_max_entries`
- `github_webhook_secret`
- `repository_cache_root`
- `sqlite_path`

### Important environment variables

Control plane and CLI:

- `FORGE_CONFIG`
- `FORGE_URL`
- `FORGE_TOKEN`
- `FORGE_MASTER_KEY`
- `FORGE_CADDY_ADMIN_URL`
- `FORGE_CADDY_PUBLIC_URL`
- `FORGE_APPS_DOMAIN`

Web/OAuth:

- `FORGE_GITHUB_OAUTH_CLIENT_ID`
- `FORGE_GITHUB_OAUTH_CLIENT_SECRET`
- `FORGE_PUBLIC_URL`
- `FORGE_SESSION_SECRET`
- `ALLOW_NEW_REGISTRATION`

CLI token signing and rotation:

- `FORGE_CLI_TOKEN_SECRET_CURRENT`
- `FORGE_CLI_TOKEN_SECRET_PREVIOUS`
- Legacy fallback still exists: `FORGE_CLI_TOKEN_SECRET`

Release/upgrade flow:

- `FORGE_RELEASE_PUBLIC_KEY`
- `FORGE_RELEASE_API_BASE_URL`
- `FORGE_RELEASE_REPOSITORY`
- `FORGE_RELEASE_PLATFORM`
- `FORGE_UPGRADE_READYZ_URL`
- `FORGE_UPGRADE_READYZ_TIMEOUT_MS`
- `FORGE_UPGRADE_READYZ_POLL_MS`
- `FORGE_SYSTEMCTL_BIN`
- `FORGE_UPGRADE_BINARY_PATH`
- `FORGE_UPGRADE_PREVIOUS_BINARY_PATH`

Do not rename or repurpose environment variables casually. They are used across runtime behavior, packaging tests, and operator workflows.

## Storage and Data Layout

The storage root is operationally sensitive. Code and docs indicate these durable artifacts matter:

- per-environment generation directories under `projects/<project>/environments/<env>/generations/<n>/`
- generation files such as `snapshot.json`, `lifecycle.json`, `probe_history.json`, `runtime_env_snapshot.json`
- pointer files such as `current` and `previous`
- control-plane files under `control_plane/`:
  - `convergence_checkpoint.json`
  - `control_plane_snapshots/*-{runtime,route,dependency}_snapshot.json`
  - `node.json`
  - `leader_lease.json`
  - `cluster_nodes.json`
  - `operations.jsonl`
  - `reconciliation_log.jsonl`
  - `reconciliation_cursor.json`
  - `quarantine/`

Never hand-edit durable state files as part of ordinary feature work. If a test or migration truly requires it, document why and add verification for crash/restart behavior.

## API and Data Flow

Contracts are described by:

- `openapi.yaml`
- `schemas/*.json`
- Axum handlers in `src/http.rs`
- API structs in `src/api.rs`

Main flow:

1. CLI/API/webhook/web requests enter through `src/main.rs` or `src/http.rs`.
2. Requests enqueue or query control-plane state.
3. Deployment execution runs through `src/deployments.rs`.
4. Runtime and route truth are maintained by `src/convergence.rs`.
5. Cached control-plane state is surfaced by `/healthz`, `/readyz`, `/metrics`, status, and diagnostics surfaces.

When modifying API payloads, update the Rust types, route handlers, tests, and `openapi.yaml` together.

## State Management Patterns

- The authoritative long-lived state is filesystem-backed, not in-memory.
- In-memory caches exist for performance, especially readiness and metrics, but durable state remains the recovery boundary.
- Atomic writes and lock discipline in `src/storage.rs` are part of correctness, not implementation detail.
- Queue, replay, pointers, checkpoints, and snapshots each carry different semantics; do not collapse them into one concept.

## Testing Strategy

The repository has meaningful test layers. Match your changes to the right layer.

- Unit/module tests inside `src/*.rs`: logic contracts and invariants
- `tests/cli.rs`: CLI parsing, output, local/remote behavior
- `tests/e2e.rs`: dogfood deploy, restart, rollback, readyz/metrics, leadership
- `tests/docker_integration.rs` and `tests/caddy_integration.rs`: real adapter behavior
- `tests/release.rs`: packaging, install, release, upgrade flow

Expected minimum verification:

- Logic-only change: `cargo test -q`
- CLI/API surface change: `cargo test --test cli`
- Deploy/convergence/runtime/routing/storage change: `cargo test -q` plus relevant `dogfood` or integration coverage
- Release/install/upgrade change: `cargo test --test release`
- Docker/Caddy behavior change: run integration gates with `FORGE_INTEGRATION=1`

## Coding Standards and Naming Conventions

- Keep authority boundaries intact:
  - orchestration decisions in daemon/deployments/convergence
  - transport in CLI/API/web
  - execution in adapters
- Prefer narrow, targeted changes. This repo explicitly rejects broad “cleanup” work.
- Preserve existing terminology: `generation`, `current`, `previous`, `snapshot`, `promotion`, `rollback`, `convergence`, `readyz`, `lease_epoch`, `follower_mode`.
- Keep environment names aligned with documented alpha scope: `development`, `staging`, `production`.
- Do not introduce alternate readiness semantics, alternate pointer semantics, or parallel deployment state machines.

## Security Guidelines

Sensitive code and docs include `src/secrets.rs`, `src/auth.rs`, `src/http.rs`, `src/events.rs`, `src/upgrade.rs`, `deploy/forge.conf.example`, and `examples/forge.env.example`.

Rules:

- Never log plaintext secrets, OAuth secrets, bearer tokens, master keys, or CLI token secrets.
- Preserve redaction behavior in events, diagnostics, logs, and status surfaces.
- `FORGE_MASTER_KEY` must remain a 64-hex secret used for secret encryption.
- Bootstrap `bearer_token` is administrative; routine remote operation should prefer CLI tokens.
- Registration gating via `ALLOW_NEW_REGISTRATION` is security-sensitive. Do not weaken default-closed behavior accidentally.
- Signed upgrade verification is part of the trust model. Do not add bypasses except explicit development flags that already exist.

Changes to authentication, token verification, redaction, OAuth flow, upgrade signature handling, or backup restore logic require human review.

## Performance Considerations

The most important performance constraint in this repository is readiness-path boundedness.

- `/healthz` is liveness only.
- `/readyz` is cached control-plane readiness only.
- `/metrics` is cached control-plane telemetry only.

Do not put any of the following on request paths:

- Docker enumeration
- Caddy enumeration
- route reconciliation
- generation reconciliation
- replay work
- cross-node communication
- deep diagnostics

If a change touches `src/daemon.rs`, `src/readiness.rs`, or `src/http.rs` around readiness/metrics, verify the behavior stays cache-backed and fail-fast.

## Observability and Diagnostics

Key operator surfaces:

- `/healthz`
- `/readyz`
- `/readiness/explain`
- `/metrics`
- `forge status`
- `forge diagnose`
- `forge control-plane leader`
- `forge control-plane lease`
- `forge control-plane replay-status`
- `forge control-plane intents`

Operational journals, checkpoints, snapshots, and readiness timelines are diagnostic artifacts. Keep them bounded, readable, and restart-safe.

## Deployment and Release Flow

Release packaging is script-driven:

- `scripts/package-release.sh`
- `scripts/publish-release.sh`
- `install.sh`

Observed release characteristics:

- signed bundles generate `dist/release-manifest.json`, `dist/release-manifest.sig`, `dist/checksums.txt`, and optionally `dist/release-public-key.pem`
- artifacts are packaged as `forge-<version>-<platform>.tar.gz`
- publish flow expects a clean git tree and `gh` authentication
- upgrade flow preserves a rollback binary (`forge.previous`) and validates `/readyz` after swap

Do not modify release scripts, installer behavior, systemd defaults, or upgrade verification casually. Changes here can break operator recovery paths.

## Unsafe / Sensitive Areas

Treat the following as high-risk:

- pointer updates in `src/storage.rs`
- promotion ordering in `src/deployments.rs`
- rollback logic in `src/convergence.rs`
- readiness cache semantics in `src/daemon.rs` and `src/readiness.rs`
- replay intent classification and cursor handling in `src/reconciliation.rs`
- auth/token/OAuth/session logic in `src/auth.rs` and `src/http.rs`
- secret sealing/redaction in `src/secrets.rs` and `src/events.rs`
- release signing and upgrade rollback in `src/upgrade.rs` and `scripts/`
- system service defaults in `deploy/forge.service`

## Common Failure Modes

Repository evidence and docs point to these recurring hazards:

- updating `current` too early
- letting `previous` reference a failed generation
- coupling readiness to expensive live inspection
- silently trusting stale or corrupt checkpoints/snapshots
- replaying destructive intents automatically
- mutating Docker/Caddy state outside Forge-owned subtrees
- leaking secrets into logs or diagnostic payloads
- changing release/install paths without matching `tests/release.rs`
- changing config/env names without updating docs/tests/installer

## AI Agent Operating Rules

- Do not edit `dist/` or `target/` by hand. They are generated outputs.
- Do not change `openapi.yaml` without changing the corresponding handler/types/tests.
- Do not change durable file names or storage layout without tracing all read/write sites and restart/replay tests.
- Do not introduce direct Docker/Caddy orchestration into HTTP handlers, CLI parsing, or web assets.
- Do not bypass atomic write helpers or lock discipline in `src/storage.rs`.
- Do not broaden refactors across multiple control-plane domains unless explicitly requested.
- Do not add dependencies without a concrete need. Prefer existing std/serde/reqwest/axum patterns first.
- Do not add background work to request paths.
- Do not change release or upgrade semantics without running `tests/release.rs`.
- Do not hand-wave restart safety. Any change to persistence, replay, leadership, or checkpoints needs restart-aware verification.

Before declaring work done:

1. Run the narrowest relevant tests.
2. Run `cargo test -q` for non-trivial Rust changes.
3. Run integration or release-specific suites when touching those areas.
4. Update docs when changing commands, env vars, invariants, or operator-visible behavior.

## Areas Requiring Human Review

- auth, OAuth, session, or token model changes
- secret encryption/redaction changes
- durable storage schema/layout changes
- replay/lease/leadership changes
- upgrade signing or rollback changes
- backup/restore semantics changes
- route ownership or Caddy subtree semantics changes
- any change that weakens fail-closed behavior

## Recommended Engineering Practices

- Start from the existing docs: `ARCHITECTURE.md`, `INVARIANTS.md`, `OPERATIONS.md`, `USAGE.md`.
- Follow existing test patterns before inventing new harnesses.
- Prefer adding a regression test before changing invariant-heavy logic.
- Preserve the distinction between deploy-time validation and steady-state convergence.
- Preserve the distinction between cached control-plane truth and deep runtime inspection.
- When uncertain, choose correctness and explicit degradation over silent recovery.

## Known Constraints and Technical Debt

Grounded repository constraints:

- single Rust crate and single shipped binary today
- single-node control plane only
- multi-node artifacts are scaffolding/detection, not distributed consensus
- Docker/Caddy are hard external dependencies for real deployments
- local `--from <path>` deploy source still exists as alpha/dev mode
- no visible repo-local CI config; verification expectations are doc-driven

Product-direction notes in docs should not be mistaken for completed migrations. In particular, the `forged` binary split is documented direction, not current repository reality.

## Quick Reference Commands

```bash
cargo build --release
cargo test -q
FORGE_INTEGRATION=1 cargo test -- --nocapture
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
./target/release/forge doctor
./target/release/forge --config /path/to/forge.conf daemon
./target/release/forge control-plane leader
./target/release/forge control-plane lease
./target/release/forge readiness explain --offline
./target/release/forge upgrade plan --release <tag>
./target/release/forge upgrade apply --release <tag>
./target/release/forge upgrade rollback
scripts/package-release.sh --sign --signing-key /path/to/key
scripts/publish-release.sh <tag> --signing-key /path/to/key
```
