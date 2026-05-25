![Forge Cover](https://testing-1355450658.cos.ap-jakarta.myqcloud.com/forge-cover3.webp)

# Forge

Deterministic deployment and runtime convergence for containerized applications on a single node.

Forge is a Rust control plane for teams that want a smaller, stricter alternative to a full platform stack. It deploys containers, validates them, activates routing, and keeps runtime state converged after restarts or drift.

The core rule is simple:

`running container != successful deployment`

A deployment only succeeds after it completes the full lifecycle:

`candidate -> validated -> finalized -> activated -> promoted`

## Why This Exists

Starting a container is easy. Operating it correctly is not.

Common failure mode:

- the image builds
- the container starts
- the app is not actually reachable
- routing points to the wrong thing
- rollback is unclear
- a restart leaves reality different from what operators think is live

Forge exists to make deployment truth explicit and enforceable. It treats deployment as a control-plane problem, not a `docker run` problem.

## What It Solves

- Prevents traffic from moving to a container that merely started but never became healthy
- Keeps `current` and `previous` generation pointers authoritative and rollback-safe
- Restores control-plane truth after daemon or host restarts from durable filesystem-backed state
- Repairs drift in runtime and route state through convergence instead of relying on one-shot deploy scripts
- Keeps readiness fast by serving cached control-plane truth instead of doing live fleet-wide inspection on request paths
- Gives operators and automation clear diagnostics when a deployment fails

## Core Features

- Deterministic deployment lifecycle with explicit promotion gates
- Single-node control plane built for Docker and Caddy
- Crash-safe, restart-safe persistence under `storage_root`
- Atomic route activation and rollback-aware generation management
- Cache-backed `/readyz` and `/metrics`
- Durable checkpoints, snapshots, and reconciliation intent logs
- Leader/follower scaffolding with lease-based single-writer semantics
- Secret redaction across logs, diagnostics, and events
- Stateful workload support with backup and restore primitives
- Operator surfaces for status, diagnostics, replay, lease, and upgrade workflows

## Why Forge Is Different

- **Correctness before convenience**: a healthy process is not enough; promotion waits for validation and route activation.
- **Convergence, not just deploys**: Forge keeps reconciling toward the intended state after the initial rollout.
- **Small operational scope**: single-node by design, not a generic multi-node scheduler.
- **Control-plane truth is durable**: state lives on disk with atomic writes, not only in memory.
- **Fast readiness semantics**: `/readyz` reports cached control-plane truth in constant time.

## Architecture

Forge keeps authority in the control plane:

- `forge` binary: operator CLI and current daemon entrypoint
- `src/deployments.rs`: deployment FSM and promotion flow
- `src/convergence.rs`: steady-state repair, rollback, retention, runtime truth
- `src/storage.rs`: durable state, pointers, checkpoints, snapshots, journals
- `src/http.rs`: API and operator-facing HTTP surfaces
- Docker: runtime substrate
- Caddy: routing substrate

Operational model:

1. A deploy request enters through CLI, API, or webhook.
2. Forge resolves source, creates a candidate generation, and starts validation.
3. Validation checks runtime reachability and health before traffic moves.
4. Route activation happens atomically.
5. Convergence keeps runtime and routing aligned with promoted truth.

Guiding principle:

`Convergence computes truth. APIs serve cached truth.`

## Quick Start

Prerequisites:

- Rust and Cargo
- Docker daemon
- Caddy with admin API enabled on `http://127.0.0.1:2019`
- `curl`

Build Forge:

```bash
cargo build --release
```

Create a minimal local config:

```bash
mkdir -p .local/var/lib/forge
cat > .local/forge.conf <<EOF
storage_root=$(pwd)/.local/var/lib/forge
api_bind=127.0.0.1:18080
bearer_token=dev-token
EOF
```

Export the required environment:

```bash
export FORGE_CONFIG="$(pwd)/.local/forge.conf"
export FORGE_MASTER_KEY="$(openssl rand -hex 32)"
export FORGE_CADDY_ADMIN_URL="http://127.0.0.1:2019"
export FORGE_CADDY_PUBLIC_URL="http://127.0.0.1"
export FORGE_URL="http://127.0.0.1:18080"
export FORGE_TOKEN="dev-token"
```

Run a local health check:

```bash
./target/release/forge doctor
```

Start the daemon:

```bash
./target/release/forge --config "$FORGE_CONFIG" daemon
```

From another shell, verify the control plane:

```bash
curl http://127.0.0.1:18080/healthz
curl http://127.0.0.1:18080/readyz
curl http://127.0.0.1:18080/metrics
```

For the full local flow, see [docs/LOCAL_QUICKSTART.md](docs/LOCAL_QUICKSTART.md).

## Example Usage

Initialize a project and deploy from a local checkout:

```bash
forge init
forge deploy api production --from .
forge status api production
forge rollback api production
```

Typical operator surfaces:

- `forge status`: lightweight runtime and environment summary
- `forge diagnose`: deep runtime inspection and failure context
- `forge control-plane leader`: current single-writer status
- `forge control-plane replay-status`: startup replay progress and health

## Who This Is For

- Operators running containerized apps on a VPS or single bare-metal host
- Teams that want stronger deployment guarantees without Kubernetes
- Builders who care about rollback correctness, restart safety, and explicit control-plane semantics
- AI-assisted software workflows that need deterministic deployment behavior instead of best-effort container startup

## Design Principles

- A running container is not a deployment success
- Promotion must be earned through validation
- Request paths stay cheap; background convergence does the expensive work
- Filesystem-backed durability is part of correctness, not an implementation detail
- Followers stay read-only; mutating control-plane work is leader-only
- Narrow scope is a feature, not a limitation

## Tech Stack

- Rust 2024
- Axum
- Tokio
- Docker CLI
- Caddy Admin API
- Serde
- Reqwest with `rustls`

## Project Status

Forge is in `alpha`.

Current validated scope:

- single Rust crate, shipping the `forge` binary
- single-node control plane
- Docker runtime orchestration
- Caddy route activation
- durable checkpoints, snapshots, and replay state
- backup/restore primitives for stateful services

Deliberate non-goals right now:

- distributed consensus
- true multi-node HA
- generic cluster scheduling

## Roadmap

Near-term focus is intentionally narrow:

- operator UX polish around status, diagnostics, history, and restore lineage
- deeper crash and recovery validation for stateful workloads
- read-only web visibility on top of the existing control-plane model

See [docs/ROADMAP.md](docs/ROADMAP.md) for the detailed milestone history and forward plan.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/INVARIANTS.md](docs/INVARIANTS.md)
- [docs/USAGE.md](docs/USAGE.md)
- [docs/LOCAL_QUICKSTART.md](docs/LOCAL_QUICKSTART.md)
- [openapi.yaml](openapi.yaml)

## Contributing

Forge favors narrow, correctness-first contributions.

Before opening a PR:

- read [docs/INVARIANTS.md](docs/INVARIANTS.md) if you are touching deployment, convergence, rollback, routing, replay, or storage code
- keep the change focused on one concern
- add or update tests with every behavior change
- run `cargo test -q` at minimum

Start here: [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md)

## License

[MIT](LICENSE)
