# Forge | AI Assistant Profile

Forge is a deterministic runtime convergence platform for AI-generated applications. Infrastructure-grade correctness is the absolute priority.

## 🛠 Operational Commands

### Build & Lint
- **Build:** `cargo build`
- **Check:** `cargo check`
- **Format:** `cargo fmt`
- **Lint:** `cargo clippy --all-targets --all-features -- -D warnings`

### Test Gates (Mandatory)
- **Unit/Standard:** `cargo test -q`
- **Integration (Docker/Caddy):** `FORGE_INTEGRATION=1 cargo test -- --nocapture`
- **Dogfood (E2E):** `FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture`

> [!CAUTION]
> If any test gate fails, **stop immediately**. Do not attempt to add features or refactor until the baseline is restored.

---

## 🏗 Core Invariants (Non-Negotiable)

1. **State Transition:** `candidate → validated → finalized → activated → promoted`. Never skip or weaken this sequence.
2. **Authority:** Forge owns orchestration; Docker is execution-only; Caddy is routing-only.
3. **Pointers:** `current` = intended active generation. Routes must reconcile toward `current`.
4. **Validation:** `running container != successful deployment`. Activation requires verified health.
5. **Safety:** Secrets must be redacted before persistence or delivery (Logs, API, CLI, Events).

---

## 💻 Implementation Rules

### Code Style & Patterns
- **Rust:** Idiomatic 2024 edition. Prefer `Result` over `unwrap`/`expect`.
- **Concurrency:** Prefer `tokio` primitives. Avoid unbounded channels/buffers.
- **Traits:** Keep boundaries stable. Do not redesign traits without explicit instruction.
- **Error Handling:** Use domain-specific error types. Preserve causal chains.
- **Slices:** Implement in narrow, functional slices (e.g., "Add GET /metrics", not "Overhaul Observability").

### Authority Boundaries
- **Adapters (Docker/Caddy):** Logic must be limited to translation/execution. No orchestration state.
- **CLI/API:** Thin wrappers only. No business logic duplication.

### Prohibited Actions
- **No Broad Refactors:** Do not "cleanup" unrelated code.
- **No Unbounded Streams:** No infinite logs/queues/retries.
- **No Premature Complexity:** No Kubernetes-style abstractions, RBAC, or multi-node logic.

---

## 🔒 Security & Secrets
- **Redaction:** Any string potentially containing a secret must pass through a redaction filter before logging or API return.
- **Persistence:** Never store raw secret values in snapshots or manifests.

---

## 🧪 Patch Discipline
1. **Research:** Read `INVARIANTS.md`, `ARCHITECTURE.md`, and `OPERATIONS.md`.
2. **Reproduce:** Confirm bugs with a test case before fixing.
3. **Surgical:** One concern per patch. Keep diffs minimal.
4. **Verify:** Run all Test Gates. Check `git diff --stat` for unintended changes.

---

## 📈 Current Roadmap Focus
1. **Metrics:** Prometheus text output, deployment/failure counters.
2. **Logs:** Bounded, redacted, persisted deployment log excerpts.
3. **Diagnostics:** `forge doctor` enhancements.
4. **Hardening:** Crash recovery during deployment/activation.
