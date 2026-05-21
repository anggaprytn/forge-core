# Contributing to Forge

Forge is deterministic runtime orchestration software where **correctness overrides velocity**. This guide ensures that all contributions—whether from humans or AI—preserve the system's operational integrity.

---

## 🚀 1. Setup & Environment

### Prerequisites
- Rust (2024 Edition)
- Docker (Running)
- Caddy (Installed/Available in path)

### Local Baseline
Before making changes, ensure your environment is healthy:
```bash
cargo test -q
```

---

## 🌿 2. Branching & Workflow

### Branch Naming
Follow these prefixes for all branches:
- `feat/` - New capabilities (e.g., `feat/metrics-endpoint`)
- `fix/` - Bug fixes (e.g., `fix/rollback-pointer`)
- `docs/` - Documentation updates
- `ref/` - Narrow, approved refactors (use with caution)

### Workflow
1. Create a branch from `main`.
2. Implement a **single concern** (narrow slice).
3. Add tests that specifically target the new behavior or fix.
4. Verify all test gates pass (see `CLAUDE.md`).

---

## 💻 3. Coding Standards

### Core Invariants
> [!IMPORTANT]
> Most architectural failures in Forge stem from violating orchestration invariants. You **must** read `INVARIANTS.md` before touching the convergence engine, rollback logic, or pointer management.

### The "Narrow Slice" Rule
We do not accept broad refactors or "cleanup" PRs.
- **Good:** "Add Prometheus metrics for queue depth"
- **Bad:** "Modernize the runtime architecture"

### Authority Boundaries
Logic must remain in the **Orchestrator**.
- Adapters (Docker/Caddy) translate and execute; they do not decide.
- CLI/API handle transport; they do not orchestrate.

---

## 📤 4. PR Submission Checklist

Before opening a PR, ensure you can check off every item:

- [ ] **Tests Added:** Every functional change includes a corresponding test.
- [ ] **Test Gates Green:** `cargo test -q` and `FORGE_INTEGRATION=1 cargo test -- --nocapture` pass locally.
- [ ] **No Invariant Violations:** Changes preserve the `candidate → validated → finalized` sequence.
- [ ] **Secrets Protected:** No plaintext secrets in logs, diagnostics, or events.
- [ ] **Commit Messages:** Follow [Conventional Commits](https://www.conventionalcommits.org/).
- [ ] **Single Concern:** The PR addresses exactly one issue or feature.

---

## 🔍 5. The Review Cycle

1. **Automated Check:** CI will run the test gates.
2. **Invariant Audit:** Reviewers will specifically look for drift in convergence semantics or pointer authority.
3. **Redaction Check:** Verification that new logs or events do not leak sensitive data.
4. **Approval:** Once invariants are confirmed and tests pass, the PR will be merged.

---

## 🤖 AI Agent Guidelines
If you are an AI assistant contributing to this repo:
1. **Be Surgical:** Minimize the number of modified files.
2. **Be Explicit:** State which invariants you are preserving in your PR description.
3. **Be Defensive:** Always include a reproduction test for bug fixes.

---

## ⚖️ Governance
Forge prioritizes a **small, correct system** over a large, fragile platform. If a proposed feature adds significant complexity without advancing deterministic convergence, it may be rejected or deferred.
