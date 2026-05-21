# Forge Support Policy

Forge is currently in **Alpha**. While the core runtime is stable and validated, the system is evolving rapidly. Support is provided on a best-effort basis with a focus on **Operational Invariants**.

---

## 🚦 Triage Tiers

Issues are prioritized based on their impact on system correctness:

| Tier | Type | Priority | Description |
| :--- | :--- | :--- | :--- |
| **Tier 1** | **Invariant Violation** | **Immediate** | Cases where `candidate → finalized` is bypassed, or pointers diverge from route truth. |
| **Tier 2** | **Recovery Failure** | **High** | Daemon or host restarts that result in non-deterministic or manual recovery. |
| **Tier 3** | **Security/Redaction** | **High** | Secret leakage in logs, events, or diagnostics. |
| **Tier 4** | **Convergence Bug** | **Medium** | Routes not reconciling toward `current`, or unhealthy containers remaining active. |
| **Tier 5** | **General/Feature** | **Low** | UX improvements, new adapters, or CLI enhancements. |

---

## 🛑 Out of Scope

We will instantly close issues requesting:
- Support for non-Docker container runtimes.
- Support for Windows or non-Unix host operating systems.
- Multi-node / Cluster orchestration features.
- Kubernetes integration or service meshes.
- Enterprise features like RBAC, Teams, or SSO.

---

## 📝 Reporting an Issue

To ensure a Tier 1–3 issue is addressed, you **must** provide:
1. **Forge Version:** (e.g., `git rev-parse HEAD`)
2. **Runtime Context:** Output of `forge diagnose` or `forge doctor`.
3. **The Violation:** Which specific invariant from `INVARIANTS.md` was broken?
4. **Reproduction:** A minimal set of steps to trigger the failure.

### Required Artifacts
- `runtime_state.json` (redacted)
- `events.jsonl`
- Docker & Caddy versions

---

## 🔒 Security Vulnerabilities

> [!CAUTION]
> **Do not open public issues for security vulnerabilities.**

If you discover a way to bypass authentication, escalate privileges, or leak plaintext secrets, please report it via the private security channel (see GitHub Security tab).

---

## 🤖 AI-Generated Issues

We welcome reports from AI agents, provided they include:
- A reproduction test case in Rust.
- A clear explanation of which state transition failed.
- A summary of the observed vs. expected convergence behavior.

---

## 💡 Support Philosophy

Forge prioritizes **Correctness > Recoverability > Determinism**. Support decisions are guided by these priorities. We would rather have a stable, narrow system that recovers perfectly than a broad platform that requires manual surgery.
