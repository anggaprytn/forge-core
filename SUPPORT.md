# Forge Support Policy

Forge is currently in:

```txt
alpha
```

The project is operationally functional but still evolving.

Support is best-effort and focused on:

- runtime correctness
- convergence behavior
- operational safety

---

# Supported Scope

Current support focus:

- deployment failures
- rollback behavior
- convergence issues
- restart recovery
- Docker integration
- Caddy integration
- runtime contracts
- secret injection and redaction
- webhook deployment flow
- diagnostics and events
- dogfood/runtime validation

---

# Not Yet Supported

Forge intentionally does not support:

- Kubernetes clusters
- multi-node orchestration
- Windows runtimes
- service meshes
- distributed queues
- enterprise RBAC
- multi-tenant isolation
- plugin ecosystems
- persistent distributed volumes

These are not alpha priorities.

---

# Before Reporting Issues

Always verify:

```bash
cargo test -q
```

If runtime-related:

```bash
FORGE_INTEGRATION=1 cargo test -- --nocapture
```

If convergence or runtime-sensitive:

```bash
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

---

# Required Debug Information

When reporting an issue, include:

- Forge version or commit SHA
- OS and Docker version
- Caddy version
- exact command executed
- deployment ID, if relevant
- relevant events
- diagnostics output
- reproduction steps

Helpful files:

```txt
runtime_state.json
events.jsonl
diagnostics/
cleanup.json
```

---

# Important Operational Rule

Do NOT manually mutate:

- finalized snapshots
- current pointer
- previous pointer
- Forge-owned Caddy subtree

unless performing explicit disaster recovery.

Manual runtime edits can invalidate convergence assumptions.

---

# Recovery Expectations

Forge is designed to recover from:

- partial deployments
- daemon crashes
- failed route activations
- unhealthy generations
- orphaned containers or routes

If runtime state diverges unexpectedly, report:

- expected behavior
- observed behavior
- runtime artifacts

---

# Security Issues

Do not open public issues for:

- secret leakage
- authentication bypass
- unsafe runtime escalation
- convergence corruption vulnerabilities

Report privately instead.

---

# Secret Handling

Never include real secret values in:

- GitHub issues
- logs
- screenshots
- diagnostics uploads

Forge should redact secrets automatically, but operators should still avoid posting sensitive values publicly.

---

# AI Agent Contributions

Forge supports AI-assisted development, but:

- AI-generated patches are not automatically trusted
- all runtime changes require tests
- invariants must remain preserved

Before submitting AI-generated changes:

```bash
git diff --stat
cargo test -q
```

---

# Support Philosophy

Forge prioritizes:

```txt
correctness
recoverability
determinism
```

over feature velocity.

A smaller stable runtime is preferred over a broader fragile platform.

---

# What Good Bug Reports Look Like

Good reports include:

```txt
- exact reproduction
- deployment lifecycle
- expected invariant
- actual invariant violation
- minimal reproduction
```

Bad reports look like:

```txt
"deploy broken"
"routing weird"
"rollback failed"
```

without runtime context.

---

# Operational Red Flags

Report immediately if you observe:

- current points to an invalid generation
- a failed generation becomes active
- secret values appear in logs or events
- routes diverge permanently from current
- generation reuse
- rollback activates an unhealthy generation
- orphaned routes accumulate
- convergence oscillation loops

These are invariant violations.

---

# Alpha Expectations

Forge is still alpha software.

Expect:

- rapid iteration
- runtime hardening work
- evolving APIs
- evolving operational tooling

Do not assume long-term API stability yet.

---

# Contribution Expectations

Preferred contributions:

- invariant tests
- runtime hardening
- deterministic recovery improvements
- observability
- operational tooling
- convergence correctness

Avoid:

- broad platformization
- speculative abstractions
- enterprise feature creep
- unnecessary refactors

---

# Long-Term Goal

Forge exists to make:

```txt
AI-generated applications operationally convergent
```

without requiring constant manual infrastructure repair.

Support decisions are guided by that goal.
