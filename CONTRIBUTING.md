# Contributing to Forge

Forge is deterministic runtime orchestration software.

Correctness matters more than feature velocity.

Before contributing, read:

- `README.md`
- `ARCHITECTURE.md`
- `INVARIANTS.md`
- `OPERATIONS.md`
- `TODO.md`

Most implementation mistakes come from violating orchestration invariants.

---

# Core Philosophy

Forge is not:

```txt id="mxeb2x"
container automation
```

Forge is:

```txt id="nrz3pn"
runtime convergence software
```

A running container is not a successful deployment.

Do not weaken this distinction.

---

# Contribution Rules

These rules apply to:

- humans
- AI agents
- copilots
- automated refactoring tools

---

# 1. Run Tests Before Every Commit

Minimum required:

```bash id="kltmzh"
cargo test -q
```

Integration baseline:

```bash id="mjlwm6"
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

Do not submit patches without running tests.

---

# 2. Preserve Core Invariants

Never violate:

```txt id="7g7i6o"
candidate
→ validated
→ finalized
→ activated
→ promoted
```

Rules:

- never promote before validation
- never finalize invalid generations
- never bypass route verification
- never weaken rollback ordering

Read `INVARIANTS.md` before touching:

- convergence
- rollback
- deployment executor
- pointers
- routing
- snapshots

---

# 3. No Broad Refactors

Forbidden contribution style:

```txt id="pryqtx"
large cleanup
architecture rewrite
trait redesign
async rewrite
runtime abstraction overhaul
```

Forge intentionally evolves in narrow slices.

Large refactors hide semantic regressions.

Prefer:

```txt id="jlwmft"
small focused changes
```

---

# 4. Small PRs Only

Target:

```txt id="9fknji"
1 concern per PR
```

## Good

- metrics endpoint only
- doctor command only
- bounded logs only
- single rollback fix

## Bad

- "observability overhaul"
- "runtime cleanup"
- "deployment improvements"

---

# 5. Runtime Changes Require Regression Tests

Any change touching:

- deployment ordering
- rollback
- convergence
- routing
- snapshots
- restart recovery
- cleanup
- secrets

must include tests.

No exceptions.

---

# 6. Preserve Authority Boundaries

Forge owns orchestration authority.

```txt id="g2d0h5"
Docker = execution runtime
Caddy  = routing layer
Forge  = orchestration authority
```

Do not move orchestration logic into:

- Docker
- Caddy
- CLI
- HTTP handlers

---

# 7. CLI Must Stay Thin

The CLI is an HTTP wrapper only.

Do not duplicate business logic in the CLI.

---

# 8. API Must Stay Thin

HTTP handlers should only:

- validate request
- delegate work
- return response

Do not place orchestration semantics in handlers.

---

# 9. No Hidden Runtime State

Avoid:

- hidden globals
- implicit retries
- silent recovery
- magic background tasks

Runtime state must remain reconstructable from:

- snapshots
- pointers
- runtime inspection
- routes
- events

---

# 10. No Unbounded Streams

Forbidden without explicit design review:

- unbounded logs
- infinite queues
- uncontrolled buffering
- unlimited retries

Operational safety matters more than convenience.

---

# 11. Secrets Must Never Leak

Secret values must never appear in:

- logs
- diagnostics
- events
- manifests
- CLI output
- API responses

Secret names may appear.

Always redact before persistence or delivery.

---

# 12. Snapshot Integrity Is Critical

Snapshots are rollback authority.

Never:

- mutate finalized snapshots
- partially finalize snapshots
- bypass atomic writes

---

# 13. Preserve Pointer Semantics

`current` means:

```txt id="i9mcr5"
intended active generation
```

Routes reconcile toward `current`.

Do not reverse this relationship.

---

# 14. Avoid Semantic Drift

Most dangerous changes:

- changing ordering
- changing authority boundaries
- changing recovery semantics
- changing convergence behavior

These often appear harmless but break operational guarantees.

---

# 15. AI Agent Rules

AI-generated patches must be:

- narrow
- test-backed
- locally verified

Before accepting AI-generated changes:

```bash id="vbbf8y"
git diff --stat
git diff
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture
```

Reject patches that:

- touch unrelated files
- introduce broad abstractions
- change trait boundaries unnecessarily
- modify convergence semantics unexpectedly
- introduce unbounded behavior

---

# 16. Commit Discipline

Recommended workflow:

```bash id="dzq0hq"
git checkout -b small-feature-slice
```

After successful tests:

```bash id="szyatq"
git commit -m "Add metrics endpoint"
```

Do not stack unrelated changes into one commit.

---

# 17. Preferred Development Style

## Good

- narrow slices
- explicit invariants
- deterministic behavior
- small commits
- tests first

## Bad

- magic
- implicit behavior
- large rewrites
- framework-driven architecture

---

# 18. Runtime Correctness > Features

Forge prioritizes:

```txt id="y0vvpi"
operational correctness
```

over feature breadth.

A smaller correct system is preferred over a larger fragile one.

---

# 19. If Unsure, Preserve Existing Semantics

When uncertain:

- preserve ordering
- preserve invariants
- preserve authority boundaries
- preserve rollback semantics

Do not casually "simplify" orchestration behavior.

---

# 20. Most Important Rule

Never break:

```txt id="j17mym"
running container != successful deployment
```

That distinction is the foundation of Forge.
