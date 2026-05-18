TODO.md

# Forge TODO

Current baseline: CLI implemented.  
Goal: reach usable alpha without scope creep.

---

## Current Completed Baseline

- [x] Core architecture defined
- [x] Implementation spec defined
- [x] Storage primitives
- [x] Immutable generation snapshots
- [x] Atomic current/previous pointers
- [x] Generation allocator
- [x] Persistent queue
- [x] Daemon bootstrap skeleton
- [x] HTTP API
- [x] CLI wrapper
- [x] Docker runtime adapter
- [x] Caddy routing adapter
- [x] Deploy-time TCP/HTTP validation
- [x] Snapshot finalization
- [x] Route activation
- [x] Rollback semantics
- [x] Steady-state convergence engine
- [x] Events and diagnostics
- [x] Secret injection and redaction
- [x] GitHub webhook trigger path
- [x] Dogfood E2E proofs

---

## Phase 1: Stabilize Current Alpha Baseline

- [ ] Commit clean CLI baseline
- [ ] Verify all tests pass

```bash
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture

	•	Remove or silence harmless warnings
	•	Ensure README.md matches actual current state
	•	Ensure ARCHITECTURE.md matches actual current state
	•	Add this TODO.md

⸻

Phase 2: Operational Visibility

Do this in narrow slices.

2.1 Metrics
	•	Add minimal metrics registry
	•	Expose GET /metrics
	•	Output Prometheus text format
	•	Track:
	•	deployments total
	•	failed deployments total
	•	rollback total
	•	queue depth
	•	probe failures
	•	convergence transitions

Tests:
	•	metrics_endpoint_exposes_prometheus_text
	•	metrics_increment_on_deploy_failure
	•	metrics_report_queue_depth

Rules:
	•	No convergence semantic changes
	•	No Docker/Caddy trait changes
	•	No logs/SSE in this slice

⸻

2.2 Bounded Logs
	•	Add bounded persisted deployment log excerpts
	•	Expose GET /logs/:deployment_id
	•	Redact secret values before persistence/delivery
	•	Enforce max retained log size

Tests:
	•	logs_endpoint_redacts_secret_values
	•	logs_endpoint_is_bounded
	•	failed_deploy_logs_preserve_diagnostic_context

Rules:
	•	No docker logs -f
	•	No SSE yet
	•	No unbounded streaming

⸻

2.3 Doctor Command
	•	Add forge doctor
	•	Check Docker availability
	•	Check Caddy availability
	•	Check storage root
	•	Check FORGE_MASTER_KEY
	•	Return clear diagnostic output

Tests:
	•	doctor_reports_docker_unavailable
	•	doctor_reports_caddy_unavailable
	•	doctor_reports_missing_master_key

⸻

Phase 3: Real Dogfooding

Goal: validate product thesis, not add features.
	•	Generate 5 AI-created sample apps
	•	Add forge.project.json
	•	Deploy via GitHub webhook
	•	Confirm route live
	•	Confirm events visible
	•	Confirm rollback works

Track:
	•	first deploy success rate
	•	manual infra fixes required
	•	failure reasons
	•	missing contract assumptions

Success target:

AI-generated app deploys with near-zero manual infrastructure repair


⸻

Phase 4: Runtime Hardening
	•	Crash during build recovery
	•	Crash during validation recovery
	•	Crash during route activation recovery
	•	Crash during rollback recovery
	•	Orphaned container cleanup
	•	Orphaned route cleanup
	•	Tombstone retry loop
	•	Disk pressure handling
	•	Docker unavailable recovery
	•	Caddy unavailable recovery

Tests:
	•	crash_during_build_recovers
	•	crash_during_route_activation_recovers
	•	orphaned_container_is_tombstoned
	•	orphaned_route_is_removed
	•	cleanup_retry_eventually_succeeds

⸻

Phase 5: Installation UX
	•	Add forge init
	•	Generate basic forge.project.json
	•	Generate local example config
	•	Add install instructions
	•	Add local development quickstart
	•	Add VPS setup guide
	•	Add GitHub webhook setup guide
	•	Add Caddy setup guide

⸻

Phase 6: Minimal Dashboard

Only after CLI and dogfood workflow are stable.

Dashboard should show:
	•	projects
	•	environments
	•	active generation
	•	deployment history
	•	events
	•	diagnostics
	•	rollback button
	•	secret references, not values

Do NOT build:
	•	analytics
	•	multi-service visual graph
	•	RBAC
	•	team management
	•	preview environment UI

⸻

Phase 7: AI Runtime Contract UX
	•	forge contract export
	•	forge contract validate
	•	forge context claude
	•	forge context cursor
	•	Generate AI-ready runtime rules
	•	Validate generated app against runtime contract before deploy

Success target:

AI agent can read Forge context and generate deployable app first try


⸻

Deferred Explicitly

Do not build yet:
	•	Kubernetes support
	•	multi-node orchestration
	•	distributed queue
	•	RBAC
	•	teams
	•	preview environments
	•	persistent volumes
	•	UDP workloads
	•	worker workloads
	•	service mesh
	•	plugin system
	•	AI auto-remediation

⸻

Agent Safety Rules

Before accepting any agent patch:

git diff --stat
git diff
cargo test -q
FORGE_INTEGRATION=1 cargo test dogfood -- --nocapture

Reject patch if it:
	•	changes convergence semantics unexpectedly
	•	changes pointer semantics
	•	changes Docker/Caddy trait boundaries unnecessarily
	•	adds broad refactors
	•	introduces unbounded logs/streams
	•	changes deployment activation ordering
	•	weakens rollback invariants

Core invariant:

candidate
→ validated
→ finalized
→ activated
→ promoted

Never break this.

```
