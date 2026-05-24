use crate::api::{MetricsResponse, ReadinessExplainResponse, ReadyzResponse};
use crate::daemon::{ControlPlaneSnapshot, READYZ_CACHE_STALE_AFTER_MS};

pub fn explain_snapshot(snapshot: &ControlPlaneSnapshot) -> ReadinessExplainResponse {
    explain(&snapshot.readyz.response, &snapshot.metrics)
}

pub fn explain(readyz: &ReadyzResponse, metrics: &MetricsResponse) -> ReadinessExplainResponse {
    let readiness_status = if readyz.status.is_empty() {
        "degraded".to_string()
    } else {
        readyz.status.clone()
    };
    let startup_phase = if readyz.startup_phase.is_empty() {
        metrics.startup_phase.clone()
    } else {
        readyz.startup_phase.clone()
    };
    let active_failure = readyz.active_failure;
    let active_failure_reason = metrics
        .convergence_active_failure_reason
        .clone()
        .or_else(|| readyz.reason.clone())
        .or_else(|| readyz.reasons.first().map(|reason| reason.message.clone()));
    let historical_failures =
        !active_failure && metrics.convergence_last_failure_historical_unix.is_some();
    let failure_scope = if active_failure {
        "active".to_string()
    } else if historical_failures {
        "historical".to_string()
    } else {
        "none".to_string()
    };
    let replay_running = metrics.replay_in_progress;
    let node_role = node_role(metrics);
    let leadership_uncertain = leadership_uncertain(metrics);
    let leadership_healthy = !leadership_uncertain
        && (metrics.leader || metrics.follower_mode || node_role == "candidate");
    let leadership_status = if leadership_uncertain {
        "uncertain".to_string()
    } else if metrics.leader {
        "active_leader".to_string()
    } else if metrics.follower_mode {
        "follower".to_string()
    } else {
        "candidate".to_string()
    };
    let convergence_blocked = metrics.convergence_start_blocked
        || leadership_uncertain
        || metrics.follower_mode
        || (!metrics.reconciliation_enabled
            && !metrics.leader
            && readiness_status != "ready"
            && startup_phase != "booting");
    let taxonomy = taxonomy(
        &readiness_status,
        active_failure,
        &active_failure_reason,
        metrics,
        leadership_uncertain,
    );
    let last_historical_failure_unix = if historical_failures {
        metrics.convergence_last_failure_historical_unix
    } else {
        None
    };
    let (operator_interpretation, safe_next_action) =
        operator_text(&taxonomy, &active_failure_reason, metrics);

    ReadinessExplainResponse {
        taxonomy,
        readiness_status,
        startup_phase,
        active_failure,
        active_failure_reason,
        failure_scope,
        historical_failures,
        convergence_blocked,
        replay_running,
        leader: metrics.leader,
        follower_mode: metrics.follower_mode,
        node_role,
        leadership_healthy,
        leadership_status,
        last_successful_convergence_unix: metrics.convergence_last_success_unix,
        last_historical_failure_unix,
        operator_interpretation,
        safe_next_action,
    }
}

fn taxonomy(
    readiness_status: &str,
    active_failure: bool,
    active_failure_reason: &Option<String>,
    metrics: &MetricsResponse,
    leadership_uncertain: bool,
) -> String {
    let cache_stale = metrics.readiness_cache_age_ms > READYZ_CACHE_STALE_AFTER_MS
        || active_failure_reason.as_deref() == Some("readiness cache stale");
    if readiness_status == "ready" && !active_failure {
        return "ready_no_active_failure".into();
    }
    if cache_stale {
        return "degraded_cache_stale".into();
    }
    if metrics.follower_mode || metrics.startup_phase == "follower" {
        return "degraded_follower_mode".into();
    }
    if leadership_uncertain {
        return "degraded_leadership_uncertain".into();
    }
    if metrics.replay_in_progress
        || metrics.replay_paused
        || metrics.startup_phase == "replaying"
        || (metrics.convergence_start_blocked && metrics.pending_intents > 0)
    {
        return "degraded_replay_incomplete".into();
    }
    if !metrics.reconciliation_enabled && !metrics.leader {
        return "degraded_convergence_disabled".into();
    }
    if active_failure {
        return "degraded_active_convergence_failure".into();
    }
    "degraded_unknown".into()
}

fn operator_text(
    taxonomy: &str,
    active_failure_reason: &Option<String>,
    metrics: &MetricsResponse,
) -> (String, String) {
    match taxonomy {
        "ready_no_active_failure" => {
            let interpretation = if metrics.convergence_last_failure_historical_unix.is_some() {
                "Control-plane readiness is healthy. Historical failures exist, but there is no active blocker.".into()
            } else {
                "Control-plane readiness is healthy and convergence is operating normally.".into()
            };
            (interpretation, "no action required".into())
        }
        "degraded_active_convergence_failure" => (
            format!(
                "Control-plane readiness is degraded by an active convergence blocker{}.",
                active_failure_reason
                    .as_deref()
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default()
            ),
            next_action_for_reason(active_failure_reason),
        ),
        "degraded_replay_incomplete" => (
            "Control-plane readiness is degraded because startup replay has not completed, so convergence remains blocked.".into(),
            "allow replay to complete, then inspect reconciliation replay status if it stays blocked".into(),
        ),
        "degraded_follower_mode" => (
            "This node is a read-only follower. Convergence is not expected to run here.".into(),
            "query the active leader for writable control-plane actions; no local repair action is required on this follower".into(),
        ),
        "degraded_leadership_uncertain" => (
            "Control-plane leadership is uncertain, so readiness cannot assert a healthy active reconciler.".into(),
            "inspect leader lease state and cluster topology before taking recovery action".into(),
        ),
        "degraded_convergence_disabled" => (
            "Convergence is currently disabled, so readiness cannot confirm normal recovery progress.".into(),
            "inspect replay state and leader lease health before attempting any operator intervention".into(),
        ),
        "degraded_cache_stale" => (
            "The cached readiness view is stale, so this explanation may not reflect current control-plane truth.".into(),
            "wait for the next cache refresh or inspect the daemon if cache staleness persists".into(),
        ),
        _ => (
            "Control-plane readiness is degraded, but the current cached state does not map cleanly to a known operator explanation class.".into(),
            "inspect readiness reasons, replay status, and leader lease health".into(),
        ),
    }
}

fn next_action_for_reason(active_failure_reason: &Option<String>) -> String {
    match active_failure_reason.as_deref() {
        Some(reason) if reason.contains("route") || reason.contains("verification") => {
            "inspect route diagnostics and Caddy admin health".into()
        }
        Some(reason) if reason.contains("docker") => {
            "inspect Docker dependency health and cached breaker diagnostics".into()
        }
        Some(reason) if reason.contains("leadership") || reason.contains("lease") => {
            "inspect leader lease state and cluster topology".into()
        }
        Some(reason) if reason.contains("replay") => {
            "inspect reconciliation replay status and blocked intents".into()
        }
        _ => "inspect cached readiness reasons and environment diagnostics".into(),
    }
}

fn node_role(metrics: &MetricsResponse) -> String {
    if !metrics.cluster.local_role.is_empty() {
        return metrics.cluster.local_role.clone();
    }
    if metrics.leader {
        "leader".into()
    } else if metrics.follower_mode {
        "follower".into()
    } else {
        "candidate".into()
    }
}

fn leadership_uncertain(metrics: &MetricsResponse) -> bool {
    metrics.cluster.local_role == "uncertain"
        || metrics.cluster.split_brain_suspected
        || metrics.cluster.multiple_active_reconcilers
        || metrics.cluster.lease_epoch_divergence
        || metrics.cluster.checkpoint_owner_mismatch
        || metrics.cluster.snapshot_owner_mismatch
        || metrics.cluster.stale_reconciler
        || metrics.cluster.degraded_markers.iter().any(|marker| {
            matches!(
                marker.as_str(),
                "split_brain_suspected" | "lease_epoch_divergence" | "stale_reconciler"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{ClusterDiagnostics, MetricsDependencySnapshot};

    fn base_readyz() -> ReadyzResponse {
        ReadyzResponse {
            status: "ready".into(),
            startup_phase: "leader_active".into(),
            active_failure: false,
            reason: None,
            reasons: Vec::new(),
        }
    }

    fn base_metrics() -> MetricsResponse {
        MetricsResponse {
            readiness_status: "ready".into(),
            startup_phase: "leader_active".into(),
            convergence_last_success_unix: Some(100),
            readiness_cache_age_ms: 5,
            leader: true,
            convergence_owner: "node-a".into(),
            reconciliation_enabled: true,
            node: None,
            docker: MetricsDependencySnapshot::default(),
            caddy: MetricsDependencySnapshot::default(),
            cluster: ClusterDiagnostics {
                local_role: "leader".into(),
                ..ClusterDiagnostics::default()
            },
            ..MetricsResponse::default()
        }
    }

    #[test]
    fn ready_with_historical_failures_requires_no_action() {
        let readyz = base_readyz();
        let mut metrics = base_metrics();
        metrics.convergence_last_failure_historical_unix = Some(90);
        let response = explain(&readyz, &metrics);
        assert_eq!(response.taxonomy, "ready_no_active_failure");
        assert!(response.historical_failures);
        assert_eq!(response.failure_scope, "historical");
        assert_eq!(response.safe_next_action, "no action required");
    }

    #[test]
    fn degraded_active_failure_explains_blocker() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.active_failure = true;
        readyz.reason = Some("route_activation_verification_failed".into());
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.convergence_active_failure = true;
        metrics.convergence_active_failure_reason =
            Some("route_activation_verification_failed".into());
        let response = explain(&readyz, &metrics);
        assert_eq!(response.taxonomy, "degraded_active_convergence_failure");
        assert_eq!(response.failure_scope, "active");
        assert!(
            response
                .operator_interpretation
                .contains("active convergence blocker")
        );
    }

    #[test]
    fn replaying_state_explains_replay_incomplete() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.startup_phase = "replaying".into();
        readyz.active_failure = true;
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.startup_phase = "replaying".into();
        metrics.replay_in_progress = true;
        metrics.convergence_start_blocked = true;
        let response = explain(&readyz, &metrics);
        assert_eq!(response.taxonomy, "degraded_replay_incomplete");
        assert!(response.replay_running);
    }

    #[test]
    fn follower_mode_explains_read_only_follower() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.startup_phase = "follower".into();
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.startup_phase = "follower".into();
        metrics.leader = false;
        metrics.reconciliation_enabled = false;
        metrics.follower_mode = true;
        metrics.cluster.local_role = "follower".into();
        let response = explain(&readyz, &metrics);
        assert_eq!(response.taxonomy, "degraded_follower_mode");
        assert!(response.safe_next_action.contains("active leader"));
    }

    #[test]
    fn stale_cache_explains_cache_stale() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.active_failure = true;
        readyz.reason = Some("readiness cache stale".into());
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.readiness_cache_age_ms = READYZ_CACHE_STALE_AFTER_MS + 1;
        metrics.convergence_active_failure = true;
        metrics.convergence_active_failure_reason = Some("readiness cache stale".into());
        let response = explain(&readyz, &metrics);
        assert_eq!(response.taxonomy, "degraded_cache_stale");
        assert!(response.operator_interpretation.contains("stale"));
    }
}
