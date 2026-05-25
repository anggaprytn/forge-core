use crate::api::{
    MetricsResponse, ReadinessExplainResponse, ReadinessRecommendation, ReadinessSummary,
    ReadinessTimelineEntry, ReadinessTimelineRelatedFields, ReadinessTimelineResponse,
    ReadyzResponse,
};
use crate::daemon::{ControlPlaneSnapshot, READYZ_CACHE_STALE_AFTER_MS};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveReadinessSnapshot {
    pub readyz: ReadyzResponse,
    pub metrics: MetricsResponse,
}

pub fn explain_snapshot(snapshot: &ControlPlaneSnapshot) -> ReadinessExplainResponse {
    let effective = effective_snapshot(snapshot);
    explain(&effective.readyz, &effective.metrics)
}

pub fn timeline_snapshot(snapshot: &ControlPlaneSnapshot) -> ReadinessTimelineResponse {
    if unix_now_ms().saturating_sub(snapshot.readyz.updated_at_unix_ms)
        <= READYZ_CACHE_STALE_AFTER_MS
    {
        return snapshot.timeline.clone();
    }
    let mut timeline = snapshot.timeline.clone();
    let recommendation = recommendation_for_cache_stale(false);
    let cache_stale = ReadinessTimelineEntry {
        timestamp_unix: unix_now_ms() / 1_000,
        status: "active".into(),
        blocker_type: "cache".into(),
        reason: "readiness cache stale".into(),
        startup_phase: "degraded".into(),
        source: "daemon_api".into(),
        active_failure: true,
        suggested_action: recommendation.title.clone(),
        recommendation: Some(recommendation),
        related_fields: Some(timeline_related_fields(&snapshot.metrics, None)),
    };
    timeline.source = "daemon_api".into();
    timeline.live = true;
    timeline.generated_at_unix = cache_stale.timestamp_unix;
    timeline.entries.retain(|entry| {
        !(entry.status == "active"
            && entry.blocker_type == "cache"
            && entry.reason == "readiness cache stale")
    });
    timeline.entries.insert(0, cache_stale);
    timeline.summary = readiness_summary_from_entries(&timeline.entries);
    timeline
}

pub fn effective_snapshot(snapshot: &ControlPlaneSnapshot) -> EffectiveReadinessSnapshot {
    let cache_age_ms = unix_now_ms().saturating_sub(snapshot.readyz.updated_at_unix_ms);
    let mut readyz = if cache_age_ms > READYZ_CACHE_STALE_AFTER_MS {
        ReadyzResponse {
            status: "degraded".into(),
            startup_phase: "degraded".into(),
            active_failure: true,
            reason: Some("readiness cache stale".into()),
            reasons: Vec::new(),
        }
    } else {
        snapshot.readyz.response.clone()
    };
    readyz.active_failure = readyz.status != "ready";

    let mut metrics = snapshot.metrics.clone();
    metrics.readiness_cache_age_ms = cache_age_ms;
    metrics.startup_phase = readyz.startup_phase.clone();
    metrics.readiness_status = if readyz.status == "ready" {
        "ready".into()
    } else {
        "degraded".into()
    };
    let readiness_reason = readyz
        .reason
        .clone()
        .or_else(|| readyz.reasons.first().map(|reason| reason.message.clone()));
    metrics.readiness_reason = readiness_reason.clone();
    metrics.convergence_active_failure = readyz.active_failure;
    metrics.convergence_active_failure_reason = if readyz.active_failure {
        readiness_reason
    } else {
        None
    };

    if metrics.readiness_status == "ready"
        && !metrics.convergence_active_failure
        && !metrics.replay_in_progress
        && !metrics.follower_mode
        && !metrics.convergence_start_blocked
        && metrics.leader
        && metrics.startup_phase == "degraded"
    {
        metrics.startup_phase = "leader_active".into();
        readyz.startup_phase = metrics.startup_phase.clone();
    }

    EffectiveReadinessSnapshot { readyz, metrics }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
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
    let recommendations = explain_recommendations(
        &taxonomy,
        active_failure,
        &active_failure_reason,
        metrics,
        false,
    );
    let summary = readiness_summary_from_recommendations(&recommendations);

    ReadinessExplainResponse {
        source: "daemon_api".into(),
        live: true,
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
        snapshot_updated_unix: None,
        snapshot_age_ms: None,
        confidence: "high".into(),
        warning: None,
        operator_interpretation,
        safe_next_action,
        summary,
        recommendations,
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

fn recommendation_for_cache_stale(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "readiness_cache_stale".into(),
        severity: "warning".into(),
        title: "Check daemon convergence loop freshness".into(),
        description: recommendation_description(
            "The cached readiness view is stale, so operator guidance may lag behind the current daemon state.",
            snapshot_based,
        ),
        command_hint: "forge readiness explain && forge readiness timeline".into(),
        safe_to_run: true,
        scope: "convergence".into(),
    }
}

fn recommendation_for_replay(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "replay_incomplete".into(),
        severity: "warning".into(),
        title: "Wait for replay or inspect replay status".into(),
        description: recommendation_description(
            "Startup replay is still blocking convergence, so mutating control-plane work should wait until replay completes or is inspected.",
            snapshot_based,
        ),
        command_hint: "forge control-plane replay-status".into(),
        safe_to_run: true,
        scope: "replay".into(),
    }
}

fn recommendation_for_follower(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "follower_mode".into(),
        severity: "info".into(),
        title: "Run mutating operation on the leader".into(),
        description: recommendation_description(
            "This node is in follower mode and is not expected to run mutating convergence locally.",
            snapshot_based,
        ),
        command_hint: "forge control-plane leader".into(),
        safe_to_run: true,
        scope: "leadership".into(),
    }
}

fn recommendation_for_leadership_uncertain(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "leadership_uncertain".into(),
        severity: "critical".into(),
        title: "Inspect lease ownership".into(),
        description: recommendation_description(
            "Leadership is uncertain or stale, so the active reconciler cannot be trusted until lease ownership is confirmed.",
            snapshot_based,
        ),
        command_hint: "forge control-plane lease".into(),
        safe_to_run: true,
        scope: "leadership".into(),
    }
}

fn recommendation_for_route(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "route_activation_verification_failed".into(),
        severity: "warning".into(),
        title: "Inspect active route target".into(),
        description: recommendation_description(
            "Cached readiness reports that route activation verification failed for the active environment target.",
            snapshot_based,
        ),
        command_hint: "forge diagnose <project> <environment>".into(),
        safe_to_run: true,
        scope: "routing".into(),
    }
}

fn recommendation_for_storage(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "filesystem_scan_timeout".into(),
        severity: "warning".into(),
        title: "Inspect storage pressure and control-plane filesystem latency".into(),
        description: recommendation_description(
            "Cached readiness indicates storage or filesystem scan latency is delaying convergence health checks.",
            snapshot_based,
        ),
        command_hint: "forge doctor".into(),
        safe_to_run: true,
        scope: "storage".into(),
    }
}

fn recommendation_for_docker(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "docker_dependency_health".into(),
        severity: "warning".into(),
        title: "Inspect cached Docker dependency health".into(),
        description: recommendation_description(
            "Cached readiness reports a Docker-related blocker, so the next step is to inspect dependency health without mutating runtime state.",
            snapshot_based,
        ),
        command_hint: "forge doctor".into(),
        safe_to_run: true,
        scope: "docker".into(),
    }
}

fn recommendation_for_caddy(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "caddy_dependency_health".into(),
        severity: "warning".into(),
        title: "Inspect cached Caddy routing health".into(),
        description: recommendation_description(
            "Cached readiness reports a Caddy-related routing blocker, so inspect routing health before taking recovery action.",
            snapshot_based,
        ),
        command_hint: "forge doctor".into(),
        safe_to_run: true,
        scope: "caddy".into(),
    }
}

fn recommendation_for_historical_only(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "historical_convergence_failure".into(),
        severity: "info".into(),
        title: "No action required".into(),
        description: recommendation_description(
            "Only historical convergence failures remain in the cached readiness history; there is no active blocker to clear.",
            snapshot_based,
        ),
        command_hint: "forge readiness timeline".into(),
        safe_to_run: true,
        scope: "convergence".into(),
    }
}

fn recommendation_for_unknown(snapshot_based: bool) -> ReadinessRecommendation {
    ReadinessRecommendation {
        action_id: "unknown_readiness_blocker".into(),
        severity: "warning".into(),
        title: "Inspect cached readiness details".into(),
        description: recommendation_description(
            "The cached readiness state does not map cleanly to a known blocker class, so inspect the cached explanation and timeline next.",
            snapshot_based,
        ),
        command_hint: "forge readiness explain && forge readiness timeline".into(),
        safe_to_run: true,
        scope: "unknown".into(),
    }
}

fn recommendation_description(base: &str, snapshot_based: bool) -> String {
    if snapshot_based {
        format!("{base} Recommendation is based on an offline snapshot and may be stale.")
    } else {
        base.into()
    }
}

fn explain_recommendations(
    taxonomy: &str,
    active_failure: bool,
    active_failure_reason: &Option<String>,
    metrics: &MetricsResponse,
    snapshot_based: bool,
) -> Vec<ReadinessRecommendation> {
    if !active_failure && metrics.convergence_last_failure_historical_unix.is_some() {
        return vec![recommendation_for_historical_only(snapshot_based)];
    }
    if !active_failure && taxonomy == "ready_no_active_failure" {
        return Vec::new();
    }
    let mut recommendations = vec![recommendation_for_state(
        taxonomy,
        active_failure_reason.as_deref(),
        metrics,
        snapshot_based,
    )];
    sort_recommendations(&mut recommendations);
    recommendations
}

pub fn offline_recommendations(
    taxonomy: &str,
    active_failure: bool,
    active_failure_reason: &Option<String>,
    metrics: &MetricsResponse,
) -> Vec<ReadinessRecommendation> {
    explain_recommendations(
        taxonomy,
        active_failure,
        active_failure_reason,
        metrics,
        true,
    )
}

fn recommendation_for_state(
    taxonomy: &str,
    active_failure_reason: Option<&str>,
    metrics: &MetricsResponse,
    snapshot_based: bool,
) -> ReadinessRecommendation {
    if taxonomy == "degraded_cache_stale" {
        return recommendation_for_cache_stale(snapshot_based);
    }
    if taxonomy == "degraded_replay_incomplete" {
        return recommendation_for_replay(snapshot_based);
    }
    if taxonomy == "degraded_follower_mode" {
        return recommendation_for_follower(snapshot_based);
    }
    if taxonomy == "degraded_leadership_uncertain" || leadership_uncertain(metrics) {
        return recommendation_for_leadership_uncertain(snapshot_based);
    }

    let reason = active_failure_reason
        .unwrap_or_default()
        .to_ascii_lowercase();
    if reason.contains("route") || reason.contains("verification") {
        return recommendation_for_route(snapshot_based);
    }
    if reason.contains("filesystem") || reason.contains("storage") {
        return recommendation_for_storage(snapshot_based);
    }
    if reason.contains("docker") {
        return recommendation_for_docker(snapshot_based);
    }
    if reason.contains("caddy") {
        return recommendation_for_caddy(snapshot_based);
    }
    if reason.contains("lease") || reason.contains("leader") || reason.contains("leadership") {
        return recommendation_for_leadership_uncertain(snapshot_based);
    }
    recommendation_for_unknown(snapshot_based)
}

pub fn build_timeline(
    readyz: &ReadyzResponse,
    metrics: &MetricsResponse,
    previous: Option<&ReadinessTimelineResponse>,
    now_unix: u64,
    source: &str,
    live: bool,
    warning: Option<String>,
) -> ReadinessTimelineResponse {
    let startup_phase = if readyz.startup_phase.is_empty() {
        metrics.startup_phase.clone()
    } else {
        readyz.startup_phase.clone()
    };
    let active_entries = readyz
        .reasons
        .iter()
        .map(|reason| {
            let timestamp_unix = reason.last_checked_unix.unwrap_or(now_unix);
            ReadinessTimelineEntry {
                timestamp_unix,
                status: "active".into(),
                blocker_type: blocker_type_for_marker(&reason.marker, &reason.message).into(),
                reason: timeline_reason(
                    reason.project_id.as_str(),
                    reason.environment.as_str(),
                    &reason.message,
                ),
                startup_phase: startup_phase.clone(),
                source: if source == "offline_snapshot" {
                    source.into()
                } else {
                    reason.source.clone()
                },
                active_failure: true,
                suggested_action: timeline_suggested_action(&reason.marker, &reason.message),
                recommendation: Some(timeline_recommendation(
                    &reason.marker,
                    &reason.message,
                    metrics,
                    source == "offline_snapshot",
                )),
                related_fields: Some(timeline_related_fields(
                    metrics,
                    reason.diagnostics.as_ref(),
                )),
            }
        })
        .collect::<Vec<_>>();

    let mut entries = active_entries.clone();
    if let Some(previous) = previous {
        for prior in &previous.entries {
            if prior.status != "active" {
                if entries
                    .iter()
                    .all(|entry| !same_timeline_identity(entry, prior))
                {
                    let mut entry = prior.clone();
                    if source == "offline_snapshot" {
                        entry.source = "offline_snapshot".into();
                    }
                    entries.push(entry);
                }
                continue;
            }
            if active_entries
                .iter()
                .any(|entry| same_timeline_identity(entry, prior))
            {
                continue;
            }
            let mut cleared = prior.clone();
            cleared.timestamp_unix = now_unix;
            cleared.status = "cleared".into();
            cleared.startup_phase = startup_phase.clone();
            cleared.source = source.into();
            cleared.active_failure = false;
            if cleared.suggested_action.is_empty() {
                cleared.suggested_action =
                    "issue cleared; inspect historical diagnostics only if it recurs".into();
            }
            if cleared.related_fields.is_none() {
                cleared.related_fields = Some(timeline_related_fields(metrics, None));
            }
            entries.push(cleared);
        }
    }

    if metrics.convergence_failures_total > 0
        || metrics.convergence_last_failure_historical_unix.is_some()
    {
        let historical = ReadinessTimelineEntry {
            timestamp_unix: metrics
                .convergence_last_failure_historical_unix
                .or(metrics.convergence_last_failure_unix)
                .unwrap_or(now_unix),
            status: "historical".into(),
            blocker_type: "convergence".into(),
            reason: "convergence failure counter incremented".into(),
            startup_phase: startup_phase.clone(),
            source: source.into(),
            active_failure: false,
            suggested_action: "not an active readiness blocker".into(),
            recommendation: Some(recommendation_for_historical_only(
                source == "offline_snapshot",
            )),
            related_fields: Some(timeline_related_fields(metrics, None)),
        };
        if entries
            .iter()
            .all(|entry| !(entry.status == "historical" && entry.reason == historical.reason))
        {
            entries.push(historical);
        }
    }

    entries.sort_by(|left, right| {
        timeline_status_rank(left.status.as_str())
            .cmp(&timeline_status_rank(right.status.as_str()))
            .then_with(|| {
                timeline_recommendation_rank(left).cmp(&timeline_recommendation_rank(right))
            })
            .then_with(|| right.timestamp_unix.cmp(&left.timestamp_unix))
            .then_with(|| left.blocker_type.cmp(&right.blocker_type))
            .then_with(|| left.reason.cmp(&right.reason))
    });
    entries.truncate(8);
    let summary = readiness_summary_from_entries(&entries);

    ReadinessTimelineResponse {
        source: source.into(),
        live,
        generated_at_unix: now_unix,
        warning,
        summary,
        entries,
    }
}

pub fn load_persisted_timeline(
    storage_root: &Path,
) -> Result<Option<ReadinessTimelineResponse>, String> {
    let path = timeline_snapshot_path(storage_root);
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read(&path).map_err(|err| err.to_string())?;
    serde_json::from_slice(&raw)
        .map(Some)
        .map_err(|err| format!("{}: {}", path.display(), err))
}

pub fn persist_timeline(
    storage_root: &Path,
    timeline: &ReadinessTimelineResponse,
) -> Result<(), String> {
    let path = timeline_snapshot_path(storage_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let body = serde_json::to_vec_pretty(timeline).map_err(|err| err.to_string())?;
    fs::write(path, body).map_err(|err| err.to_string())
}

pub fn unknown_offline_timeline(reason: &str) -> ReadinessTimelineResponse {
    let entries = vec![ReadinessTimelineEntry {
        timestamp_unix: unix_now_ms() / 1_000,
        status: "historical".into(),
        blocker_type: "unknown".into(),
        reason: reason.into(),
        startup_phase: "unknown".into(),
        source: "offline_snapshot".into(),
        active_failure: false,
        suggested_action: "query the live daemon when available".into(),
        recommendation: Some(recommendation_for_unknown(true)),
        related_fields: None,
    }];
    ReadinessTimelineResponse {
        source: "offline_snapshot".into(),
        live: false,
        generated_at_unix: unix_now_ms() / 1_000,
        warning: Some("offline snapshot may be stale".into()),
        summary: readiness_summary_from_entries(&entries),
        entries,
    }
}

fn timeline_snapshot_path(storage_root: &Path) -> PathBuf {
    storage_root
        .join("control_plane")
        .join("readiness_timeline.json")
}

fn timeline_reason(project_id: &str, environment: &str, message: &str) -> String {
    if project_id.is_empty() || project_id == "_control_plane" || environment.is_empty() {
        return message.into();
    }
    format!("{project_id}/{environment}: {message}")
}

fn blocker_type_for_marker(marker: &str, message: &str) -> &'static str {
    if marker.contains("route") || marker.contains("caddy") || message.contains("route") {
        "routing"
    } else if marker.contains("docker") {
        "dependency"
    } else if marker.contains("replay") {
        "replay"
    } else if marker.contains("lease")
        || marker.contains("leader")
        || marker.contains("follower")
        || marker.contains("ownership")
        || message.contains("leadership")
    {
        "leadership"
    } else if marker.contains("checkpoint") {
        "checkpoint"
    } else if marker.contains("cache") {
        "cache"
    } else {
        "convergence"
    }
}

fn timeline_suggested_action(marker: &str, message: &str) -> String {
    if message == "convergence failure counter incremented" {
        return "not an active readiness blocker".into();
    }
    timeline_recommendation(marker, message, &MetricsResponse::default(), false).title
}

fn timeline_recommendation(
    marker: &str,
    message: &str,
    metrics: &MetricsResponse,
    snapshot_based: bool,
) -> ReadinessRecommendation {
    let blocker_type = blocker_type_for_marker(marker, message);
    if message == "convergence failure counter incremented" {
        return recommendation_for_historical_only(snapshot_based);
    }
    match blocker_type {
        "routing" => recommendation_for_route(snapshot_based),
        "replay" => recommendation_for_replay(snapshot_based),
        "leadership" => {
            if marker.contains("follower") || message.contains("follower") {
                recommendation_for_follower(snapshot_based)
            } else {
                recommendation_for_leadership_uncertain(snapshot_based)
            }
        }
        "cache" => recommendation_for_cache_stale(snapshot_based),
        "dependency" => {
            let lower = format!("{marker} {message}").to_ascii_lowercase();
            if lower.contains("caddy") {
                recommendation_for_caddy(snapshot_based)
            } else {
                recommendation_for_docker(snapshot_based)
            }
        }
        "checkpoint" => recommendation_for_storage(snapshot_based),
        _ => recommendation_for_state("degraded_unknown", Some(message), metrics, snapshot_based),
    }
}

fn timeline_related_fields(
    metrics: &MetricsResponse,
    diagnostics: Option<&crate::api::ReadyzReasonDiagnostics>,
) -> ReadinessTimelineRelatedFields {
    let route_verification_state = diagnostics
        .and_then(|value| value.last_convergence_error.clone())
        .filter(|value| value.contains("route") || value.contains("verification"));
    let related = ReadinessTimelineRelatedFields {
        convergence_start_blocked: Some(metrics.convergence_start_blocked),
        replay_in_progress: Some(metrics.replay_in_progress),
        follower_mode: Some(metrics.follower_mode),
        leader: Some(metrics.leader),
        lease_epoch: Some(metrics.lease_epoch),
        route_verification_state,
        filesystem_scan_state: None,
    };
    if related == ReadinessTimelineRelatedFields::default() {
        ReadinessTimelineRelatedFields::default()
    } else {
        related
    }
}

fn same_timeline_identity(left: &ReadinessTimelineEntry, right: &ReadinessTimelineEntry) -> bool {
    left.blocker_type == right.blocker_type && left.reason == right.reason
}

fn timeline_status_rank(status: &str) -> u8 {
    match status {
        "active" => 0,
        "cleared" => 1,
        _ => 2,
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

fn sort_recommendations(recommendations: &mut [ReadinessRecommendation]) {
    recommendations.sort_by(|left, right| {
        recommendation_rank(left)
            .cmp(&recommendation_rank(right))
            .then_with(|| {
                left.action_id
                    .cmp(&right.action_id)
                    .then_with(|| left.title.cmp(&right.title))
            })
    });
}

fn recommendation_rank(recommendation: &ReadinessRecommendation) -> (u8, u8) {
    let status_rank = if recommendation.action_id == "historical_convergence_failure" {
        2
    } else {
        0
    };
    (status_rank, severity_rank(recommendation.severity.as_str()))
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "critical" => 0,
        "warning" => 1,
        "info" => 2,
        _ => 3,
    }
}

fn timeline_recommendation_rank(entry: &ReadinessTimelineEntry) -> u8 {
    entry
        .recommendation
        .as_ref()
        .map(|recommendation| severity_rank(recommendation.severity.as_str()))
        .unwrap_or(3)
}

fn readiness_summary_from_recommendations(
    recommendations: &[ReadinessRecommendation],
) -> Option<ReadinessSummary> {
    if recommendations.is_empty() {
        return Some(ReadinessSummary {
            active_count: 0,
            cleared_count: 0,
            historical_count: 0,
            highest_severity: "info".into(),
            primary_recommendation: None,
        });
    }
    let mut ordered = recommendations.to_vec();
    sort_recommendations(&mut ordered);
    Some(ReadinessSummary {
        active_count: ordered
            .iter()
            .filter(|recommendation| recommendation.action_id != "historical_convergence_failure")
            .count(),
        cleared_count: 0,
        historical_count: ordered
            .iter()
            .filter(|recommendation| recommendation.action_id == "historical_convergence_failure")
            .count(),
        highest_severity: ordered
            .first()
            .map(|recommendation| recommendation.severity.clone())
            .unwrap_or_else(|| "info".into()),
        primary_recommendation: ordered.first().cloned(),
    })
}

fn readiness_summary_from_entries(entries: &[ReadinessTimelineEntry]) -> Option<ReadinessSummary> {
    let active_count = entries
        .iter()
        .filter(|entry| entry.status == "active")
        .count();
    let cleared_count = entries
        .iter()
        .filter(|entry| entry.status == "cleared")
        .count();
    let historical_count = entries
        .iter()
        .filter(|entry| entry.status == "historical")
        .count();
    let mut recommendations = entries
        .iter()
        .filter_map(|entry| entry.recommendation.clone())
        .collect::<Vec<_>>();
    dedupe_historical_no_action(&mut recommendations);
    sort_recommendations(&mut recommendations);
    Some(ReadinessSummary {
        active_count,
        cleared_count,
        historical_count,
        highest_severity: recommendations
            .first()
            .map(|recommendation| recommendation.severity.clone())
            .unwrap_or_else(|| "info".into()),
        primary_recommendation: recommendations.first().cloned(),
    })
}

fn dedupe_historical_no_action(recommendations: &mut Vec<ReadinessRecommendation>) {
    let mut seen_historical_no_action = false;
    recommendations.retain(|recommendation| {
        if recommendation.action_id != "historical_convergence_failure" {
            return true;
        }
        if seen_historical_no_action {
            return false;
        }
        seen_historical_no_action = true;
        true
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{ClusterDiagnostics, MetricsDependencySnapshot};
    use crate::daemon::DaemonReadyzCache;

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
        assert_eq!(response.recommendations[0].title, "No action required");
        assert_eq!(response.recommendations[0].scope, "convergence");
        assert_eq!(
            response
                .summary
                .as_ref()
                .map(|summary| summary.historical_count),
            Some(1)
        );
        assert_eq!(
            response
                .summary
                .as_ref()
                .and_then(|summary| summary.primary_recommendation.as_ref())
                .map(|recommendation| recommendation.action_id.as_str()),
            Some("historical_convergence_failure")
        );
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
        assert_eq!(
            response.recommendations[0].action_id,
            "route_activation_verification_failed"
        );
        assert_eq!(response.recommendations[0].scope, "routing");
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
        assert_eq!(response.recommendations[0].action_id, "replay_incomplete");
        assert_eq!(response.recommendations[0].scope, "replay");
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
        assert_eq!(response.recommendations[0].action_id, "follower_mode");
        assert_eq!(response.recommendations[0].scope, "leadership");
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
        assert_eq!(
            response.recommendations[0].action_id,
            "readiness_cache_stale"
        );
        assert_eq!(response.recommendations[0].scope, "convergence");
    }

    #[test]
    fn leadership_uncertain_returns_lease_recommendation() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.active_failure = true;
        readyz.reason = Some("leadership uncertain".into());
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.leader = false;
        metrics.convergence_owner = String::new();
        metrics.cluster.local_role = "candidate".into();
        let response = explain(&readyz, &metrics);
        assert_eq!(
            response.recommendations[0].action_id,
            "leadership_uncertain"
        );
        assert_eq!(response.recommendations[0].scope, "leadership");
    }

    #[test]
    fn effective_snapshot_normalizes_ready_phase() {
        let snapshot = ControlPlaneSnapshot {
            readyz: DaemonReadyzCache {
                response: base_readyz(),
                updated_at_unix_ms: unix_now_ms(),
            },
            metrics: MetricsResponse {
                readiness_status: "ready".into(),
                startup_phase: "degraded".into(),
                leader: true,
                reconciliation_enabled: true,
                cluster: ClusterDiagnostics {
                    local_role: "leader".into(),
                    ..ClusterDiagnostics::default()
                },
                docker: MetricsDependencySnapshot::default(),
                caddy: MetricsDependencySnapshot::default(),
                ..MetricsResponse::default()
            },
            ..ControlPlaneSnapshot::default()
        };

        let effective = effective_snapshot(&snapshot);
        assert_eq!(effective.readyz.startup_phase, "leader_active");
        assert_eq!(effective.metrics.startup_phase, "leader_active");
    }

    #[test]
    fn effective_snapshot_marks_stale_cache_degraded_everywhere() {
        let snapshot = ControlPlaneSnapshot {
            readyz: DaemonReadyzCache {
                response: base_readyz(),
                updated_at_unix_ms: unix_now_ms().saturating_sub(READYZ_CACHE_STALE_AFTER_MS + 1),
            },
            metrics: base_metrics(),
            ..ControlPlaneSnapshot::default()
        };

        let effective = effective_snapshot(&snapshot);
        assert_eq!(effective.readyz.status, "degraded");
        assert_eq!(effective.readyz.startup_phase, "degraded");
        assert!(effective.readyz.active_failure);
        assert_eq!(effective.metrics.readiness_status, "degraded");
        assert_eq!(effective.metrics.startup_phase, "degraded");
        assert!(effective.metrics.convergence_active_failure);
        assert_eq!(
            effective
                .metrics
                .convergence_active_failure_reason
                .as_deref(),
            Some("readiness cache stale")
        );
    }

    #[test]
    fn active_blocker_appears_as_active_timeline_entry() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.active_failure = true;
        readyz.reasons = vec![crate::api::ReadyzReason {
            project_id: "api".into(),
            environment: "production".into(),
            generation: Some(7),
            active: true,
            unresolved: true,
            source: "runtime_state_cache".into(),
            marker: "route_activation_verification_failed".into(),
            message: "route activation verification failed".into(),
            last_checked_unix: Some(200),
            cache_age_ms: 0,
            diagnostics: None,
        }];
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.convergence_start_blocked = true;
        let timeline = build_timeline(&readyz, &metrics, None, 200, "daemon_api", true, None);
        assert_eq!(timeline.entries[0].status, "active");
        assert_eq!(timeline.entries[0].blocker_type, "routing");
        assert!(timeline.entries[0].active_failure);
        assert_eq!(
            timeline.entries[0]
                .recommendation
                .as_ref()
                .map(|value| value.scope.as_str()),
            Some("routing")
        );
    }

    #[test]
    fn cleared_blocker_does_not_remain_active_in_timeline() {
        let previous = ReadinessTimelineResponse {
            source: "daemon_api".into(),
            live: true,
            generated_at_unix: 100,
            warning: None,
            summary: None,
            entries: vec![ReadinessTimelineEntry {
                timestamp_unix: 100,
                status: "active".into(),
                blocker_type: "routing".into(),
                reason: "api/production: route activation verification failed".into(),
                startup_phase: "leader_active".into(),
                source: "runtime_state_cache".into(),
                active_failure: true,
                suggested_action: "inspect route diagnostics and Caddy admin health".into(),
                recommendation: None,
                related_fields: None,
            }],
        };
        let timeline = build_timeline(
            &base_readyz(),
            &base_metrics(),
            Some(&previous),
            200,
            "daemon_api",
            true,
            None,
        );
        assert!(
            timeline
                .entries
                .iter()
                .any(|entry| entry.status == "cleared")
        );
        assert!(!timeline.entries.iter().any(|entry| {
            entry.status == "active"
                && entry.reason == "api/production: route activation verification failed"
        }));
    }

    #[test]
    fn historical_convergence_counters_do_not_become_active_timeline_entries() {
        let mut metrics = base_metrics();
        metrics.convergence_failures_total = 3;
        metrics.convergence_last_failure_historical_unix = Some(150);
        let timeline = build_timeline(
            &base_readyz(),
            &metrics,
            None,
            200,
            "daemon_api",
            true,
            None,
        );
        assert!(timeline.entries.iter().any(|entry| {
            entry.status == "historical"
                && entry.reason == "convergence failure counter incremented"
        }));
        assert!(!timeline.entries.iter().any(|entry| {
            entry.status == "active" && entry.reason == "convergence failure counter incremented"
        }));
    }

    #[test]
    fn timeline_prioritizes_active_critical_before_active_warning() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.active_failure = true;
        readyz.reasons = vec![
            crate::api::ReadyzReason {
                project_id: String::new(),
                environment: String::new(),
                generation: None,
                active: true,
                unresolved: true,
                source: "runtime_state_cache".into(),
                marker: "route_activation_verification_failed".into(),
                message: "route activation verification failed".into(),
                last_checked_unix: Some(300),
                cache_age_ms: 0,
                diagnostics: None,
            },
            crate::api::ReadyzReason {
                project_id: String::new(),
                environment: String::new(),
                generation: None,
                active: true,
                unresolved: true,
                source: "runtime_state_cache".into(),
                marker: "lease_epoch_divergence".into(),
                message: "leadership uncertain".into(),
                last_checked_unix: Some(200),
                cache_age_ms: 0,
                diagnostics: None,
            },
        ];
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.leader = false;
        metrics.cluster.lease_epoch_divergence = true;

        let timeline = build_timeline(&readyz, &metrics, None, 400, "daemon_api", true, None);
        assert_eq!(
            timeline.entries[0]
                .recommendation
                .as_ref()
                .map(|recommendation| recommendation.severity.as_str()),
            Some("critical")
        );
        assert_eq!(
            timeline.entries[1]
                .recommendation
                .as_ref()
                .map(|recommendation| recommendation.severity.as_str()),
            Some("warning")
        );
    }

    #[test]
    fn active_warnings_sort_before_historical_info_and_summary_counts_match() {
        let mut readyz = base_readyz();
        readyz.status = "degraded".into();
        readyz.active_failure = true;
        readyz.reasons = vec![crate::api::ReadyzReason {
            project_id: String::new(),
            environment: String::new(),
            generation: None,
            active: true,
            unresolved: true,
            source: "runtime_state_cache".into(),
            marker: "route_activation_verification_failed".into(),
            message: "route activation verification failed".into(),
            last_checked_unix: Some(300),
            cache_age_ms: 0,
            diagnostics: None,
        }];
        let mut metrics = base_metrics();
        metrics.readiness_status = "degraded".into();
        metrics.convergence_failures_total = 1;
        metrics.convergence_last_failure_historical_unix = Some(150);

        let timeline = build_timeline(&readyz, &metrics, None, 400, "daemon_api", true, None);
        assert_eq!(timeline.entries[0].status, "active");
        assert_eq!(timeline.entries[1].status, "historical");
        assert_eq!(
            timeline
                .summary
                .as_ref()
                .map(|summary| summary.active_count),
            Some(1)
        );
        assert_eq!(
            timeline
                .summary
                .as_ref()
                .map(|summary| summary.historical_count),
            Some(1)
        );
        assert_eq!(
            timeline
                .summary
                .as_ref()
                .and_then(|summary| summary.primary_recommendation.as_ref())
                .map(|recommendation| recommendation.action_id.as_str()),
            Some("route_activation_verification_failed")
        );
    }
}
