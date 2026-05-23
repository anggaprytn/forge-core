use crate::runtime::{RouteUpdateRequest, RoutingRuntime};
use crate::storage::{
    EnvironmentPaths, LeaderLeaseStore, NodeMetadataStore, OperationalJournalEntry,
    OperationalJournalStore, PointerStore, RuntimeHealthState, RuntimeState, RuntimeStateStore,
    StorageError, StorageResult, atomic_write, current_unix_timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RECONCILIATION_LOG_MAX_METADATA_KEYS: usize = 16;
const RECONCILIATION_LOG_MAX_METADATA_STRING_BYTES: usize = 256;
const RECONCILIATION_REPLAY_BATCH_LIMIT: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationIntentStatus {
    Pending,
    Applied,
    Failed,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaySafety {
    ReplaySafe,
    RequiresOperatorIntervention,
    Idempotent,
    Destructive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayStatus {
    NotStarted,
    InProgress,
    Completed,
    DryRun,
    Paused,
    Blocked,
    Corrupted,
    AbortedLeaderLoss,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReconciliationCursor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied_intent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_position: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_started_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_finished_at: Option<u64>,
    #[serde(default)]
    pub replay_status: Option<ReplayStatus>,
    #[serde(default)]
    pub replay_sequence: u64,
    #[serde(default)]
    pub replay_paused: bool,
    #[serde(default)]
    pub replay_quarantined_total: u64,
    #[serde(default)]
    pub replay_aborted_total: u64,
    #[serde(default)]
    pub lease_fencing_failures: u64,
    #[serde(default)]
    pub recovered_operations: Vec<String>,
    #[serde(default)]
    pub skipped_operations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationIntentEntry {
    pub intent_id: String,
    pub timestamp_unix: u64,
    pub node_id: String,
    pub lease_epoch: u64,
    pub operation_type: String,
    pub project_id: String,
    pub environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_generation: Option<u64>,
    pub target_state: String,
    pub reconciliation_domain: String,
    pub status: ReconciliationIntentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_intent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_of: Option<String>,
    pub replay_safety: ReplaySafety,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationIntentRequest {
    pub node_id: String,
    pub lease_epoch: u64,
    pub operation_type: String,
    pub project_id: String,
    pub environment: String,
    pub target_generation: Option<u64>,
    pub target_state: String,
    pub reconciliation_domain: String,
    pub triggered_by: Option<String>,
    pub previous_intent_id: Option<String>,
    pub recovery_of: Option<String>,
    pub replay_safety: ReplaySafety,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReconciliationDiagnostics {
    pub pending_intents: usize,
    pub replay_queue_depth: usize,
    pub replay_in_progress: bool,
    pub replay_paused: bool,
    pub replay_duration_ms: u64,
    pub replay_failures_total: u64,
    pub replay_quarantined_total: u64,
    pub replay_aborted_total: u64,
    pub lease_fencing_failures: u64,
    pub unrecoverable_operations: usize,
    pub last_replayed_intent: Option<String>,
    pub reconciliation_log_size_bytes: u64,
    pub replay_cursor_corrupted: bool,
    pub reconciliation_log_corrupted: bool,
    pub replay_incomplete: bool,
    pub destructive_replay_blocked: bool,
    pub unrecoverable_pending_intents: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayOutcome {
    pub cursor: ReconciliationCursor,
    pub diagnostics: ReconciliationDiagnostics,
    pub intents: Vec<ReconciliationIntentEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplayOptions {
    pub dry_run: bool,
    pub resume: bool,
    pub max_duration_ms: Option<u64>,
    pub max_entries: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadReconciliationLog {
    pub intents: Vec<ReconciliationIntentEntry>,
    pub corrupted: bool,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct QuarantinedIntentRecord {
    quarantined_at_unix: u64,
    reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    raw_line: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    intent: Option<ReconciliationIntentEntry>,
}

pub struct ReconciliationStore {
    storage_root: PathBuf,
}

pub fn intent_request_for_storage_root(
    storage_root: &Path,
    operation_type: &str,
    project_id: &str,
    environment: &str,
    target_generation: Option<u64>,
    target_state: &str,
    reconciliation_domain: &str,
    mut metadata: BTreeMap<String, Value>,
) -> ReconciliationIntentRequest {
    let node_id = NodeMetadataStore::new(storage_root)
        .load()
        .ok()
        .flatten()
        .map(|metadata| metadata.node_id)
        .unwrap_or_else(|| "unknown".into());
    let lease_epoch = LeaderLeaseStore::new(storage_root)
        .load()
        .ok()
        .flatten()
        .map(|lease| lease.lease_epoch)
        .unwrap_or(0);
    metadata
        .entry("storage_root".into())
        .or_insert_with(|| Value::String(storage_root.display().to_string()));
    ReconciliationIntentRequest {
        node_id,
        lease_epoch,
        operation_type: operation_type.into(),
        project_id: project_id.into(),
        environment: environment.into(),
        target_generation,
        target_state: target_state.into(),
        reconciliation_domain: reconciliation_domain.into(),
        triggered_by: None,
        previous_intent_id: None,
        recovery_of: None,
        replay_safety: replay_safe_for_operation(operation_type),
        metadata,
    }
}

impl ReconciliationStore {
    pub fn new(storage_root: impl AsRef<Path>) -> Self {
        Self {
            storage_root: storage_root.as_ref().to_path_buf(),
        }
    }

    pub fn append_pending(
        &self,
        request: ReconciliationIntentRequest,
    ) -> StorageResult<ReconciliationIntentEntry> {
        let entry = ReconciliationIntentEntry {
            intent_id: next_intent_id(),
            timestamp_unix: current_unix_timestamp(),
            node_id: request.node_id,
            lease_epoch: request.lease_epoch,
            operation_type: request.operation_type,
            project_id: request.project_id,
            environment: request.environment,
            target_generation: request.target_generation,
            target_state: request.target_state,
            reconciliation_domain: request.reconciliation_domain,
            status: ReconciliationIntentStatus::Pending,
            triggered_by: request.triggered_by,
            previous_intent_id: request.previous_intent_id,
            recovery_of: request.recovery_of,
            replay_safety: request.replay_safety,
            metadata: bounded_metadata(request.metadata),
        };
        self.append_entry(&entry)?;
        maybe_simulate_crash_after_intent(&entry.operation_type);
        Ok(entry)
    }

    pub fn append_status(
        &self,
        entry: &ReconciliationIntentEntry,
        status: ReconciliationIntentStatus,
        extra_metadata: BTreeMap<String, Value>,
    ) -> StorageResult<ReconciliationIntentEntry> {
        let mut next = entry.clone();
        next.timestamp_unix = current_unix_timestamp();
        next.status = status;
        if !extra_metadata.is_empty() {
            let mut merged = next.metadata.clone();
            merged.extend(extra_metadata);
            next.metadata = bounded_metadata(merged);
        }
        self.append_entry(&next)?;
        Ok(next)
    }

    pub fn append_entry(&self, entry: &ReconciliationIntentEntry) -> StorageResult<()> {
        let path = EnvironmentPaths::reconciliation_log_file(&self.storage_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let line = serde_json::to_string(entry).map_err(invalid_data)?;
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    }

    pub fn read_all(&self) -> StorageResult<ReadReconciliationLog> {
        let path = EnvironmentPaths::reconciliation_log_file(&self.storage_root);
        if !path.exists() {
            return Ok(ReadReconciliationLog::default());
        }
        let raw = fs::read_to_string(&path)?;
        let mut latest = BTreeMap::new();
        let mut corrupted = false;
        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<ReconciliationIntentEntry>(line) {
                Ok(entry) => {
                    latest.insert(entry.intent_id.clone(), entry);
                }
                Err(_) => corrupted = true,
            }
        }
        let mut intents = latest.into_values().collect::<Vec<_>>();
        intents.sort_by(|left, right| {
            (left.timestamp_unix, left.intent_id.as_str())
                .cmp(&(right.timestamp_unix, right.intent_id.as_str()))
        });
        Ok(ReadReconciliationLog {
            intents,
            corrupted,
            size_bytes: raw.len() as u64,
        })
    }

    pub fn load_cursor(&self) -> StorageResult<Option<ReconciliationCursor>> {
        let path = EnvironmentPaths::reconciliation_cursor_file(&self.storage_root);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|err| StorageError::Io(invalid_data(err)))
    }

    pub fn save_cursor(&self, cursor: &ReconciliationCursor) -> StorageResult<()> {
        let path = EnvironmentPaths::reconciliation_cursor_file(&self.storage_root);
        let mut next = cursor.clone();
        if let Ok(Some(current)) = self.load_cursor() {
            if current.replay_sequence > next.replay_sequence {
                next.replay_sequence = current.replay_sequence;
                next.replay_position = current.replay_position;
                next.last_applied_intent = current.last_applied_intent;
            }
            next.replay_quarantined_total = next
                .replay_quarantined_total
                .max(current.replay_quarantined_total);
            next.replay_aborted_total = next.replay_aborted_total.max(current.replay_aborted_total);
            next.lease_fencing_failures = next
                .lease_fencing_failures
                .max(current.lease_fencing_failures);
        }
        atomic_write(
            path,
            serde_json::to_string_pretty(&next)
                .map_err(invalid_data)?
                .as_bytes(),
        )
    }

    pub fn replay<R: RoutingRuntime>(
        &self,
        routing: &mut R,
        node_id: &str,
        lease_epoch: u64,
        options: ReplayOptions,
    ) -> StorageResult<ReplayOutcome> {
        self.sanitize_log_and_quarantine_corruption()?;
        let log = self.read_all()?;
        let cursor_result = self.load_cursor();
        let cursor_corrupted = cursor_result.is_err();
        let mut cursor = cursor_result.unwrap_or_default().unwrap_or_default();
        let now = current_unix_timestamp();
        cursor.replay_started_at = Some(now);
        cursor.replay_finished_at = None;
        cursor.replay_status = Some(if options.dry_run {
            ReplayStatus::DryRun
        } else {
            ReplayStatus::InProgress
        });
        cursor.replay_paused = false;
        self.save_cursor(&cursor)?;

        let mut recovered = Vec::new();
        let mut skipped = Vec::new();
        let mut replay_failures_total = 0_u64;
        let mut destructive_replay_blocked = false;
        let mut unrecoverable_operations = 0_usize;
        let replay_started = Instant::now();
        let replay_budget_entries = options
            .max_entries
            .unwrap_or(RECONCILIATION_REPLAY_BATCH_LIMIT)
            .min(RECONCILIATION_REPLAY_BATCH_LIMIT);
        let replay_budget_duration_ms = options.max_duration_ms.unwrap_or(u64::MAX);

        if !self.ensure_fence(node_id, lease_epoch, lease_epoch, None, &mut cursor)? {
            cursor.replay_status = Some(ReplayStatus::AbortedLeaderLoss);
            cursor.replay_finished_at = Some(current_unix_timestamp());
            cursor.replay_aborted_total = cursor.replay_aborted_total.saturating_add(1);
            self.save_cursor(&cursor)?;
            return Ok(ReplayOutcome {
                diagnostics: build_diagnostics(
                    &log,
                    &cursor,
                    replay_failures_total,
                    unrecoverable_operations,
                    cursor_corrupted || log.corrupted,
                ),
                cursor,
                intents: log.intents,
            });
        }

        let mut pending = log
            .intents
            .iter()
            .filter(|entry| entry.status == ReconciliationIntentStatus::Pending)
            .cloned()
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| {
            (left.timestamp_unix, left.intent_id.as_str())
                .cmp(&(right.timestamp_unix, right.intent_id.as_str()))
        });
        if pending.len() > replay_budget_entries {
            pending.truncate(replay_budget_entries);
        }

        for entry in &pending {
            if replay_started.elapsed().as_millis() as u64 >= replay_budget_duration_ms {
                cursor.replay_paused = true;
                cursor.replay_status = Some(ReplayStatus::Paused);
                cursor.replay_finished_at = Some(current_unix_timestamp());
                self.save_cursor(&cursor)?;
                return Ok(ReplayOutcome {
                    diagnostics: build_diagnostics(
                        &log,
                        &cursor,
                        replay_failures_total,
                        unrecoverable_operations,
                        cursor_corrupted || log.corrupted,
                    ),
                    cursor,
                    intents: self.read_all()?.intents,
                });
            }
            cursor.replay_sequence = cursor.replay_sequence.saturating_add(1);
            cursor.replay_position = Some(entry.intent_id.clone());
            self.save_cursor(&cursor)?;
            if !self.ensure_fence(
                node_id,
                lease_epoch,
                entry.lease_epoch,
                Some(entry),
                &mut cursor,
            )? {
                cursor.replay_status = Some(ReplayStatus::AbortedLeaderLoss);
                cursor.replay_finished_at = Some(current_unix_timestamp());
                cursor.replay_aborted_total = cursor.replay_aborted_total.saturating_add(1);
                cursor.skipped_operations.extend(skipped.clone());
                self.save_cursor(&cursor)?;
                return Ok(ReplayOutcome {
                    diagnostics: build_diagnostics(
                        &log,
                        &cursor,
                        replay_failures_total,
                        unrecoverable_operations,
                        cursor_corrupted,
                    ),
                    cursor,
                    intents: log.intents,
                });
            }

            match entry.replay_safety {
                ReplaySafety::ReplaySafe | ReplaySafety::Idempotent => {
                    if options.dry_run {
                        recovered.push(describe_operation(entry));
                        continue;
                    }
                    match replay_intent(routing, entry) {
                        Ok(()) => {
                            if !self.ensure_fence(
                                node_id,
                                lease_epoch,
                                entry.lease_epoch,
                                Some(entry),
                                &mut cursor,
                            )? {
                                cursor.replay_status = Some(ReplayStatus::AbortedLeaderLoss);
                                cursor.replay_finished_at = Some(current_unix_timestamp());
                                cursor.replay_aborted_total =
                                    cursor.replay_aborted_total.saturating_add(1);
                                self.save_cursor(&cursor)?;
                                return Ok(ReplayOutcome {
                                    diagnostics: build_diagnostics(
                                        &log,
                                        &cursor,
                                        replay_failures_total,
                                        unrecoverable_operations,
                                        cursor_corrupted || log.corrupted,
                                    ),
                                    cursor,
                                    intents: self.read_all()?.intents,
                                });
                            }
                            let applied = self.append_status(
                                entry,
                                ReconciliationIntentStatus::Applied,
                                BTreeMap::new(),
                            )?;
                            cursor.last_applied_intent = Some(applied.intent_id.clone());
                            recovered.push(describe_operation(entry));
                        }
                        Err(err) => {
                            replay_failures_total = replay_failures_total.saturating_add(1);
                            unrecoverable_operations = unrecoverable_operations.saturating_add(1);
                            self.quarantine_intent(
                                entry,
                                &format!("replay_failed: {err}"),
                                None,
                                &mut cursor,
                            )?;
                            let mut metadata = BTreeMap::new();
                            metadata.insert("replay_error".into(), Value::String(err.to_string()));
                            let _ = self.append_status(
                                entry,
                                ReconciliationIntentStatus::Failed,
                                metadata,
                            );
                            skipped.push(describe_operation(entry));
                        }
                    }
                }
                ReplaySafety::RequiresOperatorIntervention => {
                    unrecoverable_operations = unrecoverable_operations.saturating_add(1);
                    self.quarantine_intent(
                        entry,
                        "requires_operator_intervention",
                        None,
                        &mut cursor,
                    )?;
                    let _ = self.append_status(
                        entry,
                        ReconciliationIntentStatus::Failed,
                        BTreeMap::from([(
                            "quarantined".into(),
                            Value::String("requires_operator_intervention".into()),
                        )]),
                    );
                    skipped.push(describe_operation(entry));
                }
                ReplaySafety::Destructive => {
                    destructive_replay_blocked = true;
                    unrecoverable_operations = unrecoverable_operations.saturating_add(1);
                    self.quarantine_intent(entry, "destructive_replay_blocked", None, &mut cursor)?;
                    let _ = self.append_status(
                        entry,
                        ReconciliationIntentStatus::Failed,
                        BTreeMap::from([(
                            "quarantined".into(),
                            Value::String("destructive_replay_blocked".into()),
                        )]),
                    );
                    skipped.push(describe_operation(entry));
                }
            }
        }

        cursor.recovered_operations = recovered.clone();
        cursor.skipped_operations = skipped.clone();
        cursor.replay_finished_at = Some(current_unix_timestamp());
        cursor.replay_paused = false;
        cursor.replay_status = Some(if options.dry_run {
            ReplayStatus::DryRun
        } else if cursor.replay_paused {
            ReplayStatus::Paused
        } else if destructive_replay_blocked || unrecoverable_operations > 0 {
            ReplayStatus::Blocked
        } else {
            ReplayStatus::Completed
        });
        self.save_cursor(&cursor)?;

        let diagnostics = build_diagnostics(
            &self.read_all()?,
            &cursor,
            replay_failures_total,
            unrecoverable_operations,
            cursor_corrupted || log.corrupted,
        );
        Ok(ReplayOutcome {
            cursor,
            diagnostics,
            intents: self.read_all()?.intents,
        })
    }

    pub fn diagnostics(&self) -> ReconciliationDiagnostics {
        let log = self.read_all().unwrap_or_default();
        let cursor = self.load_cursor().ok().flatten().unwrap_or_default();
        build_diagnostics(&log, &cursor, 0, 0, false)
    }

    fn ensure_fence(
        &self,
        node_id: &str,
        lease_epoch: u64,
        intent_epoch: u64,
        entry: Option<&ReconciliationIntentEntry>,
        cursor: &mut ReconciliationCursor,
    ) -> StorageResult<bool> {
        let owns_lease = current_node_is_active_leader(&self.storage_root, node_id, lease_epoch);
        let epoch_matches = lease_epoch > 0 && intent_epoch == lease_epoch;
        if owns_lease && epoch_matches {
            return Ok(true);
        }
        cursor.lease_fencing_failures = cursor.lease_fencing_failures.saturating_add(1);
        let journal = OperationalJournalStore::new(&self.storage_root);
        let _ = journal.append(&OperationalJournalEntry {
            schema_version: 1,
            timestamp_unix: current_unix_timestamp(),
            event_type: "lease_fencing_failed".into(),
            project_id: entry.map(|value| value.project_id.clone()),
            environment: entry.map(|value| value.environment.clone()),
            generation: entry.and_then(|value| value.target_generation),
            payload: serde_json::json!({
                "node_id": node_id,
                "expected_lease_epoch": lease_epoch,
                "intent_lease_epoch": intent_epoch,
                "current_owner_ok": owns_lease,
            }),
        });
        if let Some(entry) = entry {
            self.quarantine_intent(entry, "lease_fencing_failed", None, cursor)?;
        }
        Ok(false)
    }

    fn sanitize_log_and_quarantine_corruption(&self) -> StorageResult<()> {
        let path = EnvironmentPaths::reconciliation_log_file(&self.storage_root);
        if !path.exists() {
            return Ok(());
        }
        let raw = fs::read_to_string(&path)?;
        let mut valid_lines = Vec::new();
        let mut corrupted = false;
        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if serde_json::from_str::<ReconciliationIntentEntry>(line).is_ok() {
                valid_lines.push(line.to_string());
            } else {
                corrupted = true;
                self.quarantine_raw_line("corrupted_reconciliation_log_entry", line)?;
            }
        }
        if corrupted {
            let rewritten = if valid_lines.is_empty() {
                String::new()
            } else {
                format!("{}\n", valid_lines.join("\n"))
            };
            atomic_write(path, rewritten.as_bytes())?;
        }
        Ok(())
    }

    fn quarantine_raw_line(&self, reason: &str, raw_line: &str) -> StorageResult<()> {
        let dir = EnvironmentPaths::reconciliation_quarantine_dir(&self.storage_root);
        fs::create_dir_all(&dir)?;
        let record = QuarantinedIntentRecord {
            quarantined_at_unix: current_unix_timestamp(),
            reason: reason.into(),
            raw_line: Some(raw_line.to_string()),
            intent: None,
        };
        let name = format!("{}-{}.json", record.quarantined_at_unix, next_intent_id());
        atomic_write(
            dir.join(name),
            format!(
                "{}\n",
                serde_json::to_string_pretty(&record).map_err(invalid_data)?
            )
            .as_bytes(),
        )?;
        Ok(())
    }

    fn quarantine_intent(
        &self,
        entry: &ReconciliationIntentEntry,
        reason: &str,
        raw_line: Option<&str>,
        cursor: &mut ReconciliationCursor,
    ) -> StorageResult<()> {
        let dir = EnvironmentPaths::reconciliation_quarantine_dir(&self.storage_root);
        fs::create_dir_all(&dir)?;
        let record = QuarantinedIntentRecord {
            quarantined_at_unix: current_unix_timestamp(),
            reason: reason.into(),
            raw_line: raw_line.map(|value| value.to_string()),
            intent: Some(entry.clone()),
        };
        let name = format!("{}-{}.json", record.quarantined_at_unix, entry.intent_id);
        atomic_write(
            dir.join(name),
            format!(
                "{}\n",
                serde_json::to_string_pretty(&record).map_err(invalid_data)?
            )
            .as_bytes(),
        )?;
        cursor.replay_quarantined_total = cursor.replay_quarantined_total.saturating_add(1);
        let _ = OperationalJournalStore::new(&self.storage_root).append(&OperationalJournalEntry {
            schema_version: 1,
            timestamp_unix: current_unix_timestamp(),
            event_type: "intent_quarantined".into(),
            project_id: Some(entry.project_id.clone()),
            environment: Some(entry.environment.clone()),
            generation: entry.target_generation,
            payload: serde_json::json!({
                "intent_id": entry.intent_id,
                "reason": reason,
                "lease_epoch": entry.lease_epoch,
            }),
        });
        Ok(())
    }
}

pub fn replay_safe_for_operation(operation_type: &str) -> ReplaySafety {
    match operation_type {
        "snapshot_persistence" => ReplaySafety::ReplaySafe,
        "route_activation" => ReplaySafety::Idempotent,
        "deployment_promotion" => ReplaySafety::Idempotent,
        "route_verification" => ReplaySafety::ReplaySafe,
        "runtime_repair" => ReplaySafety::Idempotent,
        "rollback" => ReplaySafety::RequiresOperatorIntervention,
        "backup_restore" => ReplaySafety::RequiresOperatorIntervention,
        "retention_cleanup" => ReplaySafety::Destructive,
        "gc_action" => ReplaySafety::Destructive,
        "volume_repair" => ReplaySafety::RequiresOperatorIntervention,
        _ => ReplaySafety::RequiresOperatorIntervention,
    }
}

fn replay_intent<R: RoutingRuntime>(
    routing: &mut R,
    entry: &ReconciliationIntentEntry,
) -> StorageResult<()> {
    match entry.operation_type.as_str() {
        "snapshot_persistence" => replay_snapshot_persistence(entry),
        "deployment_promotion" => replay_deployment_promotion(entry),
        "route_activation" | "runtime_repair" => replay_route_activation(routing, entry),
        _ => Ok(()),
    }
}

fn replay_snapshot_persistence(entry: &ReconciliationIntentEntry) -> StorageResult<()> {
    let Some(generation) = entry.target_generation else {
        return Ok(());
    };
    let env = EnvironmentPaths::new(
        metadata_path(entry, "storage_root")?,
        &entry.project_id,
        &entry.environment,
    );
    if env.generation_dir(generation).exists()
        && !env
            .generation_dir(generation)
            .join("snapshot.json")
            .exists()
    {
        let snapshot = serde_json::json!({
            "snapshot_version": 1,
            "project_id": entry.project_id,
            "environment": entry.environment,
            "generation": generation,
            "state": entry.target_state,
            "finalized_at_unix": current_unix_timestamp(),
        });
        atomic_write(
            env.generation_dir(generation).join("snapshot.json"),
            format!(
                "{}\n",
                serde_json::to_string_pretty(&snapshot).map_err(invalid_data)?
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn replay_deployment_promotion(entry: &ReconciliationIntentEntry) -> StorageResult<()> {
    let Some(generation) = entry.target_generation else {
        return Ok(());
    };
    let env = EnvironmentPaths::new(
        metadata_path(entry, "storage_root")?,
        &entry.project_id,
        &entry.environment,
    );
    PointerStore::new(env.clone()).swap_current(generation)?;
    RuntimeStateStore::new(env).save(&RuntimeState {
        active_generation: Some(generation),
        health_state: RuntimeHealthState::Healthy,
        failed_probe_count: 0,
        successful_probe_count: 0,
        restart_attempted: false,
        degraded_since_unix: None,
        last_transition: "replay_promotion_completed".into(),
        last_error_code: None,
    })?;
    Ok(())
}

fn replay_route_activation<R: RoutingRuntime>(
    routing: &mut R,
    entry: &ReconciliationIntentEntry,
) -> StorageResult<()> {
    let subtree_id = metadata_string(entry, "subtree_id")?;
    let target = metadata_string(entry, "target")?;
    let domain = metadata_string(entry, "domain")?;
    let probe_path = entry
        .metadata
        .get("probe_path")
        .and_then(Value::as_str)
        .map(|value| value.to_string());
    routing
        .update_route(RouteUpdateRequest {
            subtree_id,
            target,
            domain: Some(domain),
            health_checks_enabled: false,
            probe_path,
        })
        .map_err(|err| StorageError::Io(invalid_data(err)))?;
    Ok(())
}

fn build_diagnostics(
    log: &ReadReconciliationLog,
    cursor: &ReconciliationCursor,
    replay_failures_total: u64,
    unrecoverable_operations: usize,
    corrupted: bool,
) -> ReconciliationDiagnostics {
    let pending = log
        .intents
        .iter()
        .filter(|entry| entry.status == ReconciliationIntentStatus::Pending)
        .collect::<Vec<_>>();
    let destructive_pending = pending
        .iter()
        .any(|entry| entry.replay_safety == ReplaySafety::Destructive);
    let unrecoverable_pending = pending.iter().any(|entry| {
        matches!(
            entry.replay_safety,
            ReplaySafety::RequiresOperatorIntervention | ReplaySafety::Destructive
        )
    });
    let replay_in_progress = cursor.replay_status == Some(ReplayStatus::InProgress);
    let replay_paused = cursor.replay_status == Some(ReplayStatus::Paused) || cursor.replay_paused;
    let replay_duration_ms = cursor
        .replay_started_at
        .zip(cursor.replay_finished_at)
        .map(|(started, finished)| finished.saturating_sub(started).saturating_mul(1_000))
        .unwrap_or(0);
    let mut unique_skipped = BTreeSet::new();
    unique_skipped.extend(cursor.skipped_operations.iter().cloned());
    ReconciliationDiagnostics {
        pending_intents: pending.len(),
        replay_queue_depth: pending.len().min(RECONCILIATION_REPLAY_BATCH_LIMIT),
        replay_in_progress,
        replay_paused,
        replay_duration_ms,
        replay_failures_total,
        replay_quarantined_total: cursor.replay_quarantined_total,
        replay_aborted_total: cursor.replay_aborted_total,
        lease_fencing_failures: cursor.lease_fencing_failures,
        unrecoverable_operations: unrecoverable_operations.max(unique_skipped.len()),
        last_replayed_intent: cursor.last_applied_intent.clone(),
        reconciliation_log_size_bytes: log.size_bytes,
        replay_cursor_corrupted: corrupted && cursor.replay_status.is_none(),
        reconciliation_log_corrupted: log.corrupted,
        replay_incomplete: !pending.is_empty(),
        destructive_replay_blocked: destructive_pending,
        unrecoverable_pending_intents: unrecoverable_pending,
    }
}

fn bounded_metadata(metadata: BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    metadata
        .into_iter()
        .take(RECONCILIATION_LOG_MAX_METADATA_KEYS)
        .map(|(key, value)| (key, bounded_value(value)))
        .collect()
}

fn bounded_value(value: Value) -> Value {
    match value {
        Value::String(text) => {
            let mut bounded = text;
            if bounded.len() > RECONCILIATION_LOG_MAX_METADATA_STRING_BYTES {
                bounded.truncate(RECONCILIATION_LOG_MAX_METADATA_STRING_BYTES);
            }
            Value::String(bounded)
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().take(8).map(bounded_value).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .take(8)
                .map(|(key, value)| (key, bounded_value(value)))
                .collect(),
        ),
        other => other,
    }
}

fn metadata_string(entry: &ReconciliationIntentEntry, key: &str) -> StorageResult<String> {
    entry
        .metadata
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            StorageError::Io(invalid_data(format!(
                "intent {} missing metadata key `{key}`",
                entry.intent_id
            )))
        })
}

fn metadata_path<'a>(entry: &'a ReconciliationIntentEntry, key: &str) -> StorageResult<&'a Path> {
    let path = entry
        .metadata
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            StorageError::Io(invalid_data(format!(
                "intent {} missing metadata key `{key}`",
                entry.intent_id
            )))
        })?;
    Ok(Path::new(path))
}

fn current_node_is_active_leader(storage_root: &Path, node_id: &str, lease_epoch: u64) -> bool {
    let now = current_unix_timestamp();
    LeaderLeaseStore::new(storage_root)
        .load()
        .ok()
        .flatten()
        .is_some_and(|lease| {
            lease.leader_node_id == node_id
                && lease.lease_epoch == lease_epoch
                && lease.expires_at_unix > now
        })
}

fn describe_operation(entry: &ReconciliationIntentEntry) -> String {
    match entry.target_generation {
        Some(generation) => format!(
            "{}:{}:{}/{}@{}",
            entry.intent_id, entry.operation_type, entry.project_id, entry.environment, generation
        ),
        None => format!(
            "{}:{}:{}/{}",
            entry.intent_id, entry.operation_type, entry.project_id, entry.environment
        ),
    }
}

fn invalid_data(err: impl ToString) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
}

fn next_intent_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("intent-{now}-{seq}")
}

#[cfg(test)]
fn maybe_simulate_crash_after_intent(operation_type: &str) {
    let Some(hook) = TEST_CRASH_AFTER_INTENT.get() else {
        return;
    };
    let configured = hook.lock().unwrap();
    if configured.as_deref() == Some(operation_type) {
        panic!("simulated crash after persisting intent for {operation_type}");
    }
}

#[cfg(not(test))]
fn maybe_simulate_crash_after_intent(_operation_type: &str) {}

#[cfg(test)]
static TEST_CRASH_AFTER_INTENT: OnceLock<Mutex<Option<String>>> = OnceLock::new();

#[cfg(test)]
pub fn set_test_crash_after_intent(operation_type: Option<&str>) {
    let hook = TEST_CRASH_AFTER_INTENT.get_or_init(|| Mutex::new(None));
    *hook.lock().unwrap() = operation_type.map(|value| value.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        RouteInspection, RouteUpdateRequest, RoutingRuntime, RoutingRuntimeError,
    };
    use crate::storage::PersistedLeaderLease;
    use crate::storage::{EnvironmentPaths, SnapshotWriter};
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct TestRoutingRuntime {
        updates: Vec<RouteUpdateRequest>,
    }

    struct AssertingRoutingRuntime {
        storage_root: PathBuf,
        saw_pending: bool,
    }

    impl RoutingRuntime for TestRoutingRuntime {
        fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
            self.updates.push(request);
            Ok(())
        }

        fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
            Ok(())
        }

        fn inspect_route(
            &mut self,
            subtree_id: &str,
        ) -> Result<RouteInspection, RoutingRuntimeError> {
            Ok(RouteInspection {
                subtree_id: subtree_id.into(),
                active_target: "http://127.0.0.1:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                health_checks_enabled: false,
                verification_url: Some("https://example.com/health".into()),
                verification_host: Some("example.com".into()),
                verification_status_code: Some(200),
                verification_response_body: Some("ok".into()),
            })
        }

        fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
            Ok(Vec::new())
        }
    }

    impl RoutingRuntime for AssertingRoutingRuntime {
        fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
            let intents = ReconciliationStore::new(&self.storage_root)
                .read_all()
                .unwrap()
                .intents;
            self.saw_pending = intents
                .iter()
                .any(|entry| entry.status == ReconciliationIntentStatus::Pending);
            let _ = request;
            Ok(())
        }

        fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
            Ok(())
        }

        fn inspect_route(
            &mut self,
            subtree_id: &str,
        ) -> Result<RouteInspection, RoutingRuntimeError> {
            Ok(RouteInspection {
                subtree_id: subtree_id.into(),
                active_target: "http://127.0.0.1:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                health_checks_enabled: false,
                verification_url: Some("https://example.com/health".into()),
                verification_host: Some("example.com".into()),
                verification_status_code: Some(200),
                verification_response_body: Some("ok".into()),
            })
        }

        fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
            Ok(Vec::new())
        }
    }

    fn test_root(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let base = std::env::temp_dir().join(format!(
            "forge-reconciliation-tests-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    fn seed_leader(root: &Path, node_id: &str, lease_epoch: u64) {
        let store = LeaderLeaseStore::new(root);
        store
            .try_acquire_or_renew(node_id, current_unix_timestamp(), 30)
            .unwrap();
        let mut lease = store.load().unwrap().unwrap();
        lease.lease_epoch = lease_epoch;
        atomic_write(
            EnvironmentPaths::leader_lease_file(root),
            format!("{}\n", serde_json::to_string_pretty(&lease).unwrap()).as_bytes(),
        )
        .unwrap();
    }

    #[test]
    fn unfinished_intents_recovered_after_restart() {
        let root = test_root("replay-safe-operations-resume");
        seed_leader(&root, "node-a", 7);
        let store = ReconciliationStore::new(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        SnapshotWriter::new(env.clone(), 3)
            .unwrap()
            .finalize("api", "production", crate::storage::SnapshotState::Healthy)
            .unwrap();
        let intent = store
            .append_pending(ReconciliationIntentRequest {
                node_id: "node-a".into(),
                lease_epoch: 7,
                operation_type: "deployment_promotion".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(3),
                target_state: "healthy".into(),
                reconciliation_domain: "runtime_container_reconciliation".into(),
                triggered_by: Some("startup".into()),
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Idempotent,
                metadata: BTreeMap::from([(
                    "storage_root".into(),
                    Value::String(root.display().to_string()),
                )]),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        let replay = store
            .replay(
                &mut routing,
                "node-a",
                7,
                ReplayOptions {
                    dry_run: false,
                    resume: true,
                    max_duration_ms: None,
                    max_entries: None,
                },
            )
            .unwrap();
        assert_eq!(replay.cursor.last_applied_intent, Some(intent.intent_id));
        assert_eq!(
            PointerStore::new(env).read_authoritative_pointer().unwrap(),
            Some(3)
        );
    }

    #[test]
    fn replay_safe_operations_resume_automatically() {
        unfinished_intents_recovered_after_restart();
    }

    #[test]
    fn destructive_operations_blocked_from_auto_replay() {
        let root = test_root("destructive-operations-blocked");
        seed_leader(&root, "node-a", 2);
        let store = ReconciliationStore::new(&root);
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "node-a".into(),
                lease_epoch: 2,
                operation_type: "gc_action".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(4),
                target_state: "deleted".into(),
                reconciliation_domain: "retention_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Destructive,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        let replay = store
            .replay(&mut routing, "node-a", 2, ReplayOptions::default())
            .unwrap();
        assert!(replay.diagnostics.destructive_replay_blocked);
        assert_eq!(routing.updates.len(), 0);
    }

    #[test]
    fn duplicate_replay_is_idempotent() {
        let root = test_root("duplicate-replay-idempotent");
        seed_leader(&root, "node-a", 3);
        let store = ReconciliationStore::new(&root);
        let route_intent = ReconciliationIntentEntry {
            intent_id: "intent-1".into(),
            timestamp_unix: current_unix_timestamp(),
            node_id: "node-a".into(),
            lease_epoch: 3,
            operation_type: "route_activation".into(),
            project_id: "api".into(),
            environment: "production".into(),
            target_generation: Some(9),
            target_state: "healthy".into(),
            reconciliation_domain: "routing_reconciliation".into(),
            status: ReconciliationIntentStatus::Pending,
            triggered_by: None,
            previous_intent_id: None,
            recovery_of: None,
            replay_safety: ReplaySafety::Idempotent,
            metadata: BTreeMap::from([
                ("subtree_id".into(), Value::String("api-production".into())),
                (
                    "target".into(),
                    Value::String("http://127.0.0.1:3000".into()),
                ),
                ("domain".into(), Value::String("example.com".into())),
            ]),
        };
        store.append_entry(&route_intent).unwrap();
        store.append_entry(&route_intent).unwrap();
        let mut routing = TestRoutingRuntime::default();
        let replay = store
            .replay(&mut routing, "node-a", 3, ReplayOptions::default())
            .unwrap();
        assert_eq!(routing.updates.len(), 1);
        assert_eq!(replay.diagnostics.pending_intents, 0);
    }

    #[test]
    fn replay_cursor_survives_restart() {
        let root = test_root("replay-cursor-survives-restart");
        seed_leader(&root, "node-a", 5);
        let store = ReconciliationStore::new(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        SnapshotWriter::new(env.clone(), 8)
            .unwrap()
            .finalize("api", "production", crate::storage::SnapshotState::Healthy)
            .unwrap();
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "node-a".into(),
                lease_epoch: 5,
                operation_type: "deployment_promotion".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(8),
                target_state: "healthy".into(),
                reconciliation_domain: "runtime_container_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Idempotent,
                metadata: BTreeMap::from([(
                    "storage_root".into(),
                    Value::String(root.display().to_string()),
                )]),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        store
            .replay(&mut routing, "node-a", 5, ReplayOptions::default())
            .unwrap();
        let cursor = ReconciliationStore::new(&root)
            .load_cursor()
            .unwrap()
            .unwrap();
        assert_eq!(cursor.replay_status, Some(ReplayStatus::Completed));
        assert!(cursor.last_applied_intent.is_some());
    }

    #[test]
    fn replay_requires_current_leader() {
        let root = test_root("replay-requires-current-leader");
        seed_leader(&root, "leader-a", 9);
        let store = ReconciliationStore::new(&root);
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "leader-a".into(),
                lease_epoch: 9,
                operation_type: "gc_action".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(1),
                target_state: "deleted".into(),
                reconciliation_domain: "retention_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Destructive,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        let replay = store
            .replay(&mut routing, "leader-a", 8, ReplayOptions::default())
            .unwrap();
        assert_eq!(
            replay.cursor.replay_status,
            Some(ReplayStatus::AbortedLeaderLoss)
        );
    }

    #[test]
    fn replay_dry_run_does_not_mutate_state() {
        let root = test_root("replay-dry-run-does-not-mutate-state");
        seed_leader(&root, "node-a", 4);
        let store = ReconciliationStore::new(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        SnapshotWriter::new(env.clone(), 11)
            .unwrap()
            .finalize("api", "production", crate::storage::SnapshotState::Healthy)
            .unwrap();
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "node-a".into(),
                lease_epoch: 4,
                operation_type: "deployment_promotion".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(11),
                target_state: "healthy".into(),
                reconciliation_domain: "runtime_container_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Idempotent,
                metadata: BTreeMap::from([(
                    "storage_root".into(),
                    Value::String(root.display().to_string()),
                )]),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        let replay = store
            .replay(
                &mut routing,
                "node-a",
                4,
                ReplayOptions {
                    dry_run: true,
                    resume: false,
                    max_duration_ms: None,
                    max_entries: None,
                },
            )
            .unwrap();
        assert_eq!(
            PointerStore::new(env).read_authoritative_pointer().unwrap(),
            None
        );
        assert_eq!(replay.cursor.replay_status, Some(ReplayStatus::DryRun));
    }

    #[test]
    fn replay_aborts_after_leader_loss() {
        let root = test_root("replay-aborts-after-leader-loss");
        seed_leader(&root, "node-a", 6);
        let store = ReconciliationStore::new(&root);
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "node-a".into(),
                lease_epoch: 6,
                operation_type: "deployment_promotion".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(2),
                target_state: "healthy".into(),
                reconciliation_domain: "runtime_container_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Idempotent,
                metadata: BTreeMap::from([(
                    "storage_root".into(),
                    Value::String(root.display().to_string()),
                )]),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        let replay = store
            .replay(&mut routing, "node-a", 999, ReplayOptions::default())
            .unwrap();
        assert_eq!(
            replay.cursor.replay_status,
            Some(ReplayStatus::AbortedLeaderLoss)
        );
    }

    #[test]
    fn intent_log_written_before_mutation() {
        let root = test_root("intent-log-written-before-mutation");
        seed_leader(&root, "node-a", 12);
        let store = ReconciliationStore::new(&root);
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "node-a".into(),
                lease_epoch: 12,
                operation_type: "route_activation".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(2),
                target_state: "healthy".into(),
                reconciliation_domain: "routing_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Idempotent,
                metadata: BTreeMap::from([
                    ("subtree_id".into(), Value::String("api-production".into())),
                    (
                        "target".into(),
                        Value::String("http://127.0.0.1:3000".into()),
                    ),
                    ("domain".into(), Value::String("example.com".into())),
                ]),
            })
            .unwrap();
        let mut routing = AssertingRoutingRuntime {
            storage_root: root,
            saw_pending: false,
        };
        store
            .replay(&mut routing, "node-a", 12, ReplayOptions::default())
            .unwrap();
        assert!(routing.saw_pending);
    }

    #[test]
    fn replay_never_runs_without_valid_lease() {
        let root = test_root("replay-never-runs-without-valid-lease");
        seed_leader(&root, "leader-a", 4);
        let store = ReconciliationStore::new(&root);
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "leader-a".into(),
                lease_epoch: 4,
                operation_type: "route_activation".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(1),
                target_state: "healthy".into(),
                reconciliation_domain: "routing_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Idempotent,
                metadata: BTreeMap::from([
                    ("subtree_id".into(), Value::String("api-production".into())),
                    (
                        "target".into(),
                        Value::String("http://127.0.0.1:3000".into()),
                    ),
                    ("domain".into(), Value::String("example.com".into())),
                ]),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        let replay = store
            .replay(&mut routing, "leader-a", 999, ReplayOptions::default())
            .unwrap();
        assert_eq!(
            replay.cursor.replay_status,
            Some(ReplayStatus::AbortedLeaderLoss)
        );
        assert!(routing.updates.is_empty());
    }

    #[derive(Default)]
    struct LeaseFlippingRoutingRuntime {
        root: PathBuf,
        updates: usize,
    }

    impl RoutingRuntime for LeaseFlippingRoutingRuntime {
        fn probe_control_plane(&mut self) -> Result<(), crate::runtime::RoutingRuntimeError> {
            Ok(())
        }

        fn update_route(
            &mut self,
            request: RouteUpdateRequest,
        ) -> Result<(), crate::runtime::RoutingRuntimeError> {
            self.updates = self.updates.saturating_add(1);
            let _ = request;
            let lease = PersistedLeaderLease {
                schema_version: 1,
                leader_node_id: "peer-node".into(),
                acquired_at_unix: current_unix_timestamp(),
                expires_at_unix: current_unix_timestamp().saturating_add(30),
                lease_epoch: 99,
                last_heartbeat_unix: current_unix_timestamp(),
            };
            atomic_write(
                EnvironmentPaths::leader_lease_file(&self.root),
                format!("{}\n", serde_json::to_string_pretty(&lease).unwrap()).as_bytes(),
            )
            .unwrap();
            Ok(())
        }

        fn remove_route(
            &mut self,
            _subtree_id: &str,
        ) -> Result<(), crate::runtime::RoutingRuntimeError> {
            Ok(())
        }

        fn inspect_route(
            &mut self,
            subtree_id: &str,
        ) -> Result<crate::runtime::RouteInspection, crate::runtime::RoutingRuntimeError> {
            Ok(crate::runtime::RouteInspection {
                subtree_id: subtree_id.into(),
                active_target: "http://127.0.0.1:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                health_checks_enabled: false,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
            })
        }

        fn list_managed_routes(
            &mut self,
        ) -> Result<Vec<crate::runtime::RouteInspection>, crate::runtime::RoutingRuntimeError>
        {
            Ok(Vec::new())
        }
    }

    #[test]
    fn replay_aborts_on_lease_loss() {
        let root = test_root("replay-aborts-on-lease-loss");
        seed_leader(&root, "node-a", 13);
        let store = ReconciliationStore::new(&root);
        for id in ["api-production-a", "api-production-b"] {
            store
                .append_pending(ReconciliationIntentRequest {
                    node_id: "node-a".into(),
                    lease_epoch: 13,
                    operation_type: "route_activation".into(),
                    project_id: "api".into(),
                    environment: "production".into(),
                    target_generation: Some(1),
                    target_state: "healthy".into(),
                    reconciliation_domain: "routing_reconciliation".into(),
                    triggered_by: None,
                    previous_intent_id: None,
                    recovery_of: None,
                    replay_safety: ReplaySafety::Idempotent,
                    metadata: BTreeMap::from([
                        ("subtree_id".into(), Value::String(id.into())),
                        (
                            "target".into(),
                            Value::String("http://127.0.0.1:3000".into()),
                        ),
                        ("domain".into(), Value::String("example.com".into())),
                    ]),
                })
                .unwrap();
        }
        let mut routing = LeaseFlippingRoutingRuntime {
            root: root.clone(),
            updates: 0,
        };
        let replay = store
            .replay(&mut routing, "node-a", 13, ReplayOptions::default())
            .unwrap();
        assert_eq!(
            replay.cursor.replay_status,
            Some(ReplayStatus::AbortedLeaderLoss)
        );
        assert_eq!(routing.updates, 1);
        assert!(replay.diagnostics.lease_fencing_failures >= 1);
    }

    #[test]
    fn replay_cursor_monotonic_under_restart() {
        let root = test_root("replay-cursor-monotonic-under-restart");
        seed_leader(&root, "node-a", 21);
        let store = ReconciliationStore::new(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        SnapshotWriter::new(env.clone(), 1)
            .unwrap()
            .finalize("api", "production", crate::storage::SnapshotState::Healthy)
            .unwrap();
        SnapshotWriter::new(env.clone(), 2)
            .unwrap()
            .finalize("api", "production", crate::storage::SnapshotState::Healthy)
            .unwrap();
        for generation in [1, 2] {
            store
                .append_pending(ReconciliationIntentRequest {
                    node_id: "node-a".into(),
                    lease_epoch: 21,
                    operation_type: "deployment_promotion".into(),
                    project_id: "api".into(),
                    environment: "production".into(),
                    target_generation: Some(generation),
                    target_state: "healthy".into(),
                    reconciliation_domain: "runtime_container_reconciliation".into(),
                    triggered_by: None,
                    previous_intent_id: None,
                    recovery_of: None,
                    replay_safety: ReplaySafety::Idempotent,
                    metadata: BTreeMap::from([(
                        "storage_root".into(),
                        Value::String(root.display().to_string()),
                    )]),
                })
                .unwrap();
        }

        let mut routing = TestRoutingRuntime::default();
        store
            .replay(
                &mut routing,
                "node-a",
                21,
                ReplayOptions {
                    max_entries: Some(1),
                    ..ReplayOptions::default()
                },
            )
            .unwrap();
        let first = store.load_cursor().unwrap().unwrap();
        store
            .replay(&mut routing, "node-a", 21, ReplayOptions::default())
            .unwrap();
        let second = store.load_cursor().unwrap().unwrap();
        assert!(second.replay_sequence >= first.replay_sequence);
    }

    #[test]
    fn quarantined_intents_removed_from_active_replay() {
        let root = test_root("quarantined-intents-removed-from-active-replay");
        seed_leader(&root, "node-a", 3);
        let store = ReconciliationStore::new(&root);
        store
            .append_pending(ReconciliationIntentRequest {
                node_id: "node-a".into(),
                lease_epoch: 3,
                operation_type: "gc_action".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(4),
                target_state: "deleted".into(),
                reconciliation_domain: "retention_reconciliation".into(),
                triggered_by: None,
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: ReplaySafety::Destructive,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let mut routing = TestRoutingRuntime::default();
        store
            .replay(&mut routing, "node-a", 3, ReplayOptions::default())
            .unwrap();
        let diagnostics = store.diagnostics();
        assert_eq!(diagnostics.pending_intents, 0);
        assert!(
            EnvironmentPaths::reconciliation_quarantine_dir(&root)
                .read_dir()
                .unwrap()
                .next()
                .is_some()
        );
    }

    #[test]
    fn replay_recovery_deterministic_under_scheduler_permutations() {
        let root = test_root("replay-recovery-deterministic-under-scheduler-permutations");
        seed_leader(&root, "node-a", 31);
        let store = ReconciliationStore::new(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        for generation in [1, 2, 3] {
            SnapshotWriter::new(env.clone(), generation)
                .unwrap()
                .finalize("api", "production", crate::storage::SnapshotState::Healthy)
                .unwrap();
            store
                .append_pending(ReconciliationIntentRequest {
                    node_id: "node-a".into(),
                    lease_epoch: 31,
                    operation_type: "deployment_promotion".into(),
                    project_id: "api".into(),
                    environment: "production".into(),
                    target_generation: Some(generation),
                    target_state: "healthy".into(),
                    reconciliation_domain: "runtime_container_reconciliation".into(),
                    triggered_by: None,
                    previous_intent_id: None,
                    recovery_of: None,
                    replay_safety: ReplaySafety::Idempotent,
                    metadata: BTreeMap::from([(
                        "storage_root".into(),
                        Value::String(root.display().to_string()),
                    )]),
                })
                .unwrap();
        }

        let schedules = vec![vec![1, 1, 1], vec![2, 1], vec![3]];
        let mut final_pending = Vec::new();
        for schedule in schedules {
            let case_root = test_root("replay-deterministic-case");
            fs::create_dir_all(case_root.join("control_plane")).unwrap();
            fs::copy(
                EnvironmentPaths::reconciliation_log_file(&root),
                EnvironmentPaths::reconciliation_log_file(&case_root),
            )
            .unwrap();
            seed_leader(&case_root, "node-a", 31);
            let case_env = EnvironmentPaths::new(&case_root, "api", "production");
            for generation in [1, 2, 3] {
                SnapshotWriter::new(case_env.clone(), generation)
                    .unwrap()
                    .finalize("api", "production", crate::storage::SnapshotState::Healthy)
                    .unwrap();
            }
            let case_store = ReconciliationStore::new(&case_root);
            let mut routing = TestRoutingRuntime::default();
            for max_entries in schedule {
                case_store
                    .replay(
                        &mut routing,
                        "node-a",
                        31,
                        ReplayOptions {
                            max_entries: Some(max_entries),
                            ..ReplayOptions::default()
                        },
                    )
                    .unwrap();
            }
            case_store
                .replay(&mut routing, "node-a", 31, ReplayOptions::default())
                .unwrap();
            final_pending.push(case_store.diagnostics().pending_intents);
        }
        assert!(final_pending.iter().all(|value| *value == 0));
    }
}
