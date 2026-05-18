use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::runtime::{
    BuildImageRequest, ContainerInspection, CreateContainerRequest, DockerRuntime,
    DockerRuntimeError, ProbeError, ProbeRuntime,
};
use crate::storage::{
    EnvironmentPaths, GenerationAllocator, PointerStore, SnapshotState, SnapshotWriter,
    StorageError,
};

#[derive(Debug)]
pub enum DeploymentError {
    Queue(QueueError),
    Storage(StorageError),
    Docker(DockerRuntimeError),
    Probe(ProbeError),
    InvalidInspection(String),
    ValidationFailed(&'static str),
    RollbackUnavailable,
}

impl Display for DeploymentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queue(err) => write!(f, "{err}"),
            Self::Storage(err) => write!(f, "{err}"),
            Self::Docker(err) => write!(f, "{err}"),
            Self::Probe(err) => write!(f, "{err}"),
            Self::InvalidInspection(err) => write!(f, "{err}"),
            Self::ValidationFailed(err) => write!(f, "{err}"),
            Self::RollbackUnavailable => write!(f, "rollback target unavailable"),
        }
    }
}

impl std::error::Error for DeploymentError {}

impl From<QueueError> for DeploymentError {
    fn from(value: QueueError) -> Self {
        Self::Queue(value)
    }
}

impl From<StorageError> for DeploymentError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<DockerRuntimeError> for DeploymentError {
    fn from(value: DockerRuntimeError) -> Self {
        Self::Docker(value)
    }
}

impl From<ProbeError> for DeploymentError {
    fn from(value: ProbeError) -> Self {
        Self::Probe(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentExecution {
    pub deployment_id: String,
    pub generation: u64,
    pub image_ref: String,
    pub container_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationPolicy {
    pub tcp_required: bool,
    pub http_health_path: Option<String>,
}

impl Default for ValidationPolicy {
    fn default() -> Self {
        Self {
            tcp_required: true,
            http_health_path: None,
        }
    }
}

pub struct DeploymentExecutor<'a, D, P> {
    storage_root: PathBuf,
    queue: &'a PersistentQueue,
    docker: &'a mut D,
    probes: &'a mut P,
    validation: ValidationPolicy,
}

impl<'a, D: DockerRuntime, P: ProbeRuntime> DeploymentExecutor<'a, D, P> {
    pub fn new(
        storage_root: impl Into<PathBuf>,
        queue: &'a PersistentQueue,
        docker: &'a mut D,
        probes: &'a mut P,
        validation: ValidationPolicy,
    ) -> Self {
        Self {
            storage_root: storage_root.into(),
            queue,
            docker,
            probes,
            validation,
        }
    }

    pub fn execute_next(&mut self) -> Result<Option<DeploymentExecution>, DeploymentError> {
        let Some(record) = self.queue.start_next()? else {
            return Ok(None);
        };

        match self.execute_record(&record) {
            Ok(execution) => {
                self.queue.complete_active()?;
                Ok(Some(execution))
            }
            Err(err) => {
                let _ = self.queue.complete_active();
                Err(err)
            }
        }
    }

    fn execute_record(
        &mut self,
        record: &DeploymentRecord,
    ) -> Result<DeploymentExecution, DeploymentError> {
        let env = EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        let generation = GenerationAllocator::new(env.clone()).allocate()?;
        let labels = forge_labels(record, generation);
        let container_name = generation_container_name(record, generation);
        let image_tag = format!("forge/{}:{}-gen-{}", record.project_id, record.environment, generation);
        let writer = SnapshotWriter::new(env.clone(), generation)?;

        let image_ref = self.docker.build_image(BuildImageRequest {
            image_tag: image_tag.clone(),
            context_path: PathBuf::from("."),
            dockerfile_path: PathBuf::from("./Dockerfile"),
            labels: labels.clone(),
        })?;

        self.docker.create_container(CreateContainerRequest {
            container_name: container_name.clone(),
            image_ref: image_ref.clone(),
            labels: labels.clone(),
        })?;
        self.docker.start_container(&container_name)?;
        let inspection = self.docker.inspect_container(&container_name)?;
        validate_inspection(&inspection, &container_name)?;
        writer.write_artifact(
            "build.json",
            &format!(
                "{{\n  \"deployment_id\": \"{}\",\n  \"image_ref\": \"{}\"\n}}\n",
                record.deployment_id, image_ref
            ),
        )?;
        writer.write_artifact(
            "runtime.json",
            &format!(
                "{{\n  \"container_name\": \"{}\",\n  \"running\": {}\n}}\n",
                inspection.container_name, inspection.running
            ),
        )?;
        self.validate_candidate(&container_name)?;

        writer.finalize(&record.project_id, &record.environment, SnapshotState::Healthy)?;
        PointerStore::new(env).swap_current(generation)?;

        Ok(DeploymentExecution {
            deployment_id: record.deployment_id.clone(),
            generation,
            image_ref,
            container_name,
        })
    }

    fn validate_candidate(&mut self, container_name: &str) -> Result<(), DeploymentError> {
        if self.validation.tcp_required && !self.probes.probe_tcp(container_name)? {
            self.cleanup_failed_generation(container_name)?;
            return Err(DeploymentError::ValidationFailed("tcp probe failed"));
        }

        if let Some(path) = &self.validation.http_health_path {
            if !self.probes.probe_http(container_name, path)? {
                self.cleanup_failed_generation(container_name)?;
                return Err(DeploymentError::ValidationFailed("http health probe failed"));
            }
        }

        Ok(())
    }

    fn cleanup_failed_generation(&mut self, container_name: &str) -> Result<(), DeploymentError> {
        let _ = self.docker.stop_container(container_name);
        self.docker.remove_container(container_name)?;
        Ok(())
    }
}

pub struct RollbackExecutor {
    storage_root: PathBuf,
}

impl RollbackExecutor {
    pub fn new(storage_root: impl Into<PathBuf>) -> Self {
        Self {
            storage_root: storage_root.into(),
        }
    }

    pub fn rollback_previous(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<u64, DeploymentError> {
        let env = EnvironmentPaths::new(&self.storage_root, project_id, environment);
        env.ensure_exists()?;
        let pointers = PointerStore::new(env.clone());
        let target = pointers
            .read_pointer("previous")?
            .ok_or(DeploymentError::RollbackUnavailable)?;
        let snapshot = env.generation_dir(target).join("snapshot.json");
        if !snapshot.exists() {
            return Err(DeploymentError::RollbackUnavailable);
        }
        pointers.swap_current(target)?;
        Ok(target)
    }
}

fn validate_inspection(
    inspection: &ContainerInspection,
    expected_container_name: &str,
) -> Result<(), DeploymentError> {
    if inspection.container_name != expected_container_name {
        return Err(DeploymentError::InvalidInspection(
            "inspected container name mismatch".into(),
        ));
    }
    if !inspection.running {
        return Err(DeploymentError::InvalidInspection(
            "container is not running".into(),
        ));
    }
    if inspection.restart_policy != "no" {
        return Err(DeploymentError::InvalidInspection(
            "restart policy must be disabled".into(),
        ));
    }
    Ok(())
}

fn forge_labels(record: &DeploymentRecord, generation: u64) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), record.project_id.clone()),
        ("forge.environment".into(), record.environment.clone()),
        ("forge.generation".into(), generation.to_string()),
        ("forge.deployment_id".into(), record.deployment_id.clone()),
    ])
}

fn generation_container_name(record: &DeploymentRecord, generation: u64) -> String {
    let env = match record.environment.as_str() {
        "production" => "prod",
        "staging" => "staging",
        "development" => "dev",
        other => other,
    };
    format!("{env}-{}-gen-{generation}", record.project_id)
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let base = std::env::temp_dir().join(format!(
        "forge-core-tests-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&base).unwrap();
    base
}

#[cfg(test)]
#[derive(Default)]
struct TestProbeRuntime {
    tcp_ok: bool,
    http_ok: bool,
}

#[cfg(test)]
impl ProbeRuntime for TestProbeRuntime {
    fn probe_tcp(&mut self, _container_name: &str) -> Result<bool, ProbeError> {
        Ok(self.tcp_ok)
    }

    fn probe_http(&mut self, _container_name: &str, _path: &str) -> Result<bool, ProbeError> {
        Ok(self.http_ok)
    }
}

#[cfg(test)]
fn queued_record(queue: &PersistentQueue) {
    queue
        .enqueue(DeploymentRecord {
            deployment_id: "dep-1".into(),
            project_id: "api".into(),
            environment: "production".into(),
        })
        .unwrap();
}

#[cfg(test)]
fn success_outputs(generation: u64) -> Vec<String> {
    vec![
        format!("image_ref=forge/api:production-gen-{generation}"),
        format!("prod-api-gen-{generation}"),
        String::new(),
        [
            format!("name=prod-api-gen-{generation}"),
            format!("running=true"),
            format!("image=forge/api:production-gen-{generation}"),
            "restart_policy=no".into(),
        ]
        .join("\n"),
    ]
}

#[cfg(test)]
pub mod deployment_fails_if_tcp_unreachable {
    use super::*;
    use crate::docker::RecordingCommandRunner;
    use crate::docker::DockerCliRuntime;

    #[test]
    fn tcp_probe_failure_rejects_deployment() {
        let root = test_root("tcp-unreachable");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            ValidationPolicy::default(),
        )
        .execute_next();

        assert!(matches!(result, Err(DeploymentError::ValidationFailed("tcp probe failed"))));
        assert!(!root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists());
    }
}

#[cfg(test)]
pub mod deployment_fails_if_http_health_invalid {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn http_probe_failure_rejects_deployment() {
        let root = test_root("http-invalid");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: false,
        };

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
            },
        )
        .execute_next();

        assert!(matches!(
            result,
            Err(DeploymentError::ValidationFailed("http health probe failed"))
        ));
        assert!(!root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists());
    }
}

#[cfg(test)]
pub mod failed_generation_is_cleaned_up {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn failed_generation_is_stopped_and_removed() {
        let root = test_root("failed-cleanup");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut runner = RecordingCommandRunner::with_outputs(success_outputs(1));
        let mut docker = DockerCliRuntime::new(std::mem::take(&mut runner));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            ValidationPolicy::default(),
        )
        .execute_next();

        let commands = &docker.runner.commands;
        assert!(commands.iter().any(|cmd| cmd.args.first() == Some(&"stop".to_string())));
        assert!(commands.iter().any(|cmd| cmd.args.first() == Some(&"rm".to_string())));
    }
}

#[cfg(test)]
pub mod snapshot_not_finalized_before_validation {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn build_and_runtime_artifacts_exist_but_snapshot_does_not() {
        let root = test_root("snapshot-before-validation");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            ValidationPolicy::default(),
        )
        .execute_next();

        let generation_dir = root.join("projects/api/environments/production/generations/1");
        assert!(generation_dir.join("build.json").exists());
        assert!(generation_dir.join("runtime.json").exists());
        assert!(!generation_dir.join("snapshot.json").exists());
    }
}

#[cfg(test)]
pub mod rollback_restores_previous_generation {
    use super::*;

    #[test]
    fn rollback_moves_current_pointer_back_to_previous() {
        let root = test_root("rollback-previous");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer1 = SnapshotWriter::new(env.clone(), 1).unwrap();
        writer1.finalize("api", "production", SnapshotState::Healthy).unwrap();
        let writer2 = SnapshotWriter::new(env.clone(), 2).unwrap();
        writer2.finalize("api", "production", SnapshotState::Healthy).unwrap();
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let restored = RollbackExecutor::new(&root)
            .rollback_previous("api", "production")
            .unwrap();

        assert_eq!(restored, 1);
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
    }
}

#[cfg(test)]
pub mod current_pointer_never_advances_before_validation {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn current_pointer_remains_unset_when_validation_fails() {
        let root = test_root("pointer-before-validation");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            ValidationPolicy::default(),
        )
        .execute_next();

        let pointers =
            PointerStore::new(EnvironmentPaths::new(&root, "api", "production"));
        assert_eq!(pointers.read_pointer("current").unwrap(), None);
    }
}

#[cfg(test)]
pub mod queued_deployment_builds_starts_validates_and_writes_snapshot {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn successful_deployment_advances_current_after_validation() {
        let root = test_root("deployment-executor-success");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };

        let execution = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
            },
        )
        .execute_next()
        .unwrap()
        .unwrap();

        assert_eq!(execution.generation, 1);
        assert!(root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists());
        let pointers =
            PointerStore::new(EnvironmentPaths::new(&root, "api", "production"));
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
    }
}
