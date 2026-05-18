use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::runtime::{
    BuildImageRequest, ContainerInspection, CreateContainerRequest, DockerRuntime,
    DockerRuntimeError,
};
use crate::storage::{EnvironmentPaths, GenerationAllocator, SnapshotState, SnapshotWriter, StorageError};

#[derive(Debug)]
pub enum DeploymentError {
    Queue(QueueError),
    Storage(StorageError),
    Docker(DockerRuntimeError),
    InvalidInspection(String),
}

impl Display for DeploymentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queue(err) => write!(f, "{err}"),
            Self::Storage(err) => write!(f, "{err}"),
            Self::Docker(err) => write!(f, "{err}"),
            Self::InvalidInspection(err) => write!(f, "{err}"),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentExecution {
    pub deployment_id: String,
    pub generation: u64,
    pub image_ref: String,
    pub container_name: String,
}

pub struct DeploymentExecutor<'a, D> {
    storage_root: PathBuf,
    queue: &'a PersistentQueue,
    docker: &'a mut D,
}

impl<'a, D: DockerRuntime> DeploymentExecutor<'a, D> {
    pub fn new(storage_root: impl Into<PathBuf>, queue: &'a PersistentQueue, docker: &'a mut D) -> Self {
        Self {
            storage_root: storage_root.into(),
            queue,
            docker,
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

        let writer = SnapshotWriter::new(env, generation)?;
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
        writer.finalize(&record.project_id, &record.environment, SnapshotState::Healthy)?;

        Ok(DeploymentExecution {
            deployment_id: record.deployment_id.clone(),
            generation,
            image_ref,
            container_name,
        })
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
mod tests {
    use super::*;
    use crate::docker::RecordingCommandRunner;
    use crate::docker::DockerCliRuntime;

    #[test]
    fn queued_deployment_builds_starts_inspects_and_writes_snapshot() {
        let root = test_root("deployment-executor");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();

        let inspect = [
            "name=prod-api-gen-1",
            "running=true",
            "image=forge/api:production-gen-1",
            "restart_policy=no",
        ]
        .join("\n");
        let outputs = vec![
            "image_ref=forge/api:production-gen-1".into(),
            "prod-api-gen-1".into(),
            String::new(),
            inspect,
        ];
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(outputs));

        let execution = DeploymentExecutor::new(&root, &queue, &mut docker)
            .execute_next()
            .unwrap()
            .unwrap();

        assert_eq!(execution.generation, 1);
        assert_eq!(execution.container_name, "prod-api-gen-1");
        let snapshot = root
            .join("projects/api/environments/production/generations/1/snapshot.json");
        assert!(snapshot.exists());
        assert!(root
            .join("projects/api/environments/production/generations/1/build.json")
            .exists());
    }
}
