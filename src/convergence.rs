use std::fmt::{Display, Formatter};

use crate::queue::{DeploymentRecord, PersistentQueue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Recovered(DeploymentRecord),
    Failed(DeploymentRecord),
    NoActiveDeployment,
}

#[derive(Debug)]
pub enum ConvergenceError {
    Queue(crate::queue::QueueError),
}

impl Display for ConvergenceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queue(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ConvergenceError {}

impl From<crate::queue::QueueError> for ConvergenceError {
    fn from(value: crate::queue::QueueError) -> Self {
        Self::Queue(value)
    }
}

pub trait ActiveDeploymentDecider {
    fn should_resume(&self, deployment: &DeploymentRecord) -> bool;
}

pub struct StartupConvergence<'a, D> {
    queue: &'a PersistentQueue,
    decider: &'a D,
}

impl<'a, D: ActiveDeploymentDecider> StartupConvergence<'a, D> {
    pub fn new(queue: &'a PersistentQueue, decider: &'a D) -> Self {
        Self { queue, decider }
    }

    pub fn recover_active_deployment(&self) -> Result<RecoveryOutcome, ConvergenceError> {
        let state = self.queue.load_state()?;
        let Some(active) = state.active else {
            return Ok(RecoveryOutcome::NoActiveDeployment);
        };

        if self.decider.should_resume(&active) {
            Ok(RecoveryOutcome::Recovered(active))
        } else {
            let failed = self.queue.complete_active()?.expect("active just checked");
            Ok(RecoveryOutcome::Failed(failed))
        }
    }
}

#[cfg(test)]
fn test_root(name: &str) -> std::path::PathBuf {
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
struct ResumeDecider(bool);

#[cfg(test)]
impl ActiveDeploymentDecider for ResumeDecider {
    fn should_resume(&self, _deployment: &DeploymentRecord) -> bool {
        self.0
    }
}

#[cfg(test)]
pub mod in_flight_deployment_is_recovered_or_failed_deterministically {
    use super::*;
    use crate::queue::DeploymentRecord;

    #[test]
    fn resumable_active_deployment_is_preserved() {
        let root = test_root("recover-active");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();
        let active = queue.start_next().unwrap().unwrap();

        let convergence = StartupConvergence::new(&queue, &ResumeDecider(true));
        let recovered = convergence.recover_active_deployment().unwrap();

        assert_eq!(recovered, RecoveryOutcome::Recovered(active));
        assert!(queue.load_state().unwrap().active.is_some());
    }

    #[test]
    fn non_resumable_active_deployment_is_failed_and_cleared() {
        let root = test_root("fail-active");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();
        let active = queue.start_next().unwrap().unwrap();

        let convergence = StartupConvergence::new(&queue, &ResumeDecider(false));
        let failed = convergence.recover_active_deployment().unwrap();

        assert_eq!(failed, RecoveryOutcome::Failed(active));
        assert!(queue.load_state().unwrap().active.is_none());
    }
}
