use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct MetricsRegistry {
    deployments_total: AtomicU64,
    deployments_failed_total: AtomicU64,
    deployments_rollback_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub deployments_total: u64,
    pub deployments_failed_total: u64,
    pub deployments_rollback_total: u64,
}

static REGISTRY: OnceLock<MetricsRegistry> = OnceLock::new();

pub fn registry() -> &'static MetricsRegistry {
    REGISTRY.get_or_init(MetricsRegistry::default)
}

impl MetricsRegistry {
    pub fn record_deployment_success(&self) {
        self.deployments_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_deployment_failure(&self) {
        self.deployments_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rollback(&self) {
        self.deployments_rollback_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            deployments_total: self.deployments_total.load(Ordering::Relaxed),
            deployments_failed_total: self.deployments_failed_total.load(Ordering::Relaxed),
            deployments_rollback_total: self.deployments_rollback_total.load(Ordering::Relaxed),
        }
    }
}

pub fn render_prometheus(queue_depth: usize) -> String {
    let snapshot = registry().snapshot();
    format!(
        "# TYPE forge_deployments_total counter\nforge_deployments_total {}\n# TYPE forge_deployments_failed_total counter\nforge_deployments_failed_total {}\n# TYPE forge_deployments_rollback_total counter\nforge_deployments_rollback_total {}\n# TYPE forge_queue_depth gauge\nforge_queue_depth {}\n",
        snapshot.deployments_total,
        snapshot.deployments_failed_total,
        snapshot.deployments_rollback_total,
        queue_depth
    )
}

#[cfg(test)]
pub fn reset_for_tests() {
    let registry = registry();
    registry.deployments_total.store(0, Ordering::Relaxed);
    registry
        .deployments_failed_total
        .store(0, Ordering::Relaxed);
    registry
        .deployments_rollback_total
        .store(0, Ordering::Relaxed);
}
