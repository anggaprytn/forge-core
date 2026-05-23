use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct MetricsRegistry {
    deployments_total: AtomicU64,
    deployments_failed_total: AtomicU64,
    deployments_rollback_total: AtomicU64,
    readyz_requests_total: AtomicU64,
    readyz_latency_ms: AtomicU64,
    readyz_degraded_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub deployments_total: u64,
    pub deployments_failed_total: u64,
    pub deployments_rollback_total: u64,
    pub readyz_requests_total: u64,
    pub readyz_latency_ms: u64,
    pub readyz_degraded_total: u64,
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

    pub fn record_readyz_request(&self, latency_ms: u64, degraded: bool) {
        self.readyz_requests_total.fetch_add(1, Ordering::Relaxed);
        self.readyz_latency_ms.store(latency_ms, Ordering::Relaxed);
        if degraded {
            self.readyz_degraded_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            deployments_total: self.deployments_total.load(Ordering::Relaxed),
            deployments_failed_total: self.deployments_failed_total.load(Ordering::Relaxed),
            deployments_rollback_total: self.deployments_rollback_total.load(Ordering::Relaxed),
            readyz_requests_total: self.readyz_requests_total.load(Ordering::Relaxed),
            readyz_latency_ms: self.readyz_latency_ms.load(Ordering::Relaxed),
            readyz_degraded_total: self.readyz_degraded_total.load(Ordering::Relaxed),
        }
    }
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
    registry.readyz_requests_total.store(0, Ordering::Relaxed);
    registry.readyz_latency_ms.store(0, Ordering::Relaxed);
    registry.readyz_degraded_total.store(0, Ordering::Relaxed);
}
