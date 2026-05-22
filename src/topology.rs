use std::collections::BTreeMap;

use crate::storage::{PersistedRuntimeInfo, PersistedServiceRuntimeInfo};

pub fn select_primary_service_id(
    runtime: &PersistedRuntimeInfo,
    services: &BTreeMap<String, PersistedServiceRuntimeInfo>,
) -> Option<String> {
    let startup_order = if runtime.startup_order.is_empty() {
        services.keys().cloned().collect::<Vec<_>>()
    } else {
        runtime.startup_order.clone()
    };

    startup_order
        .iter()
        .find(|service_id| {
            services
                .get(*service_id)
                .is_some_and(|service| service.externally_exposed)
        })
        .cloned()
        .or_else(|| {
            services
                .iter()
                .find(|(_, service)| service.externally_exposed)
                .map(|(service_id, _)| service_id.clone())
        })
        .or_else(|| {
            startup_order
                .into_iter()
                .find(|service_id| services.contains_key(service_id))
        })
        .or_else(|| services.keys().next().cloned())
}

pub fn runtime_with_primary_service(runtime: &PersistedRuntimeInfo) -> PersistedRuntimeInfo {
    if runtime.services.is_empty() {
        return runtime.clone();
    }
    let Some(primary_service_id) = select_primary_service_id(runtime, &runtime.services) else {
        return runtime.clone();
    };
    let Some(primary_service) = runtime.services.get(&primary_service_id) else {
        return runtime.clone();
    };

    PersistedRuntimeInfo {
        container_name: primary_service.container_name.clone(),
        running: primary_service.running,
        network_name: primary_service.network_name.clone(),
        probe_path: primary_service.probe_path.clone(),
        activation: primary_service.activation.clone(),
        runtime_policy: primary_service.runtime_policy.clone(),
        runtime_usage: primary_service.runtime_usage.clone(),
        termination: primary_service.termination.clone(),
        environment_variables: primary_service.environment_variables.clone(),
        volume_mounts: primary_service.volume_mounts.clone(),
        source_ref: runtime.source_ref.clone(),
        repo_url: runtime.repo_url.clone(),
        commit_sha: runtime.commit_sha.clone(),
        source_path: runtime.source_path.clone(),
        services: runtime.services.clone(),
        startup_order: runtime.startup_order.clone(),
    }
}
