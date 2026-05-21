use crate::runtime::ContainerInspection;
use crate::storage::{PersistedActivationMode, PersistedRouteTargetSource, PersistedRuntimeInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedRoute {
    pub subtree_id: String,
    pub target: String,
    pub domain: Option<String>,
    pub probe_path: Option<String>,
}

pub fn expected_route_for_runtime(
    project_id: &str,
    environment: &str,
    domain: Option<String>,
    runtime: &PersistedRuntimeInfo,
    container: &ContainerInspection,
    preferred_network: Option<&str>,
) -> Option<ExpectedRoute> {
    let PersistedActivationMode::Http {
        internal_port,
        route_subtree_id,
        target_source,
    } = runtime.activation.as_ref()?
    else {
        return None;
    };
    let target = resolve_route_target(container, *internal_port, preferred_network, target_source)?;
    Some(ExpectedRoute {
        subtree_id: route_subtree_id
            .clone()
            .unwrap_or_else(|| format!("forge:{project_id}:{environment}")),
        target,
        domain,
        probe_path: runtime.probe_path.clone(),
    })
}

pub fn resolve_route_target(
    inspection: &ContainerInspection,
    internal_port: u16,
    preferred_network: Option<&str>,
    target_source: &PersistedRouteTargetSource,
) -> Option<String> {
    match target_source {
        PersistedRouteTargetSource::ContainerIp => {
            if let Some(network_name) = preferred_network {
                return inspection
                    .network_ips
                    .get(network_name)
                    .filter(|ip| !ip.is_empty())
                    .map(|ip| format!("{ip}:{internal_port}"));
            }

            inspection
                .network_ips
                .values()
                .find(|ip| !ip.is_empty())
                .map(|ip| format!("{ip}:{internal_port}"))
        }
    }
}
