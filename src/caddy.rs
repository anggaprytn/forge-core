use std::collections::BTreeMap;

use crate::runtime::{RouteInspection, RouteUpdateRequest, RoutingRuntime, RoutingRuntimeError};

pub struct CaddyApiRuntime {
    admin_base_url: String,
    public_base_url: String,
    probe_paths: BTreeMap<String, String>,
}

impl CaddyApiRuntime {
    fn ready_subtree_id() -> &'static str {
        "forge:ready"
    }

    pub fn new(admin_base_url: impl Into<String>, public_base_url: impl Into<String>) -> Self {
        Self {
            admin_base_url: admin_base_url.into().trim_end_matches('/').to_string(),
            public_base_url: public_base_url.into().trim_end_matches('/').to_string(),
            probe_paths: BTreeMap::new(),
        }
    }

    fn ensure_owned_subtree(subtree_id: &str) -> Result<(), RoutingRuntimeError> {
        if subtree_id.starts_with("forge:") {
            Ok(())
        } else {
            Err(RoutingRuntimeError::UpdateFailed(
                "caddy adapter may only mutate forge-owned route subtrees".into(),
            ))
        }
    }

    fn routes_url(&self) -> String {
        format!(
            "{}/config/apps/http/servers/forge/routes",
            self.admin_base_url
        )
    }

    fn load_url(&self) -> String {
        format!("{}/load", self.admin_base_url)
    }

    fn read_full_config(&self) -> Result<serde_json::Value, RoutingRuntimeError> {
        let response = reqwest::blocking::get(format!("{}/config/", self.admin_base_url))
            .map_err(|err| RoutingRuntimeError::InspectionFailed(err.to_string()))?;
        if !response.status().is_success() {
            return Err(RoutingRuntimeError::InspectionFailed(format!(
                "caddy config inspection failed with status {}",
                response.status()
            )));
        }
        response
            .json::<serde_json::Value>()
            .map_err(|err| RoutingRuntimeError::InspectionFailed(err.to_string()))
    }

    fn read_routes(&self) -> Result<Vec<serde_json::Value>, RoutingRuntimeError> {
        let response = reqwest::blocking::get(self.routes_url())
            .map_err(|err| RoutingRuntimeError::InspectionFailed(err.to_string()))?;
        if !response.status().is_success() {
            return Err(RoutingRuntimeError::InspectionFailed(format!(
                "caddy route inspection failed with status {}",
                response.status()
            )));
        }
        response
            .json::<Vec<serde_json::Value>>()
            .map_err(|err| RoutingRuntimeError::InspectionFailed(err.to_string()))
    }

    fn write_routes(&self, routes: &[serde_json::Value]) -> Result<(), RoutingRuntimeError> {
        let mut config = self.read_full_config()?;
        let route_value = serde_json::to_value(routes)
            .map_err(|err| RoutingRuntimeError::UpdateFailed(err.to_string()))?;
        config["apps"]["http"]["servers"]["forge"]["routes"] = route_value;

        let client = reqwest::blocking::Client::new();
        let response = client
            .post(self.load_url())
            .json(&config)
            .send()
            .map_err(|err| RoutingRuntimeError::UpdateFailed(err.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            Err(RoutingRuntimeError::UpdateFailed(format!(
                "caddy route update failed with status {}: {}",
                status, body
            )))
        }
    }

    fn route_json(request: &RouteUpdateRequest) -> serde_json::Value {
        serde_json::json!({
            "@id": request.subtree_id,
            "terminal": true,
            "handle": [{
                "handler": "reverse_proxy",
                "upstreams": [{
                    "dial": request.target
                }]
            }]
        })
    }

    fn order_updated_routes(
        mut routes: Vec<serde_json::Value>,
        updated_subtree_id: &str,
    ) -> Vec<serde_json::Value> {
        if updated_subtree_id == Self::ready_subtree_id() {
            return routes;
        }

        let mut ready_routes = Vec::new();
        routes.retain(|route| {
            if route.get("@id").and_then(|id| id.as_str()) == Some(Self::ready_subtree_id()) {
                ready_routes.push(route.clone());
                false
            } else {
                true
            }
        });
        routes.extend(ready_routes);
        routes
    }

    fn activation_verified(&self, subtree_id: &str) -> bool {
        let path = self
            .probe_paths
            .get(subtree_id)
            .cloned()
            .unwrap_or_else(|| "/".into());
        let url = format!("{}{}", self.public_base_url, path);
        match reqwest::blocking::get(url) {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedRouteUpdate {
    pub request: RouteUpdateRequest,
}

#[derive(Default)]
pub struct RecordingRoutingRuntime {
    pub updates: Vec<RecordedRouteUpdate>,
    pub inspections: Vec<RouteInspection>,
}

impl RecordingRoutingRuntime {
    pub fn with_inspections(inspections: Vec<RouteInspection>) -> Self {
        Self {
            updates: Vec::new(),
            inspections,
        }
    }
}

impl RoutingRuntime for RecordingRoutingRuntime {
    fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
        self.updates.push(RecordedRouteUpdate { request });
        Ok(())
    }

    fn inspect_route(&mut self, _subtree_id: &str) -> Result<RouteInspection, RoutingRuntimeError> {
        if self.inspections.is_empty() {
            return Err(RoutingRuntimeError::InspectionFailed(
                "missing inspection response".into(),
            ));
        }
        Ok(self.inspections.remove(0))
    }

    fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
        Ok(self
            .inspections
            .iter()
            .filter(|route| route.subtree_id.starts_with("forge:"))
            .cloned()
            .collect())
    }

    fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
        Ok(())
    }
}

impl RoutingRuntime for CaddyApiRuntime {
    fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
        Self::ensure_owned_subtree(&request.subtree_id)?;
        let mut routes = self.read_routes()?;
        routes.retain(|route| {
            route.get("@id").and_then(|id| id.as_str()) != Some(request.subtree_id.as_str())
        });
        routes.push(Self::route_json(&request));
        routes = Self::order_updated_routes(routes, &request.subtree_id);
        self.write_routes(&routes)?;
        self.probe_paths.insert(
            request.subtree_id,
            request.probe_path.unwrap_or_else(|| "/".into()),
        );
        Ok(())
    }

    fn inspect_route(&mut self, subtree_id: &str) -> Result<RouteInspection, RoutingRuntimeError> {
        let routes = self.read_routes()?;
        let route = routes
            .into_iter()
            .find(|route| route.get("@id").and_then(|id| id.as_str()) == Some(subtree_id))
            .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing route".into()))?;

        let active_target = route
            .get("handle")
            .and_then(|handle| handle.as_array())
            .and_then(|handle| handle.first())
            .and_then(|handler| handler.get("upstreams"))
            .and_then(|upstreams| upstreams.as_array())
            .and_then(|upstreams| upstreams.first())
            .and_then(|upstream| upstream.get("dial"))
            .and_then(|dial| dial.as_str())
            .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing active target".into()))?
            .to_string();

        Ok(RouteInspection {
            subtree_id: subtree_id.into(),
            active_target,
            activation_verified: self.activation_verified(subtree_id),
            health_checks_enabled: false,
        })
    }

    fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
        let routes = self.read_routes()?;
        let mut managed = Vec::new();
        for route in routes {
            let Some(subtree_id) = route.get("@id").and_then(|id| id.as_str()) else {
                continue;
            };
            if !subtree_id.starts_with("forge:") {
                continue;
            }
            let active_target = route
                .get("handle")
                .and_then(|handle| handle.as_array())
                .and_then(|handle| handle.first())
                .and_then(|handler| handler.get("upstreams"))
                .and_then(|upstreams| upstreams.as_array())
                .and_then(|upstreams| upstreams.first())
                .and_then(|upstream| upstream.get("dial"))
                .and_then(|dial| dial.as_str())
                .unwrap_or_default()
                .to_string();
            managed.push(RouteInspection {
                subtree_id: subtree_id.into(),
                active_target,
                activation_verified: self.activation_verified(subtree_id),
                health_checks_enabled: false,
            });
        }
        Ok(managed)
    }

    fn remove_route(&mut self, subtree_id: &str) -> Result<(), RoutingRuntimeError> {
        Self::ensure_owned_subtree(subtree_id)?;
        let mut routes = self.read_routes()?;
        routes.retain(|route| route.get("@id").and_then(|id| id.as_str()) != Some(subtree_id));
        self.write_routes(&routes)?;
        self.probe_paths.remove(subtree_id);
        Ok(())
    }
}
