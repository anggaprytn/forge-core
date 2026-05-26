use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;

use reqwest::header::HOST;

use crate::gateway_fallback::{
    FALLBACK_HEADER_NAME, ROUTE_STATE_HEADER_NAME, detect_from_headers_and_body,
};
use crate::runtime::{RouteInspection, RouteUpdateRequest, RoutingRuntime, RoutingRuntimeError};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActivationVerification {
    verified: bool,
    url: Option<String>,
    host: Option<String>,
    status_code: Option<u16>,
    response_body: Option<String>,
}

pub struct CaddyApiRuntime {
    admin_base_url: String,
    public_base_url: String,
    probe_paths: BTreeMap<String, String>,
    client: reqwest::blocking::Client,
    insecure_client: reqwest::blocking::Client,
}

impl CaddyApiRuntime {
    const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
    const ACTIVATION_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
    const REQUEST_RETRY_ATTEMPTS: usize = 3;
    const REQUEST_RETRY_DELAY: Duration = Duration::from_millis(100);

    fn ready_subtree_id() -> &'static str {
        "forge:ready"
    }

    pub fn new(admin_base_url: impl Into<String>, public_base_url: impl Into<String>) -> Self {
        Self {
            admin_base_url: admin_base_url.into().trim_end_matches('/').to_string(),
            public_base_url: public_base_url.into().trim_end_matches('/').to_string(),
            probe_paths: BTreeMap::new(),
            client: reqwest::blocking::Client::builder()
                .timeout(Self::ADMIN_REQUEST_TIMEOUT)
                .build()
                .expect("reqwest blocking client should build"),
            insecure_client: reqwest::blocking::Client::builder()
                .timeout(Self::ACTIVATION_PROBE_TIMEOUT)
                .danger_accept_invalid_certs(true)
                .build()
                .expect("reqwest blocking client should build"),
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
        let response = self.retry_client_request(
            || {
                self.client
                    .get(format!("{}/config/", self.admin_base_url))
                    .send()
            },
            false,
        )?;
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
        let response =
            self.retry_client_request(|| self.client.get(self.routes_url()).send(), false)?;
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

        let response = self.retry_client_request(
            || self.client.post(self.load_url()).json(&config).send(),
            true,
        )?;
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
        let mut route = serde_json::json!({
            "@id": request.subtree_id,
            "terminal": true,
            "handle": [{
                "handler": "reverse_proxy",
                "upstreams": [{
                    "dial": request.target
                }]
            }]
        });
        if let Some(domain) = request.domain.as_deref() {
            route["match"] = serde_json::json!([{
                "host": [domain]
            }]);
        }
        route
    }

    fn route_order_bucket(route: &serde_json::Value) -> u8 {
        if route.get("@id").and_then(|id| id.as_str()) == Some(Self::ready_subtree_id()) {
            return 2;
        }
        if route_has_explicit_host_matcher(route) {
            return 0;
        }
        1
    }

    fn order_updated_routes(mut routes: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
        routes.sort_by_key(Self::route_order_bucket);
        routes
    }

    fn activation_probe_domain(&self, route: &serde_json::Value) -> Option<String> {
        route_matchers(route)
            .iter()
            .find_map(|matcher| {
                matcher
                    .get("host")
                    .and_then(|hosts| hosts.as_array())
                    .and_then(|hosts| hosts.first())
                    .and_then(|host| host.as_str())
            })
            .map(ToOwned::to_owned)
    }

    fn build_route_inspection(
        &self,
        routes: &[serde_json::Value],
        route_index: usize,
        subtree_id: &str,
    ) -> Result<RouteInspection, RoutingRuntimeError> {
        let route = &routes[route_index];
        let active_target = route_active_target(route)
            .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing active target".into()))?;
        let domain = self.activation_probe_domain(route);
        let probe_path = self
            .probe_paths
            .get(subtree_id)
            .map(String::as_str)
            .unwrap_or("/");
        let shadowed = routes[..route_index]
            .iter()
            .filter(|candidate| {
                candidate.get("terminal").and_then(|value| value.as_bool()) == Some(true)
            })
            .any(|candidate| route_matches_request(candidate, domain.as_deref(), probe_path));
        let mut verification = self.activation_verification(subtree_id, domain.as_deref());
        if shadowed {
            verification.verified = false;
            let note = "route shadowed by an earlier terminal matcher";
            verification.response_body = Some(match verification.response_body.take() {
                Some(body) if !body.is_empty() => format!("{body}; {note}"),
                _ => note.to_string(),
            });
        }

        Ok(RouteInspection {
            subtree_id: subtree_id.into(),
            active_target,
            domain,
            activation_verified: verification.verified,
            verification_url: verification.url,
            verification_host: verification.host,
            verification_status_code: verification.status_code,
            verification_response_body: verification.response_body,
            health_checks_enabled: false,
        })
    }

    fn activation_verification(
        &self,
        subtree_id: &str,
        domain: Option<&str>,
    ) -> ActivationVerification {
        let path = self
            .probe_paths
            .get(subtree_id)
            .cloned()
            .unwrap_or_else(|| "/".into());
        let url = format!("{}{}", self.public_base_url, path);
        let initial = self.send_activation_probe(&url, domain, false);
        if should_retry_https(&initial, &url) {
            let https_url = url.replacen("http://", "https://", 1);
            return self.send_activation_probe(&https_url, domain, true);
        }
        initial
    }

    fn send_activation_probe(
        &self,
        url: &str,
        domain: Option<&str>,
        allow_invalid_certs: bool,
    ) -> ActivationVerification {
        let client = if allow_invalid_certs {
            &self.insecure_client
        } else {
            &self.client
        };
        let mut request = client.get(url);
        if let Some(domain) = domain {
            request = request.header(HOST, domain);
        }
        match self.retry_client_request(
            || request.try_clone().expect("request should clone").send(),
            false,
        ) {
            Ok(response) => {
                let status = response.status();
                let fallback_header = response
                    .headers()
                    .get(FALLBACK_HEADER_NAME)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned);
                let route_state_header = response
                    .headers()
                    .get(ROUTE_STATE_HEADER_NAME)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned);
                let body = response.text().ok();
                let fallback = detect_from_headers_and_body(
                    fallback_header.as_deref(),
                    route_state_header.as_deref(),
                    body.as_deref(),
                );
                ActivationVerification {
                    verified: status.is_success() && fallback.is_none(),
                    url: Some(url.to_string()),
                    host: domain.map(ToOwned::to_owned),
                    status_code: Some(status.as_u16()),
                    response_body: match (body, fallback) {
                        (Some(body), Some(fallback)) if body.trim().is_empty() => {
                            Some(fallback.summary())
                        }
                        (Some(body), Some(fallback)) => {
                            Some(format!("{}; {}", fallback.summary(), body))
                        }
                        (body, None) => body,
                        (None, Some(fallback)) => Some(fallback.summary()),
                    },
                }
            }
            Err(err) => ActivationVerification {
                verified: false,
                url: Some(url.to_string()),
                host: domain.map(ToOwned::to_owned),
                status_code: None,
                response_body: Some(err.to_string()),
            },
        }
    }

    fn retry_client_request<F>(
        &self,
        mut send: F,
        update_error: bool,
    ) -> Result<reqwest::blocking::Response, RoutingRuntimeError>
    where
        F: FnMut() -> Result<reqwest::blocking::Response, reqwest::Error>,
    {
        let mut last_error = None;
        for attempt in 0..Self::REQUEST_RETRY_ATTEMPTS {
            match send() {
                Ok(response) => return Ok(response),
                Err(err) => {
                    last_error = Some(err);
                    if attempt + 1 < Self::REQUEST_RETRY_ATTEMPTS {
                        thread::sleep(Self::REQUEST_RETRY_DELAY);
                    }
                }
            }
        }
        let message = last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "request failed".into());
        Err(if update_error {
            RoutingRuntimeError::UpdateFailed(message)
        } else {
            RoutingRuntimeError::InspectionFailed(message)
        })
    }
}

fn should_retry_https(result: &ActivationVerification, url: &str) -> bool {
    url.starts_with("http://")
        && result.status_code == Some(400)
        && result
            .response_body
            .as_deref()
            .is_some_and(|body| body.contains("HTTP request to an HTTPS server"))
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
    fn probe_control_plane(&mut self) -> Result<(), RoutingRuntimeError> {
        Ok(())
    }

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
    fn probe_control_plane(&mut self) -> Result<(), RoutingRuntimeError> {
        let response =
            self.retry_client_request(|| self.client.get(self.routes_url()).send(), false)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(RoutingRuntimeError::InspectionFailed(format!(
                "caddy admin probe failed with status {}",
                response.status()
            )))
        }
    }

    fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
        Self::ensure_owned_subtree(&request.subtree_id)?;
        let mut routes = self.read_routes()?;
        routes.retain(|route| {
            route.get("@id").and_then(|id| id.as_str()) != Some(request.subtree_id.as_str())
        });
        routes.push(Self::route_json(&request));
        routes = Self::order_updated_routes(routes);
        self.write_routes(&routes)?;
        self.probe_paths.insert(
            request.subtree_id,
            request.probe_path.unwrap_or_else(|| "/".into()),
        );
        Ok(())
    }

    fn inspect_route(&mut self, subtree_id: &str) -> Result<RouteInspection, RoutingRuntimeError> {
        let routes = self.read_routes()?;
        let route_index = routes
            .iter()
            .position(|route| route.get("@id").and_then(|id| id.as_str()) == Some(subtree_id))
            .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing route".into()))?;
        self.build_route_inspection(&routes, route_index, subtree_id)
    }

    fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
        let routes = self.read_routes()?;
        let mut managed = Vec::new();
        for (index, route) in routes.iter().enumerate() {
            let Some(subtree_id) = route.get("@id").and_then(|id| id.as_str()) else {
                continue;
            };
            if !subtree_id.starts_with("forge:") {
                continue;
            }
            if route_active_target(route).is_none() {
                continue;
            }
            managed.push(self.build_route_inspection(&routes, index, subtree_id)?);
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

fn route_matchers(route: &serde_json::Value) -> &[serde_json::Value] {
    route
        .get("match")
        .and_then(|value| value.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn route_has_explicit_host_matcher(route: &serde_json::Value) -> bool {
    route_matchers(route).iter().any(|matcher| {
        matcher
            .get("host")
            .and_then(|hosts| hosts.as_array())
            .is_some()
    })
}

fn route_active_target(route: &serde_json::Value) -> Option<String> {
    route
        .get("handle")
        .and_then(|handle| handle.as_array())
        .and_then(|handle| handle.first())
        .and_then(|handler| handler.get("upstreams"))
        .and_then(|upstreams| upstreams.as_array())
        .and_then(|upstreams| upstreams.first())
        .and_then(|upstream| upstream.get("dial"))
        .and_then(|dial| dial.as_str())
        .map(ToOwned::to_owned)
}

fn route_matches_request(route: &serde_json::Value, host: Option<&str>, path: &str) -> bool {
    let matchers = route_matchers(route);
    if matchers.is_empty() {
        return true;
    }
    matchers
        .iter()
        .any(|matcher| matcher_matches_request(matcher, host, path))
}

fn matcher_matches_request(matcher: &serde_json::Value, host: Option<&str>, path: &str) -> bool {
    let host_matches = match matcher.get("host").and_then(|value| value.as_array()) {
        Some(hosts) => host.is_some_and(|expected| {
            hosts
                .iter()
                .filter_map(|value| value.as_str())
                .any(|candidate| candidate == expected)
        }),
        None => true,
    };
    let path_matches = match matcher.get("path").and_then(|value| value.as_array()) {
        Some(paths) => paths
            .iter()
            .filter_map(|value| value.as_str())
            .any(|pattern| path_pattern_matches(pattern, path)),
        None => true,
    };
    host_matches && path_matches
}

fn path_pattern_matches(pattern: &str, path: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        path.starts_with(prefix)
    } else {
        path == pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway_fallback::fallback_response_body;

    fn proxy_route(id: &str, host: Option<&str>) -> serde_json::Value {
        let request = RouteUpdateRequest {
            subtree_id: id.into(),
            target: "target:3000".into(),
            domain: host.map(ToOwned::to_owned),
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        };
        CaddyApiRuntime::route_json(&request)
    }

    #[test]
    fn app_routes_include_host_matcher_when_domain_available() {
        let route = proxy_route("forge:api:production", Some("api.example.com"));
        assert_eq!(
            route["match"][0]["host"][0].as_str(),
            Some("api.example.com")
        );
        assert_eq!(route["terminal"].as_bool(), Some(true));
    }

    #[test]
    fn caddy_routes_order_host_specific_before_fallback() {
        let ordered = CaddyApiRuntime::order_updated_routes(vec![
            proxy_route("forge:api:production", None),
            proxy_route("forge:staging:staging", Some("staging.example.com")),
            serde_json::json!({
                "@id": "forge:ready",
                "terminal": true,
                "handle": [{"handler": "static_response", "status_code": 200}]
            }),
        ]);

        let ids: Vec<_> = ordered
            .iter()
            .filter_map(|route| route.get("@id").and_then(|id| id.as_str()))
            .collect();
        assert_eq!(
            ids,
            vec![
                "forge:staging:staging",
                "forge:api:production",
                "forge:ready"
            ]
        );
    }

    #[test]
    fn route_activation_verification_detects_shadowing() {
        let legacy = proxy_route("forge:api:production", None);
        let staged = proxy_route("forge:api:staging", Some("staging.example.com"));
        assert!(route_matches_request(
            &legacy,
            Some("staging.example.com"),
            "/health"
        ));
        assert!(!route_matches_request(
            &serde_json::json!({
                "@id": "external:preserve",
                "terminal": true,
                "match": [{"path": ["/preserve"]}],
                "handle": [{"handler": "static_response", "status_code": 204}]
            }),
            Some("staging.example.com"),
            "/health"
        ));

        let runtime = CaddyApiRuntime::new("http://127.0.0.1:2019", "http://127.0.0.1");
        let inspection = runtime
            .build_route_inspection(&[legacy, staged], 1, "forge:api:staging")
            .unwrap();
        assert!(!inspection.activation_verified);
        assert!(
            inspection
                .verification_response_body
                .as_deref()
                .is_some_and(|body| body.contains("shadowed"))
        );
    }

    #[test]
    fn route_activation_verification_rejects_fallback_body() {
        let result = ActivationVerification {
            verified: false,
            url: Some("https://app.example.com/health".into()),
            host: Some("app.example.com".into()),
            status_code: Some(200),
            response_body: Some(fallback_response_body(None)),
        };
        assert!(!result.verified);
        assert!(
            result
                .response_body
                .as_deref()
                .is_some_and(|body| body.contains("Forge route not assigned"))
        );
    }
}
