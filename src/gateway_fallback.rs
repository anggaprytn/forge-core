use serde_json::json;

pub const FALLBACK_HEADER_NAME: &str = "x-forge-fallback";
pub const ROUTE_STATE_HEADER_NAME: &str = "x-forge-route-state";
pub const FALLBACK_ROUTE_STATE: &str = "fallback";
pub const LEGACY_FALLBACK_BODY: &str = "forge caddy ready";
pub const FALLBACK_TITLE: &str = "Forge route not assigned";
pub const FALLBACK_META_MARKER: &str = r#"<meta name="forge-route-state" content="fallback">"#;
const FALLBACK_HUMAN_MESSAGE: &str = "Gateway reachable, but application route is not active.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayFallbackDetection {
    pub code: &'static str,
    pub message: &'static str,
}

impl GatewayFallbackDetection {
    pub fn summary(&self) -> String {
        format!("{}: {}", self.code, self.message)
    }
}

pub fn detect_from_headers_and_body(
    fallback_header: Option<&str>,
    route_state_header: Option<&str>,
    body: Option<&str>,
) -> Option<GatewayFallbackDetection> {
    if fallback_header
        .map(|value| value.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return Some(GatewayFallbackDetection {
            code: "gateway_fallback_response",
            message: FALLBACK_HUMAN_MESSAGE,
        });
    }
    if route_state_header
        .map(|value| value.trim().eq_ignore_ascii_case(FALLBACK_ROUTE_STATE))
        .unwrap_or(false)
    {
        return Some(GatewayFallbackDetection {
            code: "application_route_not_active",
            message: FALLBACK_HUMAN_MESSAGE,
        });
    }
    let body = body?;
    let lower = body.to_ascii_lowercase();
    if lower.contains(LEGACY_FALLBACK_BODY) {
        return Some(GatewayFallbackDetection {
            code: "gateway_fallback_response",
            message: FALLBACK_HUMAN_MESSAGE,
        });
    }
    if lower.contains(r#"<meta name="forge-route-state" content="fallback">"#)
        || lower.contains("forge route not assigned")
        || lower.contains("application route is not active")
    {
        return Some(GatewayFallbackDetection {
            code: "route_fallback_served",
            message: FALLBACK_HUMAN_MESSAGE,
        });
    }
    None
}

pub fn detect_from_body(body: Option<&str>) -> Option<GatewayFallbackDetection> {
    detect_from_headers_and_body(None, None, body)
}

pub fn fallback_response_body(control_plane_url: Option<&str>) -> String {
    let control_plane_url = control_plane_url.and_then(sanitize_control_plane_url);
    let redirect_meta = control_plane_url.as_deref().map(|url| {
        format!(
            r#"<meta http-equiv="refresh" content="3;url={}">"#,
            escape_html(url)
        )
    });
    let redirect_script = control_plane_url.as_deref().map(|url| {
        format!(
            r#"<script>setTimeout(function(){{window.location.href={};}},3000);</script>"#,
            serde_json::to_string(url).expect("control-plane url should encode as json string")
        )
    });
    let action_copy = control_plane_url.as_deref().map_or_else(String::new, |_| {
        "<p class=\"action\">Redirecting to Forge Control Plane in 3 seconds...</p>".into()
    });
    format!(
        concat!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">",
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
            "{meta}{marker}<title>{title}</title>",
            "<style>",
            ":root{{color-scheme:dark;--bg:#11121a;--panel:#1a1d2a;--ink:#d6d8e8;--muted:#8b93b8;--line:#2b3150;--accent:#7aa2f7;}}",
            "*{{box-sizing:border-box}}body{{margin:0;min-height:100vh;display:grid;place-items:center;background:radial-gradient(circle at top,#1f2335 0,#11121a 55%,#0b0c12 100%);font-family:\"SF Mono\",\"Menlo\",\"Consolas\",monospace;color:var(--ink);padding:24px;}}",
            ".panel{{max-width:720px;width:min(100%,720px);background:rgba(26,29,42,.94);border:1px solid var(--line);border-radius:24px;padding:32px;box-shadow:0 30px 80px rgba(0,0,0,.45);}}",
            "h1{{margin:0 0 14px;font-size:clamp(28px,4vw,40px);line-height:1.1}}p{{margin:0 0 14px;color:var(--muted);line-height:1.7}}",
            "ul{{margin:20px 0;padding-left:18px;color:var(--ink)}}li{{margin:10px 0}}.action{{color:var(--accent);margin-top:24px}}",
            "</style>{script}</head><body><main class=\"panel\"><h1>{title}</h1>",
            "<p>This domain is served by the Forge gateway, but no healthy application route is currently active for it.</p>",
            "<ul><li>The Forge gateway is reachable.</li><li>The application behind this domain is not deployed, not ready, or failed health checks.</li><li>If you just deployed, wait for health checks to complete or inspect deployment status.</li></ul>",
            "{action}</main></body></html>"
        ),
        meta = redirect_meta.unwrap_or_default(),
        marker = FALLBACK_META_MARKER,
        script = redirect_script.unwrap_or_default(),
        title = FALLBACK_TITLE,
        action = action_copy,
    )
}

pub fn fallback_static_response_config(control_plane_url: Option<&str>) -> serde_json::Value {
    json!({
        "handler": "static_response",
        "status_code": 404,
        "headers": {
            FALLBACK_HEADER_NAME: ["true"],
            ROUTE_STATE_HEADER_NAME: [FALLBACK_ROUTE_STATE],
            "Content-Type": ["text/html; charset=utf-8"],
            "Cache-Control": ["no-store"]
        },
        "body": fallback_response_body(control_plane_url)
    })
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn sanitize_control_plane_url(value: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(value).ok()?;
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_legacy_body_marker() {
        let detection = detect_from_body(Some("forge caddy ready")).unwrap();
        assert_eq!(detection.code, "gateway_fallback_response");
    }

    #[test]
    fn detects_meta_marker() {
        let detection = detect_from_body(Some(FALLBACK_META_MARKER)).unwrap();
        assert_eq!(detection.code, "route_fallback_served");
    }

    #[test]
    fn detects_fallback_headers() {
        let detection = detect_from_headers_and_body(Some("true"), Some("fallback"), None).unwrap();
        assert_eq!(detection.code, "gateway_fallback_response");
    }

    #[test]
    fn renders_redirect_only_when_url_present() {
        let with_redirect = fallback_response_body(Some("https://forge.example.com"));
        assert!(with_redirect.contains("Redirecting to Forge Control Plane in 3 seconds"));
        assert!(with_redirect.contains("http-equiv=\"refresh\""));

        let without_redirect = fallback_response_body(None);
        assert!(!without_redirect.contains("http-equiv=\"refresh\""));
        assert!(!without_redirect.contains("Redirecting to Forge Control Plane in 3 seconds"));
    }

    #[test]
    fn fallback_response_sanitizes_control_plane_url() {
        let body = fallback_response_body(Some(
            "https://operator:secret@forge.example.com/path?token=abc123#frag",
        ));
        assert!(body.contains("https://forge.example.com/path"));
        assert!(!body.contains("operator"));
        assert!(!body.contains("secret"));
        assert!(!body.contains("abc123"));
    }

    #[test]
    fn fallback_static_response_config_sets_headers_and_not_found_status() {
        let config = fallback_static_response_config(Some("https://forge.example.com"));
        assert_eq!(config["status_code"].as_u64(), Some(404));
        assert_eq!(
            config["headers"][FALLBACK_HEADER_NAME][0].as_str(),
            Some("true")
        );
        assert_eq!(
            config["headers"][ROUTE_STATE_HEADER_NAME][0].as_str(),
            Some(FALLBACK_ROUTE_STATE)
        );
        let body = config["body"].as_str().unwrap();
        assert!(body.contains(FALLBACK_TITLE));
        assert!(body.contains(FALLBACK_META_MARKER));
        assert!(!body.contains(LEGACY_FALLBACK_BODY));
    }
}
