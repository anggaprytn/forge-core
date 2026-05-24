use std::env;
use std::fs;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::Url;
use serde_json::Value;

pub fn integration_enabled() -> bool {
    env::var("FORGE_INTEGRATION").ok().as_deref() == Some("1")
}

pub fn ensure_integration_enabled() -> bool {
    if integration_enabled() {
        true
    } else {
        eprintln!("skipping integration test: FORGE_INTEGRATION != 1");
        false
    }
}

#[allow(dead_code)]
pub fn require_docker_available() {
    if let Err(reason) = docker_unavailable_reason() {
        panic!(
            "integration test requested with FORGE_INTEGRATION=1, but docker is unavailable: {reason}"
        );
    }
}

#[allow(dead_code)]
pub fn ensure_docker_available() -> bool {
    match docker_unavailable_reason() {
        Ok(()) => true,
        Err(reason) => {
            eprintln!("skipping integration test: docker unavailable: {reason}");
            false
        }
    }
}

fn docker_unavailable_reason() -> Result<(), String> {
    let output = Command::new("docker")
        .args(["ps", "-q"])
        .output()
        .map_err(|_| "docker executable unavailable".to_string())?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let reason = stderr.trim();
    if reason.is_empty() {
        Err("docker daemon unavailable".to_string())
    } else {
        Err(format!("docker daemon unavailable: {reason}"))
    }
}

pub fn runtime_root(test_name: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-runtime")
        .join(test_name)
        .join(unique_suffix());
    fs::create_dir_all(&root).expect("integration runtime root should be creatable");
    root
}

pub fn sample_http_app_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sample-http-app")
}

#[allow(dead_code)]
pub fn redis_http_app_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("redis-http-app")
}

#[allow(dead_code)]
pub fn bad_http_app_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("bad-http-app")
}

#[allow(dead_code)]
pub fn secret_http_app_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("secret-http-app")
}

#[allow(dead_code)]
pub fn secret_http_bad_app_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("secret-http-bad-app")
}

#[allow(dead_code)]
pub fn available_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("ephemeral port should be allocatable")
        .local_addr()
        .expect("ephemeral port should expose local addr")
        .port()
}

#[allow(dead_code)]
pub fn wait_for_tcp_accept(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_error = "no connection attempts".to_string();
    loop {
        let addresses = (host, port)
            .to_socket_addrs()
            .map_err(|err| format!("failed to resolve {host}:{port}: {err}"))?
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(format!("failed to resolve {host}:{port}: no addresses"));
        }
        for address in addresses {
            match TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
                Ok(stream) => {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    return Ok(());
                }
                Err(err) => last_error = format!("{address}: {err}"),
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "tcp endpoint {host}:{port} did not accept connections within {timeout:?}: {last_error}"
            ));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[allow(dead_code)]
pub fn wait_for_http_readyz(url: &str, timeout: Duration) -> Result<(), String> {
    let parsed = Url::parse(url).map_err(|err| format!("invalid url `{url}`: {err}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| format!("url `{url}` is missing a host"))?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| format!("url `{url}` is missing a port"))?;
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_millis(200))
        .timeout(Duration::from_millis(500))
        .build()
        .map_err(|err| format!("failed to build http client: {err}"))?;
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    loop {
        match wait_for_tcp_accept(&host, port, Duration::from_millis(200)) {
            Ok(()) => {}
            Err(err) => {
                last_error = Some(err);
                if Instant::now() >= deadline {
                    return Err(format!(
                        "http endpoint {url} did not become ready within {timeout:?}: {}",
                        last_error.unwrap_or_else(|| "no http attempts".to_string())
                    ));
                }
                thread::sleep(Duration::from_millis(25));
                continue;
            }
        }
        match client.get(url).send() {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => last_error = Some(format!("status {}", response.status())),
            Err(err) => last_error = Some(err.to_string()),
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "http endpoint {url} did not become ready within {timeout:?}: {}",
                last_error.unwrap_or_else(|| "no http attempts".to_string())
            ));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[allow(dead_code)]
pub fn wait_for_container_health_or_port(
    container_name: &str,
    port: u16,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_observation = "no docker inspect attempts".to_string();
    loop {
        let output = Command::new("docker")
            .args(["inspect", container_name])
            .output()
            .map_err(|err| format!("docker inspect failed for {container_name}: {err}"))?;
        if !output.status.success() {
            last_observation = String::from_utf8_lossy(&output.stderr).trim().to_string();
        } else {
            match serde_json::from_slice::<Vec<Value>>(&output.stdout) {
                Ok(inspections) if !inspections.is_empty() => {
                    let inspection = &inspections[0];
                    let running = inspection["State"]["Running"].as_bool().unwrap_or(false);
                    let health = inspection["State"]["Health"]["Status"]
                        .as_str()
                        .unwrap_or("");
                    if health == "healthy" {
                        return Ok(());
                    }
                    if running {
                        match container_accepts_port(container_name, port) {
                            Ok(()) => return Ok(()),
                            Err(err) => {
                                last_observation =
                                    format!("running={running} health={health} port_probe={err}");
                            }
                        }
                    } else {
                        last_observation = format!("running={running} health={health}");
                    }
                }
                Ok(_) => last_observation = "docker inspect returned no records".to_string(),
                Err(err) => {
                    last_observation = format!("failed to decode docker inspect json: {err}")
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "container {container_name} did not report healthy or accept tcp/{port} within {timeout:?}: {last_observation}"
            ));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn container_accepts_port(container_name: &str, port: u16) -> Result<(), String> {
    let probe = format!(
        "if command -v python3 >/dev/null 2>&1; then \
             python3 -c \"import socket; s = socket.create_connection(('127.0.0.1', {port}), 1); s.close()\"; \
         elif command -v python >/dev/null 2>&1; then \
             python -c \"import socket; s = socket.create_connection(('127.0.0.1', {port}), 1); s.close()\"; \
         elif command -v nc >/dev/null 2>&1; then \
             nc -z 127.0.0.1 {port}; \
         elif command -v busybox >/dev/null 2>&1; then \
             busybox nc -z 127.0.0.1 {port}; \
         elif command -v redis-cli >/dev/null 2>&1; then \
             redis-cli -p {port} ping >/dev/null; \
         else \
             exit 127; \
         fi"
    );
    let output = Command::new("docker")
        .args(["exec", container_name, "sh", "-lc", &probe])
        .output()
        .map_err(|err| format!("docker exec failed for {container_name}: {err}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stderr.is_empty() {
        Err(stderr)
    } else if !stdout.is_empty() {
        Err(stdout)
    } else {
        Err(format!("probe exited with status {}", output.status))
    }
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be valid")
        .as_nanos();
    format!("pid-{}-{nanos}", std::process::id())
}
