use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be valid")
        .as_nanos();
    format!("pid-{}-{nanos}", std::process::id())
}
