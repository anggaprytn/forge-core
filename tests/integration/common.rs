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

pub fn ensure_docker_available() -> bool {
    let Ok(output) = Command::new("docker").args(["ps", "-q"]).output() else {
        eprintln!("skipping integration test: docker executable unavailable");
        return false;
    };

    if output.status.success() {
        true
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let reason = stderr.trim();
        if reason.is_empty() {
            eprintln!("skipping integration test: docker daemon unavailable");
        } else {
            eprintln!("skipping integration test: docker daemon unavailable: {reason}");
        }
        false
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
pub fn bad_http_app_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("bad-http-app")
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
