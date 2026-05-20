use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::{env, fs};

use serde_json::Value;
use serde_yaml::Value as YamlValue;

#[derive(Debug, Clone)]
struct CapturedRequest {
    method: String,
    path: String,
    authorization: String,
    body: String,
}

#[test]
fn cli_deploy_posts_deployment_request() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"deployment_id":"dep-1","queue_position":1}}"#,
    );

    let output = run_cli(&url, &["deploy", "api", "production"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"deployment_id\": \"dep-1\""));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/deployments");
    assert_eq!(request.authorization, "Bearer test-token");
    let json: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(json["project_id"], "api");
    assert_eq!(json["environment"], "production");
    assert_eq!(json["intent"], "deploy");
}

#[test]
fn cli_status_reads_deployment_status() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"deployment_id":"dep-1","project_id":"api","environment":"production","state":"healthy"}}"#,
    );

    let output = run_cli(&url, &["status", "dep-1"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"state\": \"healthy\""));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/deployments/dep-1");
}

#[test]
fn cli_events_reads_event_stream() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"events":[{"timestamp_unix":1,"project_id":"api","environment":"production","generation":1,"deployment_id":"dep-1","event_type":"DEPLOYMENT_STARTED","reason":null}]}}"#,
    );

    let output = run_cli(&url, &["events"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("DEPLOYMENT_STARTED"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/events");
}

#[test]
fn cli_rollback_posts_rollback_intent() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"deployment_id":"dep-rollback","queue_position":1}}"#,
    );

    let output = run_cli(&url, &["rollback", "api", "production"]);
    assert!(output.status.success());

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/deployments");
    let json: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(json["intent"], "rollback");
}

#[test]
fn cli_secrets_set_writes_secret_without_echoing_value() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"secret_id":"api:production:DATABASE_URL"}}"#,
    );

    let output = run_cli(
        &url,
        &[
            "secrets",
            "set",
            "api",
            "production",
            "DATABASE_URL",
            "postgres://supersecretvalue",
        ],
    );
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("api:production:DATABASE_URL"));
    assert!(!stdout.contains("postgres://supersecretvalue"));
    assert!(!stderr.contains("postgres://supersecretvalue"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/secrets");
    let json: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(json["key"], "DATABASE_URL");
    assert_eq!(json["value"], "postgres://supersecretvalue");
}

#[test]
fn cli_doctor_reports_local_diagnostics() {
    let root = test_root("cli-doctor");
    fs::create_dir_all(root.join("queue")).unwrap();
    fs::create_dir_all(root.join("projects")).unwrap();
    let config_path = root.join("forge.conf");
    fs::write(
        &config_path,
        format!(
            "storage_root={}\napi_bind=127.0.0.1:1\nbearer_token=test-token\n",
            root.display()
        ),
    )
    .unwrap();

    unsafe {
        env::set_var(
            "FORGE_MASTER_KEY",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
        );
    }

    let output = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--caddy-admin-url",
            "http://127.0.0.1:1",
            "doctor",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[OK] Storage root writable"));
    assert!(stdout.contains("[OK] Queue root exists"));
    assert!(stdout.contains("[OK] Snapshot root exists"));
    assert!(stdout.contains("[OK] API token configured"));
}

#[test]
fn init_creates_forge_yml() {
    let root = test_root("cli-init-creates");

    let output = run_cli_in_dir(&root, &["init"]);
    assert!(output.status.success());

    let rendered = fs::read_to_string(root.join("forge.yml")).unwrap();
    assert_eq!(
        rendered,
        concat!(
            "version: 1\n",
            "name: api\n",
            "type: web\n",
            "\n",
            "build:\n",
            "  dockerfile: Dockerfile\n",
            "  context: .\n",
            "\n",
            "runtime:\n",
            "  port: 3000\n",
            "  healthcheck:\n",
            "    path: /health\n",
            "    expected_status: 200\n",
            "\n",
            "invariants:\n",
            "  - name: health\n",
            "    path: /health\n",
            "    expect_status: 200\n",
        )
    );
}

#[test]
fn init_refuses_to_overwrite_existing_file() {
    let root = test_root("cli-init-refuses-overwrite");
    fs::write(root.join("forge.yml"), "version: 999\n").unwrap();

    let output = run_cli_in_dir(&root, &["init"]);
    assert!(!output.status.success());
    assert_eq!(
        fs::read_to_string(root.join("forge.yml")).unwrap(),
        "version: 999\n"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("forge.yml already exists"));
    assert!(stderr.contains("--force"));
}

#[test]
fn init_force_overwrites_existing_file() {
    let root = test_root("cli-init-force");
    fs::write(root.join("forge.yml"), "version: 999\n").unwrap();

    let output = run_cli_in_dir(&root, &["init", "--force"]);
    assert!(output.status.success());

    let rendered = fs::read_to_string(root.join("forge.yml")).unwrap();
    assert!(rendered.contains("version: 1"));
    assert!(rendered.contains("name: api"));
}

#[test]
fn init_output_is_valid_yaml() {
    let root = test_root("cli-init-yaml");

    let output = run_cli_in_dir(&root, &["init"]);
    assert!(output.status.success());

    let rendered = fs::read_to_string(root.join("forge.yml")).unwrap();
    let yaml: YamlValue = serde_yaml::from_str(&rendered).unwrap();
    assert_eq!(yaml["version"].as_u64(), Some(1));
    assert_eq!(yaml["name"].as_str(), Some("api"));
    assert_eq!(yaml["type"].as_str(), Some("web"));
}

#[test]
fn cli_config_env_vars_override_saved_config() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"deployment_id":"dep-1","queue_position":1}}"#,
    );
    let config_root = test_root("cli-config-override");
    let forge_config_dir = config_root.join("forge");
    fs::create_dir_all(&forge_config_dir).unwrap();
    fs::write(
        forge_config_dir.join("config.toml"),
        concat!(
            "server_url = \"http://127.0.0.1:9\"\n",
            "token = \"saved-token\"\n",
        ),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["deploy", "api", "production"])
        .env("XDG_CONFIG_HOME", &config_root)
        .env("FORGE_URL", &url)
        .env("FORGE_TOKEN", "env-token")
        .output()
        .unwrap();
    assert!(output.status.success());

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.path, "/deployments");
    assert_eq!(request.authorization, "Bearer env-token");
}

fn run_cli(url: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(args)
        .env("FORGE_URL", url)
        .env("FORGE_TOKEN", "test-token")
        .output()
        .unwrap()
}

fn run_cli_in_dir(workdir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(args)
        .current_dir(workdir)
        .output()
        .unwrap()
}

fn spawn_server(
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    response_body: &'static str,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = Vec::new();
        let mut temp = [0u8; 4096];
        loop {
            let read = stream.read(&mut temp).unwrap();
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&temp[..read]);
            if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                let header_end = buffer
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .unwrap()
                    + 4;
                let headers = String::from_utf8_lossy(&buffer[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            Some(value.trim().parse::<usize>().unwrap())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                while buffer.len() < header_end + content_length {
                    let read = stream.read(&mut temp).unwrap();
                    if read == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&temp[..read]);
                }
                break;
            }
        }

        let request = parse_request(&buffer);
        requests.lock().unwrap().push(request);

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    (url, handle)
}

fn test_root(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let base = std::env::temp_dir().join(format!(
        "forge-core-tests-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&base).unwrap();
    base
}

fn parse_request(buffer: &[u8]) -> CapturedRequest {
    let header_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4;
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = headers.lines();
    let request_line = lines.next().unwrap();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap().to_string();
    let path = parts.next().unwrap().to_string();
    let authorization = lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("authorization") {
                Some(value.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_default();
    let body = String::from_utf8_lossy(&buffer[header_end..]).to_string();
    CapturedRequest {
        method,
        path,
        authorization,
        body,
    }
}
