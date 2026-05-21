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
fn cli_logs_reads_deployment_diagnostics() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"deployment_id":"dep-1","project_id":"api","environment":"staging","lines":["image built"],"lifecycle":["image built","generation promoted"],"container_logs":["Server is running on 0.0.0.0:3000"],"validation_failure_summary":"validating_runtime: http health probe failed","diagnostics_source":"projects/api/environments/staging/generations/1/diagnostics"}}"#,
    );

    let output = run_cli(&url, &["logs", "dep-1"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Deployment: dep-1"));
    assert!(body.contains("Container Logs:"));
    assert!(body.contains("Server is running on 0.0.0.0:3000"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/deployments/dep-1/logs");
}

#[test]
fn cli_project_status_reads_authoritative_runtime_status() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        concat!(
            r#"{"data":{"project_id":"api","environment":"staging","status":"healthy","active_generation":7,"domain":"staging-api.example.com","commit_sha":"340ac8108006d84dbf951d8c0bb04ecfaf0eccac","source_ref":"main","container_name":"staging-api-gen-7","container_running":true,"network_name":"forge-managed","container_ip":"172.29.0.2","route_active":true,"probe_path":"/health","image_ref":"forge/api:staging-gen-7","last_deployment_id":"dep-7","deployed_at_unix":1779320528}}"#
        ),
    );

    let output = run_cli(&url, &["status", "api", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Project: api"));
    assert!(body.contains("Environment: staging"));
    assert!(body.contains("Status: healthy"));
    assert!(body.contains("https://staging-api.example.com"));
    assert!(body.contains("Container: staging-api-gen-7"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/staging/status"
    );
}

#[test]
fn cli_project_status_json_reads_authoritative_runtime_status() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"staging","status":"degraded","active_generation":7,"domain":"staging-api.example.com","container_running":false,"route_active":false}}"#,
    );

    let output = run_cli(&url, &["status", "--json", "api", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"status\": \"degraded\""));
    assert!(body.contains("\"route_active\": false"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/staging/status"
    );
}

#[test]
fn cli_diagnose_reads_environment_diagnostics() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"staging","status":"degraded","active_generation":7,"last_deployment_id":"dep-8","container":{"container_name":"staging-api-gen-7","running":true,"state_status":"running","network_name":"forge-managed","container_ip":"172.29.0.2"},"route":{"route_required":true,"route_active":true,"matches_expected":false,"current_target":"172.29.0.99:3000","expected_target":"172.29.0.2:3000","domain":"staging-api.example.com","mismatch_reason":"route target mismatch: current=172.29.0.99:3000 expected=172.29.0.2:3000"},"probe_target":{"host":"172.29.0.2","port":3000,"path":"/health"},"recent_failures":[{"deployment_id":"dep-8","generation":8,"failure_stage":"validating_runtime","failure_reason":"http health probe failed","validation_failure_summary":"http health probe returned unhealthy (172.29.0.3:3000/health)","diagnostics_source":"projects/api/environments/staging/generations/8/diagnostics"}],"likely_failure_stage":"validating_runtime","diagnostics_source":"projects/api/environments/staging/generations/8/diagnostics"}}"#,
    );

    let output = run_cli(&url, &["diagnose", "api", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Likely Failure Stage:"));
    assert!(body.contains("Route Mismatch:"));
    assert!(body.contains("Recent Failures:"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/staging/diagnostics"
    );
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
fn cli_project_add_posts_project_request() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","repo_url":"https://github.com/example/api.git","default_branch":"main","base_domain":"api-k7x9q2.forge.example.com","domain_mode":"generated","created_at_unix":1,"updated_at_unix":1}}"#,
    );

    let output = run_cli(
        &url,
        &[
            "project",
            "add",
            "api",
            "--repo",
            "https://github.com/example/api.git",
        ],
    );
    assert!(output.status.success());

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/api/projects");
    let json: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(json["project_id"], "api");
    assert_eq!(json["repo_url"], "https://github.com/example/api.git");
    assert_eq!(json["default_branch"], "main");
    assert!(json["base_domain"].is_null());
}

#[test]
fn cli_project_add_allows_repo_only_request() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"forge-fullstack-api-test","repo_url":"https://github.com/anggaprytn/forge-fullstack-api-test.git","default_branch":"main","base_domain":"forge-fullstack-api-test.forge.example.com","domain_mode":"generated","created_at_unix":1,"updated_at_unix":1}}"#,
    );

    let output = run_cli(
        &url,
        &[
            "project",
            "add",
            "--repo",
            "https://github.com/anggaprytn/forge-fullstack-api-test.git",
        ],
    );
    assert!(output.status.success());

    let request = requests.lock().unwrap().remove(0);
    let json: Value = serde_json::from_str(&request.body).unwrap();
    assert!(json["project_id"].is_null());
    assert_eq!(
        json["repo_url"],
        "https://github.com/anggaprytn/forge-fullstack-api-test.git"
    );
}

#[test]
fn cli_project_list_reads_projects() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"projects":[{"project_id":"api","repo_url":"https://github.com/example/api.git","default_branch":"main","base_domain":"api.example.com","domain_mode":"explicit","created_at_unix":1,"updated_at_unix":1}]}}"#,
    );

    let output = run_cli(&url, &["project", "list"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"project_id\": \"api\""));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/projects");
}

#[test]
fn cli_project_show_reads_project() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","repo_url":"https://github.com/example/api.git","default_branch":"main","base_domain":"api.example.com","domain_mode":"explicit","created_at_unix":1,"updated_at_unix":1}}"#,
    );

    let output = run_cli(&url, &["project", "show", "api"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"base_domain\": \"api.example.com\""));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/projects/api");
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
