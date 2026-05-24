use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::{env, fs};

use serde_json::Value;
use serde_yaml::Value as YamlValue;

use forge_core::storage::{LeaderLeaseStore, NodeMetadataStore};

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
fn cli_decodes_multiservice_status_response() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        concat!(
            r#"{"data":{"project_id":"forge-multiservice-test","environment":"staging","status":"healthy","active_generation":7,"domain":"staging-forge-multiservice-test.example.com","container_name":"staging-forge-multiservice-test-api-gen-7","container_running":true,"route_active":true,"startup_order":["api","worker"],"services":[{"service_id":"api","role":"exposed","depends_on":[],"dns_aliases":["api"],"container_name":"staging-forge-multiservice-test-api-gen-7","image_ref":"forge/api:staging-gen-7","running":true,"state_status":"running","network_name":"forge-managed","container_ip":"172.29.0.2","internal_port":3000,"probe_path":"/health","route":"active","health":"running","logs_tail":["api booted"]},{"service_id":"worker","role":"internal","depends_on":["api"],"dns_aliases":["worker"],"container_name":"staging-forge-multiservice-test-worker-gen-7","image_ref":"forge/worker:staging-gen-7","running":true,"state_status":"running","network_name":"forge-managed","container_ip":"172.29.0.3","route":"none","health":"running","logs_tail":["worker polling"]}]}}"#
        ),
    );

    let output = run_cli(&url, &["status", "forge-multiservice-test", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Services:"));
    assert!(body.contains("worker"));
    assert!(body.contains("depends_on: api"));
    assert!(body.contains("dns_aliases: worker"));
}

#[test]
fn cli_decodes_multiservice_diagnostics_response() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        concat!(
            r#"{"data":{"project_id":"forge-multiservice-test","environment":"staging","status":"degraded","active_generation":7,"last_deployment_id":"dep-ms-7","container":{"container_name":"staging-forge-multiservice-test-api-gen-7","running":true,"state_status":"running","network_name":"forge-managed","container_ip":"172.29.0.2"},"route":{"route_required":true,"route_active":true,"matches_expected":true,"current_target":"172.29.0.2:3000","expected_target":"172.29.0.2:3000","domain":"staging-forge-multiservice-test.example.com"},"probe_target":{"host":"172.29.0.2","port":3000,"path":"/health"},"startup_order":["api","worker"],"services":[{"service_id":"api","role":"exposed","depends_on":[],"dns_aliases":["api"],"container_name":"staging-forge-multiservice-test-api-gen-7","image_ref":"forge/api:staging-gen-7","running":true,"state_status":"running","network_name":"forge-managed","container_ip":"172.29.0.2","internal_port":3000,"probe_path":"/health","route":"active","health":"running","logs_tail":["api booted"]},{"service_id":"worker","role":"internal","depends_on":["api"],"dns_aliases":["worker"],"container_name":"staging-forge-multiservice-test-worker-gen-7","image_ref":"forge/worker:staging-gen-7","running":true,"state_status":"running","network_name":"forge-managed","container_ip":"172.29.0.3","route":"none","health":"failed","failure_reason":"worker queue disconnected","logs_tail":["worker retrying"]}],"recent_failures":[{"deployment_id":"dep-ms-7","generation":7,"failure_stage":"warming","failure_reason":"worker queue disconnected","diagnostics_source":"projects/forge-multiservice-test/environments/staging/generations/7/diagnostics"}],"likely_failure_stage":"warming","diagnostics_source":"projects/forge-multiservice-test/environments/staging/generations/7/diagnostics"}}"#
        ),
    );

    let output = run_cli(&url, &["diagnose", "forge-multiservice-test", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Services:"));
    assert!(body.contains("worker queue disconnected"));
    assert!(body.contains("worker retrying"));
}

#[test]
fn status_response_backward_compatible_for_single_service() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"staging","status":"healthy","container_name":"staging-api-gen-7","route_active":true}}"#,
    );

    let output = run_cli(&url, &["status", "--json", "api", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"project_id\": \"api\""));
    assert!(body.contains("\"domain\": \"\""));
    assert!(body.contains("\"container_running\": false"));
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
fn cli_history_reads_environment_history() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"staging","entries":[{"generation":7,"deployment_id":"dep-7","commit_sha":"340ac8108006d84dbf951d8c0bb04ecfaf0eccac","source_ref":"main","image_ref":"forge/api:staging-gen-7","created_at_unix":1779320500,"promoted_at_unix":1779320528,"finalized_state":"healthy","finalized_at_unix":1779320520,"rollback_target":false,"restored_by_rollback":false,"retained":true,"eligible_for_gc":false,"missing_artifacts":false,"retained_reasons":["current/promoted generation"],"lifecycle_state":"promoted","retention_role":"current"}]}}"#,
    );

    let output = run_cli(&url, &["history", "api", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Generation 7"));
    assert!(body.contains("retained: yes"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/staging/history"
    );
}

#[test]
fn deployments_cli_renders_unambiguous_status_labels() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"staging","entries":[{"generation":30,"deployment_id":"dep-30","created_at_unix":1779320500,"promoted_at_unix":1779320528,"rollback_target":false,"restored_by_rollback":false,"retained":true,"eligible_for_gc":false,"missing_artifacts":false,"retained_reasons":["current/promoted generation"],"lifecycle_state":"promoted","retention_role":"current"},{"generation":29,"deployment_id":"dep-29","created_at_unix":1779320400,"promoted_at_unix":1779320428,"rollback_target":true,"restored_by_rollback":false,"retained":true,"eligible_for_gc":false,"missing_artifacts":false,"retained_reasons":["rollback-safe generation"],"lifecycle_state":"promoted","retention_role":"rollback_target"},{"generation":27,"deployment_id":"dep-27","created_at_unix":1779320200,"promoted_at_unix":1779320228,"rollback_target":false,"restored_by_rollback":false,"retained":true,"eligible_for_gc":false,"missing_artifacts":false,"retained_reasons":["recent healthy finalized generation"],"lifecycle_state":"promoted","retention_role":"retained"}]}}"#,
    );

    let output = run_cli(&url, &["history", "api", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("status: active"));
    assert!(body.contains("status: rollback_target"));
    assert!(body.contains("status: historical_promoted"));
    assert!(body.contains("retention_role: current"));
    assert!(body.contains("lifecycle_state: promoted"));
    assert!(!body.contains("status: promoted\n"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/staging/history"
    );
}

#[test]
fn cli_env_reports_redacted_secret_keys() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"staging","generation":7,"deployment_id":"dep-7","source_environment":"staging","values":[{"key":"FORGE_PROJECT_ID","value":"api","source":"forge_generated","generated":true,"redacted":false},{"key":"API_BASE_URL","value":"https://api.example.com","source":"forge_yml","generated":false,"redacted":false},{"key":"DATABASE_URL","value":"<secret>","source":"project_environment_secret","generated":false,"redacted":true}]}}"#,
    );

    let output = run_cli(&url, &["env", "api", "staging"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("FORGE_PROJECT_ID=api"));
    assert!(body.contains("API_BASE_URL=https://api.example.com"));
    assert!(body.contains("DATABASE_URL=<secret>"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/projects/api/environments/staging/env");
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
fn cli_backup_create_posts_backup_request() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"backup_id":"backup-1","project_id":"api","environment":"production","created_at_unix":10,"source_generation":3,"source_deployment_id":"dep-3","services":["api"],"volumes":[{"volume_id":"redis","docker_volume_name":"forge-api-production-vol-redis","service_id":"api","mount_path":"/data","archive_file":"api-redis.tar.gz","archive_size_bytes":12,"archive_sha256":"abc"}],"restores":[]}}"#,
    );

    let output = run_cli(&url, &["backup", "create", "api", "production"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Backup: backup-1"));
    assert!(body.contains("api:redis -> /data"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "POST");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/production/backups"
    );
}

#[test]
fn cli_backup_list_reads_backup_inventory() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"production","backups":[{"backup_id":"backup-1","project_id":"api","environment":"production","created_at_unix":10,"source_generation":3,"services":["api"],"volumes":[{"volume_id":"redis","docker_volume_name":"forge-api-production-vol-redis","service_id":"api","mount_path":"/data","archive_file":"api-redis.tar.gz","archive_size_bytes":12,"archive_sha256":"abc"}],"restores":[]}]}}"#,
    );

    let output = run_cli(&url, &["backup", "list", "api", "production"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Project: api"));
    assert!(body.contains("backup-1 gen-3 volumes=1 restores=0"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/production/backups"
    );
}

#[test]
fn backup_list_json_flag_after_args() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"production","backups":[]}}"#,
    );

    let output = run_cli(&url, &["backup", "list", "api", "production", "--json"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"backups\": []"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(
        request.path,
        "/api/projects/api/environments/production/backups"
    );
}

#[test]
fn cli_backup_inspect_reads_backup_manifest() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"backup_id":"backup-1","project_id":"api","environment":"production","created_at_unix":10,"source_generation":3,"services":["api"],"volumes":[{"volume_id":"redis","docker_volume_name":"forge-api-production-vol-redis","service_id":"api","mount_path":"/data","archive_file":"api-redis.tar.gz","archive_size_bytes":12,"archive_sha256":"abc","archive_files":[{"path":"dump.rdb","size_bytes":4,"sha256":"def"}]}],"hooks":[{"service_id":"api","volume_id":"redis","container_name":"prod-api-gen-3","pre_backup_command":"redis-cli SAVE","started_at_unix":11,"completed_at_unix":12,"timeout_seconds":30,"stdout":"OK","stderr":"","exit_code":0}],"restores":[{"restored_generation":4,"restored_deployment_id":"restore-backup-1-gen-4","restored_at_unix":20,"status":"completed"}]}}"#,
    );

    let output = run_cli(&url, &["backup", "inspect", "backup-1"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Restores:"));
    assert!(body.contains("gen-4 restore-backup-1-gen-4"));
    assert!(body.contains("Hooks:"));
    assert!(body.contains("redis-cli SAVE"));
    assert!(body.contains("archive file: dump.rdb"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/backups/backup-1");
}

#[test]
fn backup_inspect_cli_parses_backup_id() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"backup_id":"backup-1","project_id":"api","environment":"production","created_at_unix":10,"source_generation":3,"services":["api"],"volumes":[],"restores":[]}}"#,
    );

    let output = run_cli(&url, &["backup", "inspect", "backup-1"]);
    assert!(output.status.success());

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.path, "/api/backups/backup-1");
}

#[test]
fn backup_inspect_json_flag_after_backup_id() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"backup_id":"backup-1","project_id":"api","environment":"production","created_at_unix":10,"source_generation":3,"services":["api"],"volumes":[],"restores":[]}}"#,
    );

    let output = run_cli(&url, &["backup", "inspect", "backup-1", "--json"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"backup_id\": \"backup-1\""));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.path, "/api/backups/backup-1");
}

#[test]
fn cli_backup_restore_posts_restore_request() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"backup_id":"backup-1","restored_generation":4,"restored_deployment_id":"restore-backup-1-gen-4","restored_at_unix":20}}"#,
    );

    let output = run_cli(&url, &["backup", "restore", "--json", "backup-1"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"restored_generation\": 4"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/api/backups/backup-1/restore");
}

#[test]
fn diagnose_reports_restore_lineage() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"production","status":"healthy","active_generation":4,"container":{"running":true},"route":{"route_required":false,"route_active":false,"matches_expected":true},"retained_generations":[],"recent_gc_actions":[],"missing_required_secrets":[],"recent_secret_mutations":[],"startup_order":[],"services":[],"recent_failures":[],"active_restore":{"backup_id":"backup-1","restored_generation":4,"source_generation":3,"source_deployment_id":"dep-3","restored_at_unix":20,"hook_succeeded":true,"restored_volumes":[{"volume_id":"redis","docker_volume_name":"forge-api-production-vol-redis","service_id":"api","mount_path":"/data","archive_file":"api-redis.tar.gz","archive_size_bytes":12,"archive_sha256":"abc","archive_files":[{"path":"dump.rdb","size_bytes":4,"sha256":"def"}],"restored_docker_volume_name":"forge-api-production-restore-gen-4-vol-redis"}]},"backup_restore_events":["restored backup backup-1 into gen-4"]}}"#,
    );

    let output = run_cli(&url, &["diagnose", "api", "production"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(
        body.contains("Active Restore: backup=backup-1 restored_generation=4 source_generation=3")
    );
    assert!(body.contains("hook_succeeded=true"));
    assert!(body.contains("restored_volume=forge-api-production-restore-gen-4-vol-redis"));
    assert!(body.contains("Backup Restore Events:"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/production/diagnostics"
    );
}

#[test]
fn diagnose_reports_partial_restore_lineage() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"production","status":"healthy","active_generation":9,"last_deployment_id":"restore-backup-1779481391-gen-9","container":{"running":true},"route":{"route_required":false,"route_active":false,"matches_expected":true},"retained_generations":[],"recent_gc_actions":[],"missing_required_secrets":[],"recent_secret_mutations":[],"startup_order":[],"services":[],"recent_failures":[],"active_restore":{"backup_id":"backup-1779481391","restored_generation":9},"backup_restore_events":[]}}"#,
    );

    let output = run_cli(&url, &["diagnose", "api", "production"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(
        body.contains(
            "Active Restore: backup=backup-1779481391 restored_generation=9 source=unknown"
        )
    );
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
fn cli_secrets_list_reads_redacted_secret_inventory() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"production","secrets":[{"key":"DATABASE_URL","value":"<secret>","created_at_unix":1,"updated_at_unix":2,"referenced_by_generations":[1]}]}}"#,
    );

    let output = run_cli(&url, &["secrets", "list", "api", "production"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("DATABASE_URL=<secret>"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/production/secrets"
    );
}

#[test]
fn cli_secrets_unset_deletes_secret() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"secret_id":"api:production:DATABASE_URL","removed":true}}"#,
    );

    let output = run_cli(
        &url,
        &["secrets", "unset", "api", "production", "DATABASE_URL"],
    );
    assert!(output.status.success());

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "DELETE");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/production/secrets/DATABASE_URL"
    );
}

#[test]
fn cli_env_diff_reads_generation_diff() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"project_id":"api","environment":"production","from_generation":28,"to_generation":29,"added":[{"key":"FEATURE_FLAG","value":"true"}],"removed":[{"key":"OLD_API_URL","value":"https://old.example.com"}],"changed_values":[{"key":"DATABASE_URL","before":"<secret changed>","after":"<secret changed>"}],"changed_secret_references":[]}}"#,
    );

    let output = run_cli(
        &url,
        &[
            "env",
            "diff",
            "api",
            "production",
            "--generation",
            "28",
            "--generation",
            "29",
        ],
    );
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("+ FEATURE_FLAG=true"));
    assert!(body.contains("~ DATABASE_URL=<secret changed>"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/api/projects/api/environments/production/env/diff?generation=28&generation=29"
    );
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
fn token_create_shows_token_once() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"data":{"token":"forge_cli.test-token","metadata":{"token_id":"tok-1","name":"laptop","created_at":1,"github_login":"octocat","source":"token_create"}}}"#,
    );

    let output = run_cli(&url, &["token", "create", "--name", "laptop"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert_eq!(body.matches("forge_cli.test-token").count(), 1);

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/api/tokens");
    assert_eq!(request.authorization, "Bearer test-token");
    let json: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(json["name"], "laptop");
}

#[test]
fn version_outputs_build_metadata() {
    let output = run_cli_in_dir(Path::new("."), &["version"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"version\":"));
    assert!(body.contains(env!("CARGO_PKG_VERSION")));
    assert!(body.contains("\"git_commit\":"));
    assert!(body.contains("\"git_dirty\":"));
    assert!(body.contains("\"build_timestamp\":"));
    assert!(body.contains("\"target_triple\":"));
}

#[test]
fn version_outputs_schema_versions() {
    let output = run_cli_in_dir(Path::new("."), &["version"]);
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("\"schema_versions\""));
    assert!(body.contains("\"manifest_schema\""));
    assert!(body.contains("\"snapshot_schema\""));
    assert!(body.contains("\"checkpoint_schema\""));
    assert!(body.contains("\"reconciliation_log_schema\""));
    assert!(body.contains("\"storage_compatibility\""));
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

#[test]
fn bench_readyz_decodes_raw_ready_response() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests.clone(),
        vec![
            r#"{"status":"ready"}"#,
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":0,"convergence_failures_total":0,"docker_probe_latency_ms":0,"caddy_probe_latency_ms":0,"docker":{"probe_latency_ms":0,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":0,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let output = run_cli(&url, &["bench", "readyz", "--samples", "1"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("status: ready"));
    let captured = requests.lock().unwrap();
    assert_eq!(captured[0].path, "/readyz");
    assert_eq!(captured[1].path, "/metrics");
}

#[test]
fn bench_readyz_decodes_raw_degraded_response() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests,
        vec![
            r#"{"status":"degraded","reasons":[{"project_id":"api","environment":"production","generation":9,"active":true,"unresolved":true,"source":"runtime_state_cache","marker":"route_activation_verification_failed","message":"route target mismatch: current=172.29.0.99:3000 expected=172.29.0.2:3000","last_checked_unix":1779320528,"cache_age_ms":21}]}"#,
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":1,"convergence_failures_total":1,"docker_probe_latency_ms":0,"caddy_probe_latency_ms":0,"docker":{"probe_latency_ms":0,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":0,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let output = run_cli(&url, &["bench", "readyz", "--samples", "1"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("status: degraded"));
}

#[test]
fn bench_convergence_decodes_raw_metrics_response() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests.clone(),
        vec![
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":0,"convergence_failures_total":0,"docker_probe_latency_ms":8,"caddy_probe_latency_ms":6,"docker":{"probe_latency_ms":8,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":6,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let output = run_cli(&url, &["bench", "convergence", "--samples", "1"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("convergence_duration_ms: 51"));
    assert_eq!(requests.lock().unwrap()[0].path, "/metrics");
}

#[test]
fn bench_does_not_expect_api_envelope_for_control_plane_endpoints() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests,
        vec![
            r#"{"status":"ready"}"#,
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":0,"convergence_failures_total":0,"docker_probe_latency_ms":0,"caddy_probe_latency_ms":0,"docker":{"probe_latency_ms":0,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":0,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let output = run_cli(&url, &["bench", "readyz", "--samples", "1"]);
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("missing field data"));
}

#[test]
fn bench_snapshots_completes_under_timeout() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests,
        vec![
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":0,"convergence_failures_total":0,"docker_probe_latency_ms":8,"caddy_probe_latency_ms":6,"docker":{"probe_latency_ms":8,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":6,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let started = std::time::Instant::now();
    let output = run_cli(&url, &["bench", "snapshots", "--samples", "1"]);
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn bench_diagnostics_completes_under_timeout() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests,
        vec![
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":0,"convergence_failures_total":0,"docker_probe_latency_ms":8,"caddy_probe_latency_ms":6,"convergence_domains":[{"domain":"metrics_refresh","status":"healthy","duration_ms":0}],"docker":{"probe_latency_ms":8,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":6,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let started = std::time::Instant::now();
    let output = run_cli(&url, &["bench", "diagnostics", "--samples", "1"]);
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn control_plane_leader_uses_remote_api_when_logged_in() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests.clone(),
        vec![
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":0,"convergence_failures_total":0,"docker_probe_latency_ms":8,"caddy_probe_latency_ms":6,"leader":true,"lease_epoch":1,"lease_age_ms":12,"lease_expiry_ms":3456,"convergence_owner":"node-a","reconciliation_enabled":true,"follower_mode":false,"node":{"node_id":"node-a","booted_at_unix":1779320500,"hostname":"forge-a","capabilities":["control_plane"]},"docker":{"probe_latency_ms":8,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":6,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let output = run_cli(&url, &["control-plane", "leader"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("leader: true"));
    assert!(body.contains("local_node_id: node-a"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/metrics");
}

#[test]
fn control_plane_lease_uses_remote_api_when_logged_in() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server_sequence(
        requests.clone(),
        vec![
            r#"{"queue_depth":0,"convergence_loop_duration_ms":51,"readiness_cache_age_ms":52,"readyz_requests_total":0,"readyz_latency_ms":0,"readyz_degraded_total":0,"convergence_failures_total":0,"docker_probe_latency_ms":8,"caddy_probe_latency_ms":6,"leader":true,"lease_epoch":7,"lease_age_ms":44,"lease_expiry_ms":9000,"convergence_owner":"node-b","reconciliation_enabled":true,"follower_mode":false,"docker":{"probe_latency_ms":8,"breaker":{"state":"closed","failure_count":0}},"caddy":{"probe_latency_ms":6,"breaker":{"state":"closed","failure_count":0}}}"#,
        ],
    );

    let output = run_cli(&url, &["control-plane", "lease"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("lease_epoch: 7"));
    assert!(body.contains("leader_node_id: node-b"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/metrics");
}

#[test]
fn readiness_explain_uses_remote_api_when_logged_in() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"taxonomy":"ready_no_active_failure","readiness_status":"ready","startup_phase":"leader_active","active_failure":false,"failure_scope":"historical","historical_failures":true,"convergence_blocked":false,"replay_running":false,"leader":true,"follower_mode":false,"node_role":"leader","leadership_healthy":true,"leadership_status":"active_leader","last_successful_convergence_unix":1779320528,"last_historical_failure_unix":1779320400,"operator_interpretation":"Control-plane readiness is healthy. Historical failures exist, but there is no active blocker.","safe_next_action":"no action required"}"#,
    );

    let output = run_cli(&url, &["readiness", "explain"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("Readiness: ready"));
    assert!(body.contains("Historical failures: yes"));
    assert!(body.contains("Operator action: no action required"));

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/readiness/explain");
}

#[test]
fn readiness_explain_json_is_machine_readable() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests.clone(),
        r#"{"taxonomy":"degraded_active_convergence_failure","readiness_status":"degraded","startup_phase":"leader_active","active_failure":true,"active_failure_reason":"route_activation_verification_failed","failure_scope":"active","historical_failures":false,"convergence_blocked":true,"replay_running":false,"leader":true,"follower_mode":false,"node_role":"leader","leadership_healthy":true,"leadership_status":"active_leader","last_successful_convergence_unix":1779320528,"operator_interpretation":"Control-plane readiness is degraded by an active convergence blocker: route_activation_verification_failed.","safe_next_action":"inspect route diagnostics and Caddy admin health"}"#,
    );

    let output = run_cli(&url, &["readiness", "explain", "--json"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["taxonomy"], "degraded_active_convergence_failure");
    assert_eq!(json["active_failure"], true);
    assert_eq!(
        json["active_failure_reason"],
        "route_activation_verification_failed"
    );
    assert_eq!(
        json["safe_next_action"],
        "inspect route diagnostics and Caddy admin health"
    );

    let request = requests.lock().unwrap().remove(0);
    assert_eq!(request.path, "/readiness/explain");
}

#[test]
fn readiness_explain_output_does_not_expose_cli_token() {
    let requests = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let (url, _server) = spawn_server(
        requests,
        r#"{"taxonomy":"ready_no_active_failure","readiness_status":"ready","startup_phase":"leader_active","active_failure":false,"failure_scope":"none","historical_failures":false,"convergence_blocked":false,"replay_running":false,"leader":true,"follower_mode":false,"node_role":"leader","leadership_healthy":true,"leadership_status":"active_leader","operator_interpretation":"Control-plane readiness is healthy and convergence is operating normally.","safe_next_action":"no action required"}"#,
    );

    let output = run_cli(&url, &["readiness", "explain"]);
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(!body.contains("test-token"));
    assert!(!body.contains("Bearer"));
}

#[test]
fn control_plane_local_missing_config_returns_helpful_error() {
    let root = test_root("control-plane-local-missing-config-returns-helpful-error");
    let output = run_cli_in_dir(&root, &["control-plane", "leader"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("missing config path; use --config /etc/forge/forge.conf"));
    assert!(!stderr.contains("os error 2"));
}

#[test]
fn control_plane_leader_renders_metrics_leader_state() {
    let root = test_root("control-plane-leader-renders-metrics-leader-state");
    let storage_root = root.join("storage");
    std::fs::create_dir_all(&storage_root).unwrap();
    let node = NodeMetadataStore::new(&storage_root)
        .load_or_create()
        .unwrap();
    let lease = match LeaderLeaseStore::new(&storage_root)
        .try_acquire_or_renew(&node.node_id, 100, 60)
        .unwrap()
    {
        forge_core::storage::LeaseAcquireOutcome::Leader(lease) => lease,
        forge_core::storage::LeaseAcquireOutcome::Follower(_) => {
            panic!("expected local node to hold the lease")
        }
    };
    let config_path = root.join("forge.conf");
    std::fs::write(
        &config_path,
        format!(
            "storage_root={}\napi_bind=127.0.0.1:8080\nbearer_token=test-token\n",
            storage_root.display()
        ),
    )
    .unwrap();

    let output = run_cli_in_dir(
        &root,
        &[
            "--config",
            config_path.to_str().unwrap(),
            "control-plane",
            "leader",
        ],
    );
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("leader: true"));
    assert!(body.contains(&format!("leader_node_id: {}", lease.leader_node_id)));
}

#[test]
fn control_plane_lease_renders_lease_epoch_and_owner() {
    let root = test_root("control-plane-lease-renders-lease-epoch-and-owner");
    let storage_root = root.join("storage");
    std::fs::create_dir_all(&storage_root).unwrap();
    let node = NodeMetadataStore::new(&storage_root)
        .load_or_create()
        .unwrap();
    let lease = match LeaderLeaseStore::new(&storage_root)
        .try_acquire_or_renew(&node.node_id, 100, 60)
        .unwrap()
    {
        forge_core::storage::LeaseAcquireOutcome::Leader(lease) => lease,
        forge_core::storage::LeaseAcquireOutcome::Follower(_) => {
            panic!("expected local node to hold the lease")
        }
    };
    let config_path = root.join("forge.conf");
    std::fs::write(
        &config_path,
        format!(
            "storage_root={}\napi_bind=127.0.0.1:8080\nbearer_token=test-token\n",
            storage_root.display()
        ),
    )
    .unwrap();

    let output = run_cli_in_dir(
        &root,
        &[
            "--config",
            config_path.to_str().unwrap(),
            "control-plane",
            "lease",
        ],
    );
    assert!(output.status.success(), "{output:?}");
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains(&format!("lease_epoch: {}", lease.lease_epoch)));
    assert!(body.contains(&format!("leader_node_id: {}", lease.leader_node_id)));
}

#[test]
fn cli_read_only_control_plane_commands_do_not_take_exclusive_lease_lock() {
    let root = test_root("cli-read-only-control-plane-commands-do-not-take-exclusive-lease-lock");
    let storage_root = root.join("storage");
    std::fs::create_dir_all(&storage_root).unwrap();
    let node = NodeMetadataStore::new(&storage_root)
        .load_or_create()
        .unwrap();
    let lease = match LeaderLeaseStore::new(&storage_root)
        .try_acquire_or_renew(&node.node_id, 100, 60)
        .unwrap()
    {
        forge_core::storage::LeaseAcquireOutcome::Leader(lease) => lease,
        forge_core::storage::LeaseAcquireOutcome::Follower(_) => {
            panic!("expected local node to hold the lease")
        }
    };
    let lock_path = storage_root.join("control_plane/leader_lease.lock");
    std::fs::create_dir_all(&lock_path).unwrap();
    let config_path = root.join("forge.conf");
    std::fs::write(
        &config_path,
        format!(
            "storage_root={}\napi_bind=127.0.0.1:8080\nbearer_token=test-token\n",
            storage_root.display()
        ),
    )
    .unwrap();

    let leader = run_cli_in_dir(
        &root,
        &[
            "--config",
            config_path.to_str().unwrap(),
            "control-plane",
            "leader",
        ],
    );
    let lease_output = run_cli_in_dir(
        &root,
        &[
            "--config",
            config_path.to_str().unwrap(),
            "control-plane",
            "lease",
        ],
    );

    assert!(leader.status.success(), "{leader:?}");
    assert!(lease_output.status.success(), "{lease_output:?}");
    assert!(String::from_utf8_lossy(&leader.stdout).contains(&lease.leader_node_id));
    assert!(String::from_utf8_lossy(&lease_output.stdout).contains("lease_epoch"));
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
        .env("HOME", workdir)
        .env("XDG_CONFIG_HOME", workdir.join(".config"))
        .env_remove("FORGE_URL")
        .env_remove("FORGE_TOKEN")
        .env_remove("FORGE_CONFIG")
        .output()
        .unwrap()
}

fn spawn_server(
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    response_body: &'static str,
) -> (String, thread::JoinHandle<()>) {
    spawn_server_sequence(requests, vec![response_body])
}

fn spawn_server_sequence(
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    response_bodies: Vec<&'static str>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        for response_body in response_bodies {
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
        }
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
