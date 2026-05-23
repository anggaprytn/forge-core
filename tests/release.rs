use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::thread;

use forge_core::upgrade::{UpgradeOptions, apply, plan, read_recent_events, rollback};

fn test_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "forge-core-release-tests-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    root
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn make_fake_binary(path: &Path, version: &str) {
    make_fake_binary_with_schema(path, version, 1, 1, 1, 1, 1);
}

fn make_fake_binary_with_schema(
    path: &Path,
    version: &str,
    manifest_schema: u64,
    snapshot_schema: u64,
    checkpoint_schema: u64,
    reconciliation_log_schema: u64,
    storage_compatibility: u64,
) {
    write_executable(
        path,
        &format!(
            "#!/usr/bin/env bash\nif [ \"$1\" = \"version\" ]; then\n  printf '%s\\n' '{{\"version\":\"{version}\",\"schema_versions\":{{\"manifest_schema\":{manifest_schema},\"snapshot_schema\":{snapshot_schema},\"checkpoint_schema\":{checkpoint_schema},\"reconciliation_log_schema\":{reconciliation_log_schema},\"storage_compatibility\":{storage_compatibility}}}}}'\nelse\n  exit 0\nfi\n"
        ),
    );
}

fn make_artifact(root: &Path, version: &str, mode: u32) -> PathBuf {
    let stage = root.join("stage");
    fs::create_dir_all(&stage).unwrap();
    make_fake_binary(&stage.join("forge"), version);
    fs::write(
        stage.join("forge.conf.example"),
        "storage_root=/tmp/forge\napi_bind=127.0.0.1:18080\nbearer_token=test-token\n",
    )
    .unwrap();
    fs::write(
        stage.join("forge.env.example"),
        "FORGE_MASTER_KEY=replace-with-64-hex-characters\n",
    )
    .unwrap();
    fs::write(stage.join("README.md"), "release").unwrap();
    fs::write(stage.join("install.sh"), "#!/usr/bin/env bash\n").unwrap();
    let mut permissions = fs::metadata(stage.join("install.sh"))
        .unwrap()
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(stage.join("install.sh"), permissions).unwrap();

    let artifact = root.join(format!("forge-{version}-linux-amd64.tar.gz"));
    let status = Command::new("tar")
        .current_dir(&stage)
        .args(["-czf"])
        .arg(&artifact)
        .args([
            "forge",
            "install.sh",
            "README.md",
            "forge.conf.example",
            "forge.env.example",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    let mut artifact_permissions = fs::metadata(&artifact).unwrap().permissions();
    artifact_permissions.set_mode(mode);
    fs::set_permissions(&artifact, artifact_permissions).unwrap();
    artifact
}

fn sha256(path: &Path) -> String {
    let output = if Command::new("sha256sum").arg(path).output().is_ok() {
        Command::new("sha256sum").arg(path).output().unwrap()
    } else {
        Command::new("shasum")
            .args(["-a", "256"])
            .arg(path)
            .output()
            .unwrap()
    };
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap()
        .to_string()
}

fn spawn_ok_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        for _ in 0..32 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buffer = [0_u8; 1024];
            let _ = stream.read(&mut buffer);
            let response = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
            let _ = stream.write_all(response);
        }
    });
    (url, handle)
}

fn spawn_readyz_sequence_server(statuses: Vec<u16>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        for status in statuses {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buffer = [0_u8; 1024];
            let _ = stream.read(&mut buffer);
            let response = match status {
                200 => {
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok"
                        .as_slice()
                }
                _ => b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 4\r\nconnection: close\r\n\r\nfail"
                    .as_slice(),
            };
            let _ = stream.write_all(response);
        }
    });
    (url, handle)
}

fn with_env<R>(vars: &[(&str, String)], f: impl FnOnce() -> R) -> R {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    let saved = vars
        .iter()
        .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
        .collect::<Vec<_>>();
    for (key, value) in vars {
        unsafe { std::env::set_var(key, value) };
    }
    let result = f();
    for (key, previous) in saved {
        match previous {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }
    result
}

fn write_upgrade_config(root: &Path, bearer_token: &str) {
    fs::write(
        root.join("forge.conf"),
        format!(
            "storage_root={}\napi_bind=127.0.0.1:18080\nbearer_token={bearer_token}\n",
            root.join("storage").display()
        ),
    )
    .unwrap();
}

fn write_upgrade_env(root: &Path, master_key: &str) {
    fs::write(
        root.join("forge.env"),
        format!("FORGE_MASTER_KEY={master_key}\n"),
    )
    .unwrap();
}

fn prepare_upgrade_root(
    root: &Path,
    current_version: &str,
    target_version: &str,
) -> (PathBuf, PathBuf) {
    let current = root.join("bin/forge");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, current_version);
    let artifact = make_artifact(root, target_version, 0o644);
    write_upgrade_config(root, "test-token");
    write_upgrade_env(root, "abc");
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    (current, artifact)
}

#[test]
fn package_release_creates_tarball() {
    let root = test_root("package-tarball");
    let bin_dir = root.join("bin/linux-amd64");
    fs::create_dir_all(&bin_dir).unwrap();
    make_fake_binary(&bin_dir.join("forge"), "9.9.9");

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_PACKAGE_OUTPUT_DIR", root.join("dist"))
        .env("FORGE_PACKAGE_VERSION", "9.9.9")
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .env("FORGE_PACKAGE_BIN_DIR", root.join("bin"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(root.join("dist/forge-9.9.9-linux-amd64.tar.gz").exists());
}

#[test]
fn package_release_creates_checksums() {
    let root = test_root("package-checksums");
    let bin_dir = root.join("bin/linux-amd64");
    fs::create_dir_all(&bin_dir).unwrap();
    make_fake_binary(&bin_dir.join("forge"), "9.9.9");

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_PACKAGE_OUTPUT_DIR", root.join("dist"))
        .env("FORGE_PACKAGE_VERSION", "9.9.9")
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .env("FORGE_PACKAGE_BIN_DIR", root.join("bin"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let checksums = fs::read_to_string(root.join("dist/checksums.txt")).unwrap();
    assert!(checksums.contains("forge-9.9.9-linux-amd64.tar.gz"));
}

#[test]
fn package_release_includes_required_files() {
    let root = test_root("package-contents");
    let bin_dir = root.join("bin/linux-amd64");
    fs::create_dir_all(&bin_dir).unwrap();
    make_fake_binary(&bin_dir.join("forge"), "9.9.9");

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_PACKAGE_OUTPUT_DIR", root.join("dist"))
        .env("FORGE_PACKAGE_VERSION", "9.9.9")
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .env("FORGE_PACKAGE_BIN_DIR", root.join("bin"))
        .output()
        .unwrap();
    assert!(output.status.success());

    let extract = root.join("extract");
    fs::create_dir_all(&extract).unwrap();
    let status = Command::new("tar")
        .args(["-xzf"])
        .arg(root.join("dist/forge-9.9.9-linux-amd64.tar.gz"))
        .args(["-C"])
        .arg(&extract)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(extract.join("forge").exists());
    assert!(extract.join("install.sh").exists());
    assert!(extract.join("README.md").exists());
    assert!(extract.join("forge.conf.example").exists());
    assert!(extract.join("forge.env.example").exists());
}

#[test]
fn install_preserves_existing_config() {
    let root = test_root("install-config");
    let artifact = make_artifact(&root, "1.2.3", 0o644);
    let config_dir = root.join("etc/forge");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(config_dir.join("forge.conf"), "sentinel-config\n").unwrap();
    fs::write(config_dir.join("forge.env"), "sentinel-env\n").unwrap();

    let output = Command::new("bash")
        .arg("install.sh")
        .arg("--artifact")
        .arg(&artifact)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_ALLOW_UNPRIVILEGED_INSTALL", "1")
        .env("FORGE_BIN_DEST", root.join("bin/forge"))
        .env("FORGE_PREVIOUS_BIN_DEST", root.join("bin/forge.previous"))
        .env("FORGE_CONFIG_DIR", &config_dir)
        .env("FORGE_STORAGE_ROOT", root.join("var/lib/forge"))
        .env("FORGE_SRV_ROOT", root.join("srv/forge"))
        .env("FORGE_SAMPLE_ROOT", root.join("srv/forge/sample-http-app"))
        .env("FORGE_UNIT_PATH", root.join("forge.service"))
        .env("FORGE_SERVICE_SRC", root.join("missing.service"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        fs::read_to_string(config_dir.join("forge.conf")).unwrap(),
        "sentinel-config\n"
    );
}

#[test]
fn install_preserves_existing_env() {
    let root = test_root("install-env");
    let artifact = make_artifact(&root, "1.2.3", 0o644);
    let config_dir = root.join("etc/forge");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(config_dir.join("forge.env"), "sentinel-env\n").unwrap();

    let output = Command::new("bash")
        .arg("install.sh")
        .arg("--artifact")
        .arg(&artifact)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_ALLOW_UNPRIVILEGED_INSTALL", "1")
        .env("FORGE_BIN_DEST", root.join("bin/forge"))
        .env("FORGE_PREVIOUS_BIN_DEST", root.join("bin/forge.previous"))
        .env("FORGE_CONFIG_DIR", &config_dir)
        .env("FORGE_STORAGE_ROOT", root.join("var/lib/forge"))
        .env("FORGE_SRV_ROOT", root.join("srv/forge"))
        .env("FORGE_SAMPLE_ROOT", root.join("srv/forge/sample-http-app"))
        .env("FORGE_UNIT_PATH", root.join("forge.service"))
        .env("FORGE_SERVICE_SRC", root.join("missing.service"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        fs::read_to_string(config_dir.join("forge.env")).unwrap(),
        "sentinel-env\n"
    );
}

#[test]
fn install_writes_previous_binary() {
    let root = test_root("install-previous");
    let artifact = make_artifact(&root, "1.2.3", 0o644);
    fs::create_dir_all(root.join("bin")).unwrap();
    fs::write(root.join("bin/forge"), b"old").unwrap();

    let output = Command::new("bash")
        .arg("install.sh")
        .arg("--artifact")
        .arg(&artifact)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_ALLOW_UNPRIVILEGED_INSTALL", "1")
        .env("FORGE_BIN_DEST", root.join("bin/forge"))
        .env("FORGE_PREVIOUS_BIN_DEST", root.join("bin/forge.previous"))
        .env("FORGE_CONFIG_DIR", root.join("etc/forge"))
        .env("FORGE_STORAGE_ROOT", root.join("var/lib/forge"))
        .env("FORGE_SRV_ROOT", root.join("srv/forge"))
        .env("FORGE_SAMPLE_ROOT", root.join("srv/forge/sample-http-app"))
        .env("FORGE_UNIT_PATH", root.join("forge.service"))
        .env("FORGE_SERVICE_SRC", root.join("missing.service"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(fs::read(root.join("bin/forge.previous")).unwrap(), b"old");
}

#[test]
fn install_artifact_install_is_atomic() {
    let root = test_root("install-atomic");
    let artifact = make_artifact(&root, "1.2.3", 0o644);

    let output = Command::new("bash")
        .arg("install.sh")
        .arg("--artifact")
        .arg(&artifact)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_ALLOW_UNPRIVILEGED_INSTALL", "1")
        .env("FORGE_BIN_DEST", root.join("bin/forge"))
        .env("FORGE_PREVIOUS_BIN_DEST", root.join("bin/forge.previous"))
        .env("FORGE_CONFIG_DIR", root.join("etc/forge"))
        .env("FORGE_STORAGE_ROOT", root.join("var/lib/forge"))
        .env("FORGE_SRV_ROOT", root.join("srv/forge"))
        .env("FORGE_SAMPLE_ROOT", root.join("srv/forge/sample-http-app"))
        .env("FORGE_UNIT_PATH", root.join("forge.service"))
        .env("FORGE_SERVICE_SRC", root.join("missing.service"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(root.join("bin/forge").exists());
    assert!(fs::read_dir(root.join("bin")).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp.")
    }));
}

#[test]
fn upgrade_plan_reports_current_and_target_version() {
    let root = test_root("upgrade-plan");
    let current = root.join("bin/forge");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "1.0.0");
    let artifact = make_artifact(&root, "2.0.0", 0o644);
    fs::write(
        root.join("forge.conf"),
        format!(
            "storage_root={}\napi_bind=127.0.0.1:18080\nbearer_token=test-token\n",
            root.join("storage").display()
        ),
    )
    .unwrap();
    fs::write(root.join("forge.env"), "FORGE_MASTER_KEY=abc\n").unwrap();
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    let plan_output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            plan(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap()
        },
    );
    assert_eq!(plan_output.current_version, "1.0.0");
    assert_eq!(plan_output.target_version, "2.0.0");
}

#[test]
fn upgrade_plan_does_not_mutate_state() {
    let root = test_root("upgrade-plan-non-mutating");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    let plan_output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            plan(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap()
        },
    );

    assert_eq!(plan_output.current_version, "1.0.0");
    assert_eq!(
        fs::read_to_string(&current)
            .unwrap()
            .matches("1.0.0")
            .count(),
        1
    );
    assert!(!root.join("storage/control_plane/upgrades.jsonl").exists());
}

#[test]
fn upgrade_plan_rejects_storage_compatibility_mismatch() {
    let root = test_root("upgrade-plan-storage-compat");
    let current = root.join("bin/forge");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary_with_schema(&current, "1.0.0", 1, 1, 1, 1, 1);
    let stage = root.join("stage");
    fs::create_dir_all(&stage).unwrap();
    make_fake_binary_with_schema(&stage.join("forge"), "2.0.0", 1, 1, 1, 1, 2);
    fs::write(
        stage.join("forge.conf.example"),
        "storage_root=/tmp/forge\n",
    )
    .unwrap();
    fs::write(
        stage.join("forge.env.example"),
        "FORGE_MASTER_KEY=replace\n",
    )
    .unwrap();
    fs::write(stage.join("README.md"), "release").unwrap();
    fs::write(stage.join("install.sh"), "#!/usr/bin/env bash\n").unwrap();
    let mut permissions = fs::metadata(stage.join("install.sh"))
        .unwrap()
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(stage.join("install.sh"), permissions).unwrap();
    let artifact = root.join("forge-2.0.0-linux-amd64.tar.gz");
    assert!(
        Command::new("tar")
            .current_dir(&stage)
            .args(["-czf"])
            .arg(&artifact)
            .args([
                "forge",
                "install.sh",
                "README.md",
                "forge.conf.example",
                "forge.env.example",
            ])
            .status()
            .unwrap()
            .success()
    );
    write_upgrade_config(&root, "test-token");
    write_upgrade_env(&root, "abc");
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    let plan_output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            plan(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap()
        },
    );
    assert!(plan_output.checks.iter().any(|check| {
        check.message.contains("Storage compatibility mismatch") && check.status == "error"
    }));
}

#[test]
fn artifact_checksum_mismatch_blocks_upgrade() {
    let root = test_root("upgrade-checksum");
    let current = root.join("bin/forge");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "1.0.0");
    let artifact = make_artifact(&root, "2.0.0", 0o644);
    fs::write(
        root.join("checksums.txt"),
        "deadbeef  forge-2.0.0-linux-amd64.tar.gz\n",
    )
    .unwrap();
    fs::write(
        root.join("forge.conf"),
        format!(
            "storage_root={}\napi_bind=127.0.0.1:18080\nbearer_token=test-token\n",
            root.join("storage").display()
        ),
    )
    .unwrap();
    fs::write(root.join("forge.env"), "FORGE_MASTER_KEY=abc\n").unwrap();
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap_err();
            assert!(err.to_string().contains("checksum mismatch"));
        },
    );
}

#[test]
fn world_writable_artifact_rejected() {
    let root = test_root("upgrade-world-writable");
    let current = root.join("bin/forge");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "1.0.0");
    let artifact = make_artifact(&root, "2.0.0", 0o666);
    fs::write(
        root.join("forge.conf"),
        format!(
            "storage_root={}\napi_bind=127.0.0.1:18080\nbearer_token=test-token\n",
            root.join("storage").display()
        ),
    )
    .unwrap();
    fs::write(root.join("forge.env"), "FORGE_MASTER_KEY=abc\n").unwrap();
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap_err();
            assert!(err.to_string().contains("world-writable"));
        },
    );
}

#[test]
fn upgrade_apply_runs_plan_first() {
    let root = test_root("upgrade-apply-plan-first");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    fs::write(
        root.join("checksums.txt"),
        "deadbeef  forge-2.0.0-linux-amd64.tar.gz\n",
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let systemctl_log = root.join("systemctl.log");
    write_executable(
        &fake_bin.join("systemctl"),
        &format!(
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
            systemctl_log.display()
        ),
    );
    let (url, _handle) = spawn_ok_server();

    let err = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap_err()
        },
    );

    assert!(err.to_string().contains("checksum mismatch"));
    assert!(!systemctl_log.exists());
}

#[test]
fn upgrade_apply_backs_up_current_binary() {
    let root = test_root("upgrade-apply-backup");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url.clone()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap();
        },
    );

    assert!(fs::read_to_string(previous).unwrap().contains("1.0.0"));
}

#[test]
fn upgrade_apply_uses_sudo_for_system_paths() {
    let root = test_root("upgrade-apply-sudo");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let sudo_log = root.join("sudo.log");
    write_executable(
        &fake_bin.join("sudo"),
        &format!(
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> '{}'\nexec \"$@\"\n",
            sudo_log.display()
        ),
    );
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SUDO_BIN",
                fake_bin.join("sudo").display().to_string(),
            ),
            ("FORGE_UPGRADE_FORCE_SUDO", "1".into()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url.clone()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap();
        },
    );

    let log = fs::read_to_string(sudo_log).unwrap();
    assert!(log.contains("systemctl stop forge.service"));
    assert!(log.contains("cp"));
    assert!(log.contains("install -m 0755"));
    assert!(log.contains("mv"));
}

#[test]
fn upgrade_apply_installs_binary_atomically() {
    let root = test_root("upgrade-apply-atomic");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url.clone()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap();
        },
    );

    assert!(fs::read_dir(root.join("bin")).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp.")
    }));
}

#[test]
fn upgrade_apply_rolls_back_when_readyz_fails() {
    let root = test_root("upgrade-apply-auto-rollback");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();
    let (url, _handle) = spawn_readyz_sequence_server(vec![503, 503, 503, 503, 200, 200, 200]);

    let output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url.clone()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "150".into()),
            ("FORGE_UPGRADE_READYZ_POLL_MS", "50".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: caddy_url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap()
        },
    );

    assert_eq!(output.result, "auto_rolled_back");
    assert!(fs::read_to_string(&current).unwrap().contains("1.0.0"));
}

#[test]
fn upgrade_apply_no_auto_rollback_preserves_failed_binary() {
    let root = test_root("upgrade-apply-no-auto-rollback");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();

    let err = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", "http://127.0.0.1:9".into()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "150".into()),
            ("FORGE_UPGRADE_READYZ_POLL_MS", "50".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: caddy_url,
                artifact_path: artifact,
                auto_rollback: false,
            })
            .unwrap_err()
        },
    );

    assert!(
        err.to_string()
            .contains("readyz failed at http://127.0.0.1:9")
    );
    assert!(fs::read_to_string(current).unwrap().contains("2.0.0"));
}

#[test]
fn upgrade_readyz_wait_times_out_with_context() {
    let root = test_root("upgrade-readyz-timeout");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();

    let err = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", "http://127.0.0.1:9".into()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "150".into()),
            ("FORGE_UPGRADE_READYZ_POLL_MS", "50".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: caddy_url,
                artifact_path: artifact,
                auto_rollback: false,
            })
            .unwrap_err()
        },
    );

    let message = err.to_string();
    assert!(message.contains("readyz failed at http://127.0.0.1:9"));
    assert!(message.contains("attempts"));
    assert!(message.contains("after"));
}

#[test]
fn upgrade_apply_writes_journal() {
    let root = test_root("upgrade-apply-journal");
    let current = root.join("bin/forge");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "1.0.0");
    let previous = root.join("bin/forge.previous");
    let artifact = make_artifact(&root, "2.0.0", 0o644);
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    fs::write(
        root.join("forge.conf"),
        format!(
            "storage_root={}\napi_bind=127.0.0.1:18080\nbearer_token=test-token\n",
            root.join("storage").display()
        ),
    )
    .unwrap();
    fs::write(root.join("forge.env"), "FORGE_MASTER_KEY=abc\n").unwrap();
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url.clone()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || {
            let output = apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap();
            assert_eq!(output.result, "ok");
            let events = read_recent_events(&root.join("storage"), 5);
            assert!(events.iter().any(|event| event.action == "apply"));
        },
    );
}

#[test]
fn upgrade_rollback_restores_previous_binary() {
    let root = test_root("upgrade-rollback");
    let current = root.join("bin/forge");
    let previous = root.join("bin/forge.previous");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "2.0.0");
    make_fake_binary(&previous, "1.0.0");
    fs::write(
        root.join("forge.conf"),
        format!(
            "storage_root={}\napi_bind=127.0.0.1:18080\nbearer_token=test-token\n",
            root.join("storage").display()
        ),
    )
    .unwrap();
    fs::write(root.join("forge.env"), "FORGE_MASTER_KEY=abc\n").unwrap();
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || {
            rollback(&root.join("forge.conf")).unwrap();
            let current_body = fs::read_to_string(&current).unwrap();
            assert!(current_body.contains("1.0.0"));
            let events = read_recent_events(&root.join("storage"), 5);
            assert!(events.iter().any(|event| event.action == "rollback"));
        },
    );
}

#[test]
fn upgrade_rollback_uses_sudo_for_system_paths() {
    let root = test_root("upgrade-rollback-sudo");
    let current = root.join("bin/forge");
    let previous = root.join("bin/forge.previous");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "2.0.0");
    make_fake_binary(&previous, "1.0.0");
    write_upgrade_config(&root, "temp-token");
    write_upgrade_env(&root, "temp-master-key");
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let sudo_log = root.join("sudo.log");
    write_executable(
        &fake_bin.join("sudo"),
        &format!(
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> '{}'\nexec \"$@\"\n",
            sudo_log.display()
        ),
    );
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SUDO_BIN",
                fake_bin.join("sudo").display().to_string(),
            ),
            ("FORGE_UPGRADE_FORCE_SUDO", "1".into()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || rollback(&root.join("forge.conf")).unwrap(),
    );

    let log = fs::read_to_string(sudo_log).unwrap();
    assert!(log.contains("install -m 0755"));
    assert!(log.contains("mv"));
    assert!(log.contains("systemctl restart forge.service"));
}

#[test]
fn upgrade_rollback_test_does_not_touch_real_system_paths() {
    let root = test_root("upgrade-rollback-temp-paths");
    let current = root.join("bin/forge");
    let previous = root.join("bin/forge.previous");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "2.0.0");
    make_fake_binary(&previous, "1.0.0");
    write_upgrade_config(&root, "temp-token");
    write_upgrade_env(&root, "temp-master-key");
    fs::create_dir_all(root.join("storage/projects")).unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    let log_path = root.join("systemctl.log");
    write_executable(
        &fake_bin.join("systemctl"),
        &format!(
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
            log_path.display()
        ),
    );
    let (url, _handle) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            ("FORGE_UPGRADE_READYZ_URL", url),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || rollback(&root.join("forge.conf")).unwrap(),
    );

    assert!(
        fs::read_to_string(log_path)
            .unwrap()
            .contains("restart forge.service")
    );
    assert!(fs::read_to_string(&current).unwrap().contains("1.0.0"));
    assert!(!current.starts_with("/usr/local/bin"));
}

#[test]
fn upgrade_journal_records_apply_and_rollback() {
    let root = test_root("upgrade-apply-rollback-journal");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    write_upgrade_config(&root, "bearer-secret-token");
    write_upgrade_env(&root, "master-secret-value");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (apply_url, _handle1) = spawn_ok_server();
    let (rollback_url, _handle2) = spawn_ok_server();

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", apply_url.clone()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: apply_url,
                artifact_path: artifact,
                auto_rollback: true,
            })
            .unwrap();
        },
    );

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            ("FORGE_UPGRADE_READYZ_URL", rollback_url),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "3000".into()),
        ],
        || rollback(&root.join("forge.conf")).unwrap(),
    );

    let events = read_recent_events(&root.join("storage"), 10);
    assert!(events.iter().any(|event| event.action == "apply"));
    assert!(events.iter().any(|event| event.action == "rollback"));
    let journal = fs::read_to_string(root.join("storage/control_plane/upgrades.jsonl")).unwrap();
    assert!(!journal.contains("master-secret-value"));
    assert!(!journal.contains("bearer-secret-token"));
}

#[test]
fn upgrade_does_not_log_env_secrets() {
    let root = test_root("upgrade-secret-redaction");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    let bearer_token = "bearer-token-should-not-leak";
    let master_key = "master-key-should-not-leak";
    write_upgrade_config(&root, bearer_token);
    write_upgrade_env(&root, master_key);
    fs::write(
        root.join("checksums.txt"),
        format!(
            "{}  {}\n",
            sha256(&artifact),
            artifact.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();

    let err = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "FORGE_UPGRADE_PREVIOUS_BINARY_PATH",
                previous.display().to_string(),
            ),
            (
                "FORGE_SYSTEMCTL_BIN",
                fake_bin.join("systemctl").display().to_string(),
            ),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
            ("FORGE_UPGRADE_READYZ_URL", "http://127.0.0.1:9".into()),
            ("FORGE_UPGRADE_READYZ_TIMEOUT_MS", "150".into()),
            ("FORGE_UPGRADE_READYZ_POLL_MS", "50".into()),
        ],
        || {
            apply(&UpgradeOptions {
                config_path: root.join("forge.conf"),
                caddy_admin_url: caddy_url,
                artifact_path: artifact,
                auto_rollback: false,
            })
            .unwrap_err()
        },
    );

    let message = err.to_string();
    assert!(!message.contains(bearer_token));
    assert!(!message.contains(master_key));
}

#[test]
fn package_release_child_process_timeout() {
    let root = test_root("package-timeout");
    let bin_dir = root.join("bin/linux-amd64");
    fs::create_dir_all(&bin_dir).unwrap();
    make_fake_binary(&bin_dir.join("forge"), "9.9.9");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(
        &fake_bin.join("tar"),
        "#!/usr/bin/env bash\nsleep 2\nexit 0\n",
    );

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env(
            "PATH",
            format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
        )
        .env("FORGE_PACKAGE_OUTPUT_DIR", root.join("dist"))
        .env("FORGE_PACKAGE_VERSION", "9.9.9")
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .env("FORGE_PACKAGE_BIN_DIR", root.join("bin"))
        .env("FORGE_PACKAGE_TIMEOUT_SECS", "1")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("timed out after 1s"));
}

#[test]
fn release_tests_use_temp_install_root() {
    let root = test_root("install-temp-root");
    let artifact = make_artifact(&root, "1.2.3", 0o644);
    let config_dir = root.join("etc/forge");

    let output = Command::new("bash")
        .arg("install.sh")
        .arg("--artifact")
        .arg(&artifact)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_ALLOW_UNPRIVILEGED_INSTALL", "1")
        .env("FORGE_BIN_DEST", root.join("bin/forge"))
        .env("FORGE_PREVIOUS_BIN_DEST", root.join("bin/forge.previous"))
        .env("FORGE_CONFIG_DIR", &config_dir)
        .env("FORGE_STORAGE_ROOT", root.join("var/lib/forge"))
        .env("FORGE_SRV_ROOT", root.join("srv/forge"))
        .env("FORGE_SAMPLE_ROOT", root.join("srv/forge/sample-http-app"))
        .env("FORGE_UNIT_PATH", root.join("forge.service"))
        .env("FORGE_SERVICE_SRC", root.join("missing.service"))
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(root.join("bin/forge").exists());
    assert!(config_dir.join("forge.conf").exists());
    assert!(config_dir.join("forge.env").exists());
}
