use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::thread;

use base64::Engine;
use forge_core::upgrade::{UpgradeOptions, apply, plan, read_recent_events, rollback};
use serde_json::json;

const ARTIFACT_GIT_COMMIT: &str = "artifact-commit";
const ARTIFACT_TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";
const ARTIFACT_BUILD_TIMESTAMP: &str = "1712345678";

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

fn run_git(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap()
}

fn git_ok(root: &Path, args: &[&str]) {
    let output = run_git(root, args);
    assert!(
        output.status.success(),
        "git {:?} failed: stdout={} stderr={}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_stdout(root: &Path, args: &[&str]) -> String {
    let output = run_git(root, args);
    assert!(
        output.status.success(),
        "git {:?} failed: stdout={} stderr={}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn init_package_repo(name: &str) -> PathBuf {
    let root = test_root(name);
    fs::create_dir_all(root.join("scripts")).unwrap();
    fs::create_dir_all(root.join("deploy")).unwrap();
    fs::create_dir_all(root.join("examples")).unwrap();
    fs::create_dir_all(root.join("fake-bin")).unwrap();

    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/package-release.sh"),
        root.join("scripts/package-release.sh"),
    )
    .unwrap();
    write_executable(
        &root.join("install.sh"),
        "#!/usr/bin/env bash\nset -euo pipefail\n",
    );
    fs::write(root.join("README.md"), "release notes\n").unwrap();
    fs::write(
        root.join("deploy/forge.conf.example"),
        "storage_root=/tmp/forge\n",
    )
    .unwrap();
    fs::write(
        root.join("examples/forge.env.example"),
        "FORGE_MASTER_KEY=replace-with-64-hex-characters\n",
    )
    .unwrap();
    fs::write(root.join("LICENSE"), "license\n").unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = \"forge_core\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    fs::write(root.join(".gitignore"), "dist/\ntarget/\ncargo-env.log\n").unwrap();
    write_executable(
        &root.join("fake-bin/cargo"),
        "#!/usr/bin/env bash\nset -euo pipefail\ntriple=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"--target\" ]; then\n    triple=\"$2\"\n    shift 2\n    continue\n  fi\n  shift\ndone\n[ -n \"$triple\" ] || { echo \"missing --target\" >&2; exit 1; }\nprintf '%s|%s|%s|%s\\n' \"${FORGE_GIT_COMMIT:-}\" \"${FORGE_GIT_DIRTY:-}\" \"${FORGE_BUILD_TIMESTAMP:-}\" \"${FORGE_TARGET_TRIPLE:-}\" >> cargo-env.log\nmkdir -p \"target/$triple/release\"\ncat > \"target/$triple/release/forge\" <<EOF\n#!/usr/bin/env bash\nprintf '%s\\n' '{\"version\":\"0.1.0\",\"git_commit\":\"${FORGE_GIT_COMMIT:-unknown}\",\"git_dirty\":\"${FORGE_GIT_DIRTY:-unknown}\",\"build_timestamp\":\"${FORGE_BUILD_TIMESTAMP:-unknown}\",\"target_triple\":\"${FORGE_TARGET_TRIPLE:-unknown}\",\"schema_versions\":{\"manifest_schema\":1,\"snapshot_schema\":1,\"checkpoint_schema\":1,\"reconciliation_log_schema\":1,\"storage_compatibility\":1}}'\nEOF\nchmod +x \"target/$triple/release/forge\"\n",
    );

    git_ok(&root, &["init"]);
    git_ok(&root, &["config", "user.name", "Forge Tests"]);
    git_ok(&root, &["config", "user.email", "forge-tests@example.com"]);
    git_ok(&root, &["add", "."]);
    git_ok(&root, &["commit", "-m", "initial"]);
    root
}

fn make_fake_binary(path: &Path, version: &str) {
    make_fake_binary_with_schema_and_metadata(
        path,
        version,
        ARTIFACT_GIT_COMMIT,
        "false",
        ARTIFACT_BUILD_TIMESTAMP,
        ARTIFACT_TARGET_TRIPLE,
        1,
        1,
        1,
        1,
        1,
    );
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
    make_fake_binary_with_schema_and_metadata(
        path,
        version,
        ARTIFACT_GIT_COMMIT,
        "false",
        ARTIFACT_BUILD_TIMESTAMP,
        ARTIFACT_TARGET_TRIPLE,
        manifest_schema,
        snapshot_schema,
        checkpoint_schema,
        reconciliation_log_schema,
        storage_compatibility,
    );
}

fn make_fake_binary_with_schema_and_metadata(
    path: &Path,
    version: &str,
    git_commit: &str,
    git_dirty: &str,
    build_timestamp: &str,
    target_triple: &str,
    manifest_schema: u64,
    snapshot_schema: u64,
    checkpoint_schema: u64,
    reconciliation_log_schema: u64,
    storage_compatibility: u64,
) {
    write_executable(
        path,
        &format!(
            "#!/usr/bin/env bash\nif [ \"$1\" = \"version\" ]; then\n  printf '%s\\n' '{{\"version\":\"{version}\",\"git_commit\":\"{git_commit}\",\"git_dirty\":\"{git_dirty}\",\"build_timestamp\":\"{build_timestamp}\",\"target_triple\":\"{target_triple}\",\"schema_versions\":{{\"manifest_schema\":{manifest_schema},\"snapshot_schema\":{snapshot_schema},\"checkpoint_schema\":{checkpoint_schema},\"reconciliation_log_schema\":{reconciliation_log_schema},\"storage_compatibility\":{storage_compatibility}}}}}'\nelse\n  exit 0\nfi\n"
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

fn artifact_size(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

fn write_release_manifest(
    manifest_path: &Path,
    artifact: &Path,
    version: &str,
    git_commit: &str,
    git_dirty: bool,
    target_triple: &str,
) {
    let manifest = json!({
        "version": version,
        "git_commit": git_commit,
        "git_dirty": git_dirty,
        "build_timestamp": ARTIFACT_BUILD_TIMESTAMP,
        "artifacts": [
            {
                "name": artifact.file_name().unwrap().to_string_lossy(),
                "target_triple": target_triple,
                "sha256": sha256(artifact),
                "size_bytes": artifact_size(artifact),
                "created_at_unix": 1712345678_u64
            }
        ],
        "schema_versions": {
            "manifest_schema": 1,
            "snapshot_schema": 1,
            "checkpoint_schema": 1,
            "reconciliation_log_schema": 1,
            "storage_compatibility_version": 1
        }
    });
    fs::write(manifest_path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
}

fn write_unsigned_release_manifest(root: &Path, artifact: &Path, version: &str) -> PathBuf {
    let manifest_path = root.join("release-manifest.json");
    write_release_manifest(
        &manifest_path,
        artifact,
        version,
        ARTIFACT_GIT_COMMIT,
        false,
        ARTIFACT_TARGET_TRIPLE,
    );
    manifest_path
}

fn generate_signing_keypair(root: &Path) -> (PathBuf, PathBuf) {
    let private_key = root.join("release-signing-key.pem");
    let public_key = root.join("release-public-key.pem");
    let status = Command::new("openssl")
        .args(["genpkey", "-algorithm", "Ed25519", "-out"])
        .arg(&private_key)
        .status()
        .unwrap();
    assert!(status.success());
    let status = Command::new("openssl")
        .args(["pkey", "-in"])
        .arg(&private_key)
        .args(["-pubout", "-out"])
        .arg(&public_key)
        .status()
        .unwrap();
    assert!(status.success());
    (private_key, public_key)
}

fn sign_manifest(manifest_path: &Path, private_key: &Path, signature_path: &Path) {
    let signature_bin = signature_path.with_extension("sig.bin");
    let status = Command::new("openssl")
        .args(["pkeyutl", "-sign", "-rawin", "-inkey"])
        .arg(private_key)
        .args(["-in"])
        .arg(manifest_path)
        .args(["-out"])
        .arg(&signature_bin)
        .status()
        .unwrap();
    assert!(status.success());
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(fs::read(&signature_bin).unwrap());
    fs::write(signature_path, format!("{encoded}\n")).unwrap();
    fs::remove_file(signature_bin).unwrap();
}

fn default_upgrade_options(
    root: &Path,
    caddy_admin_url: String,
    artifact_path: PathBuf,
) -> UpgradeOptions {
    UpgradeOptions {
        config_path: root.join("forge.conf"),
        caddy_admin_url,
        artifact_path,
        manifest_path: None,
        signature_path: None,
        allow_unsigned: true,
        allow_dirty_artifact: false,
        auto_rollback: true,
    }
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

fn default_signed_upgrade_options(
    root: &Path,
    caddy_admin_url: String,
    artifact_path: PathBuf,
) -> UpgradeOptions {
    let manifest_path = write_unsigned_release_manifest(root, &artifact_path, "2.0.0");
    UpgradeOptions {
        manifest_path: Some(manifest_path),
        allow_unsigned: true,
        ..default_upgrade_options(root, caddy_admin_url, artifact_path)
    }
}

#[test]
fn package_release_emits_manifest() {
    let root = test_root("package-tarball");
    let bin_dir = root.join("bin/linux-amd64");
    fs::create_dir_all(&bin_dir).unwrap();
    make_fake_binary(&bin_dir.join("forge"), "9.9.9");

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--allow-dirty")
        .arg("--unsigned")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_PACKAGE_OUTPUT_DIR", root.join("dist"))
        .env("FORGE_PACKAGE_VERSION", "9.9.9")
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .env("FORGE_PACKAGE_BIN_DIR", root.join("bin"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.join("dist/forge-9.9.9-linux-amd64.tar.gz").exists());
    assert!(root.join("dist/release-manifest.json").exists());
}

#[test]
fn package_release_manifest_contains_artifact_hash() {
    let root = test_root("package-checksums");
    let bin_dir = root.join("bin/linux-amd64");
    fs::create_dir_all(&bin_dir).unwrap();
    make_fake_binary(&bin_dir.join("forge"), "9.9.9");

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--allow-dirty")
        .arg("--unsigned")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_PACKAGE_OUTPUT_DIR", root.join("dist"))
        .env("FORGE_PACKAGE_VERSION", "9.9.9")
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .env("FORGE_PACKAGE_BIN_DIR", root.join("bin"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("dist/release-manifest.json")).unwrap())
            .unwrap();
    assert_eq!(
        manifest["artifacts"][0]["sha256"],
        serde_json::Value::String(sha256(&root.join("dist/forge-9.9.9-linux-amd64.tar.gz")))
    );
}

#[test]
fn package_release_includes_required_files() {
    let root = test_root("package-contents");
    let bin_dir = root.join("bin/linux-amd64");
    fs::create_dir_all(&bin_dir).unwrap();
    make_fake_binary(&bin_dir.join("forge"), "9.9.9");

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--allow-dirty")
        .arg("--unsigned")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("FORGE_PACKAGE_OUTPUT_DIR", root.join("dist"))
        .env("FORGE_PACKAGE_VERSION", "9.9.9")
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .env("FORGE_PACKAGE_BIN_DIR", root.join("bin"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

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
fn package_release_injects_current_git_commit() {
    let root = init_package_repo("package-injects-git-commit");
    let expected_commit = git_stdout(&root, &["rev-parse", "HEAD"]);
    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--unsigned")
        .current_dir(&root)
        .env(
            "PATH",
            format!(
                "{}:{}",
                root.join("fake-bin").display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .output()
        .unwrap();

    assert!(output.status.success());
    let env_log = fs::read_to_string(root.join("cargo-env.log")).unwrap();
    let line = env_log.lines().last().unwrap();
    let parts = line.split('|').collect::<Vec<_>>();
    assert_eq!(parts[0], expected_commit);
    assert_eq!(parts[1], "false");
    assert_eq!(parts[3], "x86_64-unknown-linux-gnu");
}

#[test]
fn package_release_refuses_dirty_tree_by_default() {
    let root = init_package_repo("package-refuses-dirty");
    fs::write(root.join("README.md"), "dirty release notes\n").unwrap();

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .current_dir(&root)
        .env(
            "PATH",
            format!(
                "{}:{}",
                root.join("fake-bin").display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("signing mode required"), "{stderr}");
}

#[test]
fn package_release_unsigned_requires_flag() {
    let root = init_package_repo("package-unsigned-requires-flag");

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .current_dir(&root)
        .env(
            "PATH",
            format!(
                "{}:{}",
                root.join("fake-bin").display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("signing mode required"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn package_release_allows_dirty_tree_with_flag() {
    let root = init_package_repo("package-allows-dirty");
    fs::write(root.join("README.md"), "dirty release notes\n").unwrap();

    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--allow-dirty")
        .arg("--unsigned")
        .current_dir(&root)
        .env(
            "PATH",
            format!(
                "{}:{}",
                root.join("fake-bin").display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .output()
        .unwrap();

    assert!(output.status.success());
    let env_log = fs::read_to_string(root.join("cargo-env.log")).unwrap();
    let line = env_log.lines().last().unwrap();
    let parts = line.split('|').collect::<Vec<_>>();
    assert_eq!(parts[1], "true");
}

#[test]
fn package_release_does_not_reuse_stale_git_commit() {
    let root = init_package_repo("package-stale-git-commit");
    let path_env = format!(
        "{}:{}",
        root.join("fake-bin").display(),
        std::env::var("PATH").unwrap()
    );

    let first_commit = git_stdout(&root, &["rev-parse", "HEAD"]);
    let first = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--unsigned")
        .current_dir(&root)
        .env("PATH", &path_env)
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .output()
        .unwrap();
    assert!(first.status.success());

    fs::write(root.join("README.md"), "release notes v2\n").unwrap();
    git_ok(&root, &["add", "README.md"]);
    git_ok(&root, &["commit", "-m", "second"]);
    let second_commit = git_stdout(&root, &["rev-parse", "HEAD"]);

    let second = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--unsigned")
        .current_dir(&root)
        .env("PATH", &path_env)
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .output()
        .unwrap();
    assert!(second.status.success());

    let env_log = fs::read_to_string(root.join("cargo-env.log")).unwrap();
    let commits = env_log
        .lines()
        .map(|line| line.split('|').next().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(commits, vec![first_commit, second_commit.clone()]);
    assert_ne!(commits[0], second_commit);
}

#[test]
fn package_release_signed_emits_signature() {
    let root = init_package_repo("package-signed-emits-signature");
    let (private_key, _) = generate_signing_keypair(&root);
    let output = Command::new("bash")
        .arg("scripts/package-release.sh")
        .arg("--allow-dirty")
        .args(["--sign", "--signing-key"])
        .arg(&private_key)
        .current_dir(&root)
        .env(
            "PATH",
            format!(
                "{}:{}",
                root.join("fake-bin").display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("FORGE_PACKAGE_TARGETS", "linux-amd64")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(root.join("dist/release-manifest.sig").exists());
    assert!(root.join("dist/release-public-key.pem").exists());
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
    let options = default_signed_upgrade_options(&root, url.clone(), artifact.clone());

    let plan_output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || plan(&options).unwrap(),
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
    let options = default_signed_upgrade_options(&root, url.clone(), artifact.clone());

    let plan_output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || plan(&options).unwrap(),
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
    let manifest_path = write_unsigned_release_manifest(&root, &artifact, "2.0.0");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["schema_versions"]["storage_compatibility_version"] = serde_json::Value::from(2_u64);
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        allow_unsigned: true,
        ..default_upgrade_options(&root, url.clone(), artifact.clone())
    };

    let plan_output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || plan(&options).unwrap(),
    );
    assert!(plan_output.checks.iter().any(|check| {
        check.message.contains("Storage compatibility mismatch") && check.status == "error"
    }));
}

#[test]
fn upgrade_plan_rejects_checksum_mismatch() {
    let root = test_root("upgrade-checksum");
    let current = root.join("bin/forge");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    make_fake_binary(&current, "1.0.0");
    let artifact = make_artifact(&root, "2.0.0", 0o644);
    let manifest_path = root.join("release-manifest.json");
    write_release_manifest(
        &manifest_path,
        &artifact,
        "2.0.0",
        ARTIFACT_GIT_COMMIT,
        false,
        ARTIFACT_TARGET_TRIPLE,
    );
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["artifacts"][0]["sha256"] = serde_json::Value::String("deadbeef".into());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
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
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        allow_unsigned: true,
        ..default_upgrade_options(&root, url, artifact)
    };

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&options).unwrap_err();
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
    let options = default_signed_upgrade_options(&root, url, artifact);

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&options).unwrap_err();
            assert!(err.to_string().contains("world-writable"));
        },
    );
}

#[test]
fn upgrade_plan_rejects_missing_manifest_without_allow_unsigned() {
    let root = test_root("upgrade-missing-manifest");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let mut options = default_upgrade_options(&root, url, artifact);
    options.allow_unsigned = false;

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&options).unwrap_err();
            assert!(err.to_string().contains("release manifest required"));
        },
    );
}

#[test]
fn upgrade_plan_accepts_unsigned_with_allow_unsigned() {
    let root = test_root("upgrade-allow-unsigned");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let options = default_upgrade_options(&root, url, artifact);

    let output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || plan(&options).unwrap(),
    );

    assert_eq!(output.target_version, "2.0.0");
}

#[test]
fn upgrade_plan_rejects_artifact_not_in_manifest() {
    let root = test_root("upgrade-artifact-not-in-manifest");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let manifest_path = root.join("release-manifest.json");
    write_release_manifest(
        &manifest_path,
        &artifact,
        "2.0.0",
        ARTIFACT_GIT_COMMIT,
        false,
        ARTIFACT_TARGET_TRIPLE,
    );
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["artifacts"][0]["name"] =
        serde_json::Value::String("forge-other-linux-amd64.tar.gz".into());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        allow_unsigned: true,
        ..default_upgrade_options(&root, url, artifact)
    };

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&options).unwrap_err();
            assert!(err.to_string().contains("artifact not listed"));
        },
    );
}

#[test]
fn upgrade_plan_rejects_dirty_manifest_without_allow_dirty() {
    let root = test_root("upgrade-dirty-manifest");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let manifest_path = root.join("release-manifest.json");
    write_release_manifest(
        &manifest_path,
        &artifact,
        "2.0.0",
        ARTIFACT_GIT_COMMIT,
        true,
        ARTIFACT_TARGET_TRIPLE,
    );
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        allow_unsigned: true,
        ..default_upgrade_options(&root, url, artifact)
    };

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&options).unwrap_err();
            assert!(err.to_string().contains("dirty release manifest"));
        },
    );
}

#[test]
fn upgrade_plan_rejects_world_writable_manifest() {
    let root = test_root("upgrade-world-writable-manifest");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let manifest_path = write_unsigned_release_manifest(&root, &artifact, "2.0.0");
    let mut permissions = fs::metadata(&manifest_path).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&manifest_path, permissions).unwrap();
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        allow_unsigned: true,
        ..default_upgrade_options(&root, url, artifact)
    };

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&options).unwrap_err();
            assert!(err.to_string().contains("world-writable manifest"));
        },
    );
}

#[test]
fn upgrade_plan_rejects_invalid_signature() {
    let root = test_root("upgrade-invalid-signature");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let manifest_path = write_unsigned_release_manifest(&root, &artifact, "2.0.0");
    let signature_path = root.join("release-manifest.sig");
    let (private_key, public_key) = generate_signing_keypair(&root);
    sign_manifest(&manifest_path, &private_key, &signature_path);
    fs::write(&signature_path, "invalid-signature\n").unwrap();
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        signature_path: Some(signature_path),
        allow_unsigned: false,
        ..default_upgrade_options(&root, url, artifact)
    };

    with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            ("FORGE_RELEASE_PUBLIC_KEY", public_key.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || {
            let err = plan(&options).unwrap_err();
            assert!(
                err.to_string()
                    .contains("invalid release manifest signature")
                    || err.to_string().contains("signature verification failed")
            );
        },
    );
}

#[test]
fn upgrade_plan_accepts_valid_signature() {
    let root = test_root("upgrade-valid-signature");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let manifest_path = write_unsigned_release_manifest(&root, &artifact, "2.0.0");
    let signature_path = root.join("release-manifest.sig");
    let (private_key, public_key) = generate_signing_keypair(&root);
    sign_manifest(&manifest_path, &private_key, &signature_path);
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        signature_path: Some(signature_path),
        allow_unsigned: false,
        ..default_upgrade_options(&root, url, artifact)
    };

    let output = with_env(
        &[
            ("FORGE_UPGRADE_BINARY_PATH", current.display().to_string()),
            ("FORGE_RELEASE_PUBLIC_KEY", public_key.display().to_string()),
            (
                "PATH",
                format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap()),
            ),
        ],
        || plan(&options).unwrap(),
    );

    assert_eq!(output.target_version, "2.0.0");
}

#[test]
fn upgrade_apply_runs_same_verification_as_plan() {
    let root = test_root("upgrade-apply-plan-first");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let manifest_path = root.join("release-manifest.json");
    write_release_manifest(
        &manifest_path,
        &artifact,
        "2.0.0",
        ARTIFACT_GIT_COMMIT,
        false,
        ARTIFACT_TARGET_TRIPLE,
    );
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["artifacts"][0]["sha256"] = serde_json::Value::String("deadbeef".into());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
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
    let options = UpgradeOptions {
        manifest_path: Some(manifest_path),
        allow_unsigned: true,
        ..default_upgrade_options(&root, url, artifact)
    };

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
        || apply(&options).unwrap_err(),
    );

    assert!(err.to_string().contains("checksum mismatch"));
    assert!(!systemctl_log.exists());
}

#[test]
fn upgrade_apply_backs_up_current_binary() {
    let root = test_root("upgrade-apply-backup");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let options = default_signed_upgrade_options(&root, url.clone(), artifact.clone());

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
            apply(&options).unwrap();
        },
    );

    assert!(fs::read_to_string(previous).unwrap().contains("1.0.0"));
}

#[test]
fn upgrade_apply_uses_sudo_for_system_paths() {
    let root = test_root("upgrade-apply-sudo");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
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
    let options = default_signed_upgrade_options(&root, url.clone(), artifact.clone());

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
            apply(&options).unwrap();
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
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (url, _handle) = spawn_ok_server();
    let options = default_signed_upgrade_options(&root, url.clone(), artifact.clone());

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
            apply(&options).unwrap();
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
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();
    let (url, _handle) = spawn_readyz_sequence_server(vec![503, 503, 503, 503, 200, 200, 200]);
    let options = default_signed_upgrade_options(&root, caddy_url.clone(), artifact.clone());

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
        || apply(&options).unwrap(),
    );

    assert_eq!(output.result, "auto_rolled_back");
    assert!(fs::read_to_string(&current).unwrap().contains("1.0.0"));
}

#[test]
fn upgrade_apply_no_auto_rollback_preserves_failed_binary() {
    let root = test_root("upgrade-apply-no-auto-rollback");
    let (current, artifact) = prepare_upgrade_root(&root, "1.0.0", "2.0.0");
    let previous = root.join("bin/forge.previous");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();
    let mut options = default_signed_upgrade_options(&root, caddy_url.clone(), artifact.clone());
    options.auto_rollback = false;

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
        || apply(&options).unwrap_err(),
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
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();
    let mut options = default_signed_upgrade_options(&root, caddy_url.clone(), artifact.clone());
    options.auto_rollback = false;

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
        || apply(&options).unwrap_err(),
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
    let options = default_signed_upgrade_options(&root, url.clone(), artifact.clone());

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
            let output = apply(&options).unwrap();
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
    write_upgrade_config(&root, "bearer-secret-token");
    write_upgrade_env(&root, "master-secret-value");
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (apply_url, _handle1) = spawn_ok_server();
    let (rollback_url, _handle2) = spawn_ok_server();
    let options = default_signed_upgrade_options(&root, apply_url.clone(), artifact.clone());

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
            apply(&options).unwrap();
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
    let fake_bin = root.join("fake-bin");
    fs::create_dir_all(&fake_bin).unwrap();
    write_executable(&fake_bin.join("docker"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(&fake_bin.join("systemctl"), "#!/usr/bin/env bash\nexit 0\n");
    let (caddy_url, _caddy_handle) = spawn_ok_server();
    let mut options = default_signed_upgrade_options(&root, caddy_url.clone(), artifact.clone());
    options.auto_rollback = false;

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
        || apply(&options).unwrap_err(),
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
        .arg("--allow-dirty")
        .arg("--unsigned")
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
