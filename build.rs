use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");
    println!("cargo:rerun-if-changed=.git/refs/tags");
    println!("cargo:rerun-if-changed=.git/packed-refs");
    println!("cargo:rerun-if-changed=.git/index");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-env-changed=FORGE_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=FORGE_GIT_DIRTY");
    println!("cargo:rerun-if-env-changed=FORGE_BUILD_TIMESTAMP");
    println!("cargo:rerun-if-env-changed=FORGE_TARGET_TRIPLE");

    println!(
        "cargo:rustc-env=FORGE_GIT_COMMIT={}",
        metadata_override("FORGE_GIT_COMMIT")
            .or_else(git_commit)
            .unwrap_or_else(|| "unknown".into())
    );
    println!(
        "cargo:rustc-env=FORGE_GIT_DIRTY={}",
        metadata_override("FORGE_GIT_DIRTY")
            .or_else(git_dirty)
            .unwrap_or_else(|| "unknown".into())
    );
    println!(
        "cargo:rustc-env=FORGE_BUILD_TIMESTAMP={}",
        build_timestamp()
    );
    println!(
        "cargo:rustc-env=FORGE_TARGET_TRIPLE={}",
        metadata_override("FORGE_TARGET_TRIPLE")
            .or_else(|| std::env::var("TARGET").ok())
            .unwrap_or_else(|| "unknown".into())
    );
}

fn metadata_override(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn git_dirty() -> Option<String> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    Some((!value.trim().is_empty()).to_string())
}

fn build_timestamp() -> String {
    if let Some(value) = metadata_override("FORGE_BUILD_TIMESTAMP") {
        return value;
    }
    if let Ok(value) = std::env::var("SOURCE_DATE_EPOCH") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "unknown".into())
}
