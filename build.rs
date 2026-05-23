use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");

    println!(
        "cargo:rustc-env=FORGE_GIT_COMMIT={}",
        git_commit().unwrap_or_else(|| "unknown".into())
    );
    println!(
        "cargo:rustc-env=FORGE_BUILD_TIMESTAMP={}",
        build_timestamp()
    );
    println!(
        "cargo:rustc-env=FORGE_TARGET_TRIPLE={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".into())
    );
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

fn build_timestamp() -> String {
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
