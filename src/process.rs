use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    SpawnFailed(String),
    TimedOut { program: String, timeout: Duration },
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SpawnFailed(message) => write!(f, "{message}"),
            Self::TimedOut { program, timeout } => {
                write!(f, "{program} timed out after {}s", timeout.as_secs())
            }
        }
    }
}

impl std::error::Error for CommandError {}

pub fn run_command_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> Result<Output, CommandError> {
    let program = command.get_program().to_string_lossy().into_owned();
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| CommandError::SpawnFailed(err.to_string()))?;
    let started = Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .map_err(|err| CommandError::SpawnFailed(err.to_string()));
            }
            Ok(None) if started.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(CommandError::TimedOut { program, timeout });
            }
            Ok(None) => thread::sleep(Duration::from_millis(25)),
            Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(CommandError::SpawnFailed(err.to_string()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_command_with_timeout_returns_output() {
        let output = run_command_with_timeout(
            Command::new("sh").arg("-lc").arg("printf ok"),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(String::from_utf8_lossy(&output.stdout), "ok");
    }

    #[test]
    fn run_command_with_timeout_kills_stalled_process() {
        let err = run_command_with_timeout(
            Command::new("sh").arg("-lc").arg("sleep 1"),
            Duration::from_millis(100),
        )
        .unwrap_err();
        assert!(matches!(err, CommandError::TimedOut { .. }));
    }
}
