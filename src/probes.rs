use std::fmt::{Display, Formatter};
use std::process::Command;

use crate::runtime::{ProbeError, ProbeRuntime};

#[derive(Debug)]
pub enum ProbeRuntimeInitError {
    MissingNetworkName,
}

impl Display for ProbeRuntimeInitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingNetworkName => write!(f, "probe runtime requires a docker network name"),
        }
    }
}

impl std::error::Error for ProbeRuntimeInitError {}

pub struct DockerNetworkProbeRuntime {
    network_name: String,
    internal_port: u16,
    image_ref: String,
}

impl DockerNetworkProbeRuntime {
    pub fn new(network_name: impl Into<String>, internal_port: u16) -> Self {
        Self {
            network_name: network_name.into(),
            internal_port,
            image_ref: "busybox:1.36".into(),
        }
    }

    fn run_probe(&self, command: &str) -> Result<bool, ProbeError> {
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                self.network_name.as_str(),
                self.image_ref.as_str(),
                "sh",
                "-lc",
                command,
            ])
            .output()
            .map_err(|err| ProbeError::Failed(err.to_string()))?;

        Ok(output.status.success())
    }
}

impl ProbeRuntime for DockerNetworkProbeRuntime {
    fn probe_tcp(&mut self, container_name: &str) -> Result<bool, ProbeError> {
        self.run_probe(&format!(
            "nc -z -w 1 {container_name} {}",
            self.internal_port
        ))
    }

    fn probe_http(&mut self, container_name: &str, path: &str) -> Result<bool, ProbeError> {
        self.run_probe(&format!(
            "wget -q -T 2 -O /dev/null http://{container_name}:{}{}",
            self.internal_port, path
        ))
    }
}
