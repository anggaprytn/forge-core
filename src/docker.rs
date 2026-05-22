use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::process::Command;

use crate::runtime::{
    BuildImageRequest, ContainerInspection, ContainerVolumeMount, CreateContainerRequest,
    CreateVolumeRequest, DockerRuntime, DockerRuntimeError, ManagedImage, ManagedVolume,
};

pub trait CommandRunner {
    fn run(&mut self, program: &str, args: &[String]) -> Result<String, DockerRuntimeError>;
    fn run_with_env(
        &mut self,
        program: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
    ) -> Result<String, DockerRuntimeError>;
}

pub struct ProcessCommandRunner;

impl CommandRunner for ProcessCommandRunner {
    fn run(&mut self, program: &str, args: &[String]) -> Result<String, DockerRuntimeError> {
        self.run_with_env(program, args, &BTreeMap::new())
    }

    fn run_with_env(
        &mut self,
        program: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
    ) -> Result<String, DockerRuntimeError> {
        let output = Command::new(program)
            .envs(env)
            .args(args)
            .output()
            .map_err(|err| DockerRuntimeError::CommandFailed(err.to_string()))?;
        if !output.status.success() {
            return Err(DockerRuntimeError::CommandFailed(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

pub struct DockerCliRuntime<R> {
    pub(crate) runner: R,
}

impl<R> DockerCliRuntime<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R: CommandRunner> DockerRuntime for DockerCliRuntime<R> {
    fn build_image(&mut self, request: BuildImageRequest) -> Result<String, DockerRuntimeError> {
        let mut args = vec![
            "build".to_string(),
            "-f".to_string(),
            request.dockerfile_path.display().to_string(),
            "-t".to_string(),
            request.image_tag.clone(),
        ];
        for (key, value) in &request.build_args {
            args.push("--build-arg".to_string());
            args.push(format!("{key}={value}"));
        }
        for (key, value) in &request.labels {
            args.push("--label".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(request.context_path.display().to_string());

        let output = self.runner.run("docker", &args)?;
        Ok(parse_built_image_ref(&output).unwrap_or(request.image_tag))
    }

    fn ensure_network(&mut self, network_name: &str) -> Result<(), DockerRuntimeError> {
        let inspect_args = vec![
            "network".to_string(),
            "inspect".to_string(),
            network_name.to_string(),
        ];
        if self.runner.run("docker", &inspect_args).is_ok() {
            return Ok(());
        }

        let create_args = vec![
            "network".to_string(),
            "create".to_string(),
            network_name.to_string(),
        ];
        self.runner.run("docker", &create_args).map(|_| ())
    }

    fn ensure_volume(&mut self, request: CreateVolumeRequest) -> Result<(), DockerRuntimeError> {
        let inspect_args = vec![
            "volume".to_string(),
            "inspect".to_string(),
            request.volume_name.clone(),
        ];
        if self.runner.run("docker", &inspect_args).is_ok() {
            return Ok(());
        }

        let mut create_args = vec![
            "volume".to_string(),
            "create".to_string(),
            request.volume_name.clone(),
        ];
        for (key, value) in &request.labels {
            create_args.push("--label".to_string());
            create_args.push(format!("{key}={value}"));
        }
        self.runner.run("docker", &create_args).map(|_| ())
    }

    fn create_container(
        &mut self,
        request: CreateContainerRequest,
    ) -> Result<String, DockerRuntimeError> {
        let mut args = vec![
            "create".to_string(),
            "--name".to_string(),
            request.container_name.clone(),
            "--restart".to_string(),
            "no".to_string(),
        ];
        if let Some(network_name) = &request.network_name {
            args.push("--network".to_string());
            args.push(network_name.clone());
        }
        for alias in &request.network_aliases {
            args.push("--network-alias".to_string());
            args.push(alias.clone());
        }
        for mount in &request.volume_mounts {
            args.push("--mount".to_string());
            args.push(format!(
                "type=volume,src={},dst={}",
                mount.volume_name, mount.mount_path
            ));
        }
        for (key, value) in &request.labels {
            args.push("--label".to_string());
            args.push(format!("{key}={value}"));
        }
        for key in request.environment.keys() {
            args.push("-e".to_string());
            args.push(key.clone());
        }
        args.push(request.image_ref.clone());
        if let Some(command) = &request.command {
            args.extend(command.iter().cloned());
        }

        let _ = self
            .runner
            .run_with_env("docker", &args, &request.environment)?;
        Ok(request.container_name)
    }

    fn start_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
        let args = vec!["start".to_string(), container_name.to_string()];
        self.runner.run("docker", &args).map(|_| ())
    }

    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<ContainerInspection, DockerRuntimeError> {
        let args = vec![
            "inspect".to_string(),
            "--format".to_string(),
            [
                "name={{.Name}}",
                "status={{.State.Status}}",
                "running={{.State.Running}}",
                "exit_code={{.State.ExitCode}}",
                "restart_count={{.RestartCount}}",
                "started_at={{.State.StartedAt}}",
                "image={{.Config.Image}}",
                "restart_policy={{.HostConfig.RestartPolicy.Name}}",
                "{{range $key, $value := .Config.Labels}}",
                "label:{{$key}}={{$value}}",
                "{{end}}",
                "{{range $name, $settings := .NetworkSettings.Networks}}",
                "network:{{$name}}={{$settings.IPAddress}}",
                "{{end}}",
                "{{range .Mounts}}",
                "mount:{{.Type}}:{{.Name}}={{.Destination}}",
                "{{end}}",
            ]
            .join("\n"),
            container_name.to_string(),
        ];
        let output = self.runner.run("docker", &args)?;
        parse_inspection_output(&output)
    }

    fn container_logs(
        &mut self,
        container_name: &str,
        tail_lines: usize,
    ) -> Result<String, DockerRuntimeError> {
        let command = format!(
            "docker logs --tail {} {} 2>&1",
            tail_lines,
            shell_quote(container_name)
        );
        self.runner
            .run("sh", &["-lc".to_string(), command])
            .map(|output| output.trim().to_string())
    }

    fn list_managed_containers(&mut self) -> Result<Vec<ContainerInspection>, DockerRuntimeError> {
        let args = vec![
            "ps".to_string(),
            "-a".to_string(),
            "--filter".to_string(),
            "label=forge.managed=true".to_string(),
            "--format".to_string(),
            "{{.Names}}".to_string(),
        ];
        let output = self.runner.run("docker", &args)?;
        let mut containers = Vec::new();
        for name in output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            containers.push(self.inspect_container(name)?);
        }
        Ok(containers)
    }

    fn list_managed_images(&mut self) -> Result<Vec<ManagedImage>, DockerRuntimeError> {
        let args = vec![
            "image".to_string(),
            "ls".to_string(),
            "--filter".to_string(),
            "label=forge.managed=true".to_string(),
            "--format".to_string(),
            "{{.Repository}}:{{.Tag}}".to_string(),
        ];
        let output = self.runner.run("docker", &args)?;
        let mut images = Vec::new();
        for image_ref in output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && *line != "<none>:<none>")
        {
            let inspect_args = vec![
                "image".to_string(),
                "inspect".to_string(),
                "--format".to_string(),
                [
                    "image={{join .RepoTags \",\"}}",
                    "{{range $key, $value := .Config.Labels}}",
                    "label:{{$key}}={{$value}}",
                    "{{end}}",
                ]
                .join("\n"),
                image_ref.to_string(),
            ];
            let inspection = self.runner.run("docker", &inspect_args)?;
            images.push(parse_image_inspection_output(&inspection)?);
        }
        Ok(images)
    }

    fn list_managed_volumes(&mut self) -> Result<Vec<ManagedVolume>, DockerRuntimeError> {
        let args = vec![
            "volume".to_string(),
            "ls".to_string(),
            "--filter".to_string(),
            "label=forge.managed=true".to_string(),
            "--format".to_string(),
            "{{.Name}}".to_string(),
        ];
        let output = self.runner.run("docker", &args)?;
        let mut volumes = Vec::new();
        for volume_name in output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            let inspect_args = vec![
                "volume".to_string(),
                "inspect".to_string(),
                "--format".to_string(),
                [
                    "name={{.Name}}",
                    "{{range $key, $value := .Labels}}",
                    "label:{{$key}}={{$value}}",
                    "{{end}}",
                ]
                .join("\n"),
                volume_name.to_string(),
            ];
            let inspection = self.runner.run("docker", &inspect_args)?;
            volumes.push(parse_volume_inspection_output(&inspection)?);
        }
        Ok(volumes)
    }

    fn stop_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
        let args = vec!["stop".to_string(), container_name.to_string()];
        self.runner.run("docker", &args).map(|_| ())
    }

    fn remove_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
        let args = vec![
            "rm".to_string(),
            "-f".to_string(),
            container_name.to_string(),
        ];
        self.runner.run("docker", &args).map(|_| ())
    }

    fn remove_image(&mut self, image_ref: &str) -> Result<(), DockerRuntimeError> {
        let args = vec!["rmi".to_string(), "-f".to_string(), image_ref.to_string()];
        self.runner.run("docker", &args).map(|_| ())
    }

    fn remove_volume(&mut self, volume_name: &str) -> Result<(), DockerRuntimeError> {
        let args = vec![
            "volume".to_string(),
            "rm".to_string(),
            volume_name.to_string(),
        ];
        self.runner.run("docker", &args).map(|_| ())
    }
}

fn parse_built_image_ref(output: &str) -> Option<String> {
    output
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("image_ref="))
        .map(|value| value.to_string())
}

fn parse_inspection_output(output: &str) -> Result<ContainerInspection, DockerRuntimeError> {
    let mut container_name = None;
    let mut running = None;
    let mut state_status = None;
    let mut exit_code = None;
    let mut restart_count = None;
    let mut started_at = None;
    let mut image_ref = None;
    let mut restart_policy = None;
    let mut labels = BTreeMap::new();
    let mut network_ips = BTreeMap::new();
    let mut volume_mounts = Vec::new();

    for line in output.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "name" => container_name = Some(value.trim_start_matches('/').to_string()),
            "status" => state_status = Some(value.to_string()),
            "running" => running = Some(value == "true"),
            "exit_code" => exit_code = value.parse::<i32>().ok(),
            "restart_count" => restart_count = value.parse::<u64>().ok(),
            "started_at" => {
                if !value.is_empty() && value != "0001-01-01T00:00:00Z" {
                    started_at = Some(value.to_string());
                }
            }
            "image" => image_ref = Some(value.to_string()),
            "restart_policy" => restart_policy = Some(value.to_string()),
            _ if key.starts_with("label:") => {
                labels.insert(
                    key.trim_start_matches("label:").to_string(),
                    value.to_string(),
                );
            }
            _ if key.starts_with("network:") => {
                network_ips.insert(
                    key.trim_start_matches("network:").to_string(),
                    value.to_string(),
                );
            }
            _ if key.starts_with("mount:volume:") => {
                volume_mounts.push(ContainerVolumeMount {
                    volume_name: key.trim_start_matches("mount:volume:").to_string(),
                    mount_path: value.to_string(),
                });
            }
            _ => {}
        }
    }

    Ok(ContainerInspection {
        container_name: container_name
            .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing container name".into()))?,
        running: running
            .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing running state".into()))?,
        state_status: state_status.unwrap_or_else(|| {
            if running.unwrap_or(false) {
                "running".into()
            } else {
                "exited".into()
            }
        }),
        exit_code,
        restart_count: restart_count.unwrap_or(0),
        started_at,
        image_ref: image_ref
            .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing image ref".into()))?,
        labels,
        network_ips,
        volume_mounts,
        restart_policy: restart_policy
            .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing restart policy".into()))?,
    })
}

fn parse_volume_inspection_output(output: &str) -> Result<ManagedVolume, DockerRuntimeError> {
    let mut volume_name = None;
    let mut labels = BTreeMap::new();

    for line in output.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "name" => volume_name = Some(value.to_string()),
            _ if key.starts_with("label:") => {
                labels.insert(
                    key.trim_start_matches("label:").to_string(),
                    value.to_string(),
                );
            }
            _ => {}
        }
    }

    Ok(ManagedVolume {
        volume_name: volume_name
            .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing volume name".into()))?,
        labels,
    })
}

fn parse_image_inspection_output(output: &str) -> Result<ManagedImage, DockerRuntimeError> {
    let mut image_ref = None;
    let mut labels = BTreeMap::new();

    for line in output.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "image" => {
                let resolved = value
                    .split(',')
                    .map(str::trim)
                    .find(|candidate| !candidate.is_empty())
                    .unwrap_or(value);
                image_ref = Some(resolved.to_string());
            }
            _ if key.starts_with("label:") => {
                labels.insert(
                    key.trim_start_matches("label:").to_string(),
                    value.to_string(),
                );
            }
            _ => {}
        }
    }

    Ok(ManagedImage {
        image_ref: image_ref
            .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing image ref".into()))?,
        labels,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCommand {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Default)]
pub struct RecordingCommandRunner {
    pub commands: Vec<RecordedCommand>,
    pub outputs: Vec<String>,
    pub envs: Vec<BTreeMap<String, String>>,
}

impl RecordingCommandRunner {
    pub fn with_outputs(outputs: Vec<String>) -> Self {
        Self {
            commands: Vec::new(),
            outputs,
            envs: Vec::new(),
        }
    }
}

impl CommandRunner for RecordingCommandRunner {
    fn run(&mut self, program: &str, args: &[String]) -> Result<String, DockerRuntimeError> {
        self.run_with_env(program, args, &BTreeMap::new())
    }

    fn run_with_env(
        &mut self,
        program: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
    ) -> Result<String, DockerRuntimeError> {
        self.commands.push(RecordedCommand {
            program: program.to_string(),
            args: args.to_vec(),
        });
        self.envs.push(env.clone());
        Ok(if self.outputs.is_empty() {
            String::new()
        } else {
            self.outputs.remove(0)
        })
    }
}

impl<R> Display for DockerCliRuntime<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "docker-cli-runtime")
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
fn labels(project_id: &str, environment: &str, generation: u64) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), project_id.into()),
        ("forge.environment".into(), environment.into()),
        ("forge.generation".into(), generation.to_string()),
    ])
}

#[cfg(test)]
pub mod docker_adapter_builds_image_with_labels {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn build_command_contains_expected_labels() {
        let runner = RecordingCommandRunner::with_outputs(vec!["image_ref=forge:test".into()]);
        let mut docker = DockerCliRuntime::new(runner);

        let built = docker
            .build_image(BuildImageRequest {
                image_tag: "forge:test".into(),
                context_path: PathBuf::from("."),
                dockerfile_path: PathBuf::from("./Dockerfile"),
                build_args: BTreeMap::new(),
                labels: labels("api", "production", 42),
            })
            .unwrap();

        assert_eq!(built, "forge:test");
        let args = &docker.runner.commands[0].args;
        assert!(args.iter().any(|arg| arg == "forge.managed=true"));
        assert!(args.iter().any(|arg| arg == "forge.project_id=api"));
        assert!(args.iter().any(|arg| arg == "forge.environment=production"));
        assert!(args.iter().any(|arg| arg == "forge.generation=42"));
    }
}

#[cfg(test)]
pub mod docker_adapter_starts_generation_named_container {
    use super::*;

    #[test]
    fn create_and_start_use_generation_container_name() {
        let runner =
            RecordingCommandRunner::with_outputs(vec!["prod-api-gen-42".into(), String::new()]);
        let mut docker = DockerCliRuntime::new(runner);
        let name = "prod-api-gen-42".to_string();

        docker
            .create_container(CreateContainerRequest {
                container_name: name.clone(),
                image_ref: "forge:test".into(),
                labels: labels("api", "production", 42),
                environment: Default::default(),
                network_name: None,
                network_aliases: Vec::new(),
                volume_mounts: Vec::new(),
                command: None,
            })
            .unwrap();
        docker.start_container(&name).unwrap();

        assert_eq!(docker.runner.commands[0].args[2], name);
        assert_eq!(docker.runner.commands[1].args[1], "prod-api-gen-42");
    }
}

#[cfg(test)]
pub mod docker_adapter_disables_restart_policy {
    use super::*;

    #[test]
    fn create_container_sets_restart_policy_to_no() {
        let runner = RecordingCommandRunner::with_outputs(vec!["prod-api-gen-42".into()]);
        let mut docker = DockerCliRuntime::new(runner);

        docker
            .create_container(CreateContainerRequest {
                container_name: "prod-api-gen-42".into(),
                image_ref: "forge:test".into(),
                labels: labels("api", "production", 42),
                environment: Default::default(),
                network_name: None,
                network_aliases: Vec::new(),
                volume_mounts: Vec::new(),
                command: None,
            })
            .unwrap();

        let args = &docker.runner.commands[0].args;
        assert!(args.windows(2).any(|pair| pair == ["--restart", "no"]));
    }
}

#[cfg(test)]
pub mod docker_adapter_inspects_running_state {
    use super::*;

    #[test]
    fn inspect_parses_running_state_and_labels() {
        let output = [
            "name=/prod-api-gen-42",
            "running=true",
            "image=forge:test",
            "restart_policy=no",
            "label:forge.managed=true",
            "label:forge.project_id=api",
            "network:forge-test=172.19.0.5",
        ]
        .join("\n");
        let runner = RecordingCommandRunner::with_outputs(vec![output]);
        let mut docker = DockerCliRuntime::new(runner);

        let inspection = docker.inspect_container("prod-api-gen-42").unwrap();

        assert!(inspection.running);
        assert_eq!(inspection.container_name, "prod-api-gen-42");
        assert_eq!(inspection.restart_policy, "no");
        assert_eq!(
            inspection.labels.get("forge.project_id"),
            Some(&"api".to_string())
        );
        assert_eq!(
            inspection.network_ips.get("forge-test"),
            Some(&"172.19.0.5".to_string())
        );
        let args = &docker.runner.commands[0].args;
        assert_eq!(args[0], "inspect");
        assert_eq!(args[1], "--format");
    }
}

#[cfg(test)]
pub mod docker_adapter_removes_failed_generation {
    use super::*;

    #[test]
    fn remove_container_uses_force_remove() {
        let runner = RecordingCommandRunner::with_outputs(vec![String::new()]);
        let mut docker = DockerCliRuntime::new(runner);

        docker.remove_container("prod-api-gen-42").unwrap();

        assert_eq!(
            docker.runner.commands[0].args,
            vec![
                "rm".to_string(),
                "-f".to_string(),
                "prod-api-gen-42".to_string()
            ]
        );
    }

    #[test]
    fn remove_image_uses_force_remove() {
        let runner = RecordingCommandRunner::with_outputs(vec![String::new()]);
        let mut docker = DockerCliRuntime::new(runner);

        docker.remove_image("forge:test").unwrap();

        assert_eq!(
            docker.runner.commands[0].args,
            vec![
                "rmi".to_string(),
                "-f".to_string(),
                "forge:test".to_string()
            ]
        );
    }

    #[test]
    fn list_managed_images_uses_label_filter() {
        let runner = RecordingCommandRunner::with_outputs(vec![
            "forge:test".into(),
            "image=forge:test\nlabel:forge.managed=true\nlabel:forge.project_id=api\nlabel:forge.environment=production\nlabel:forge.generation=42".into(),
        ]);
        let mut docker = DockerCliRuntime::new(runner);

        let images = docker.list_managed_images().unwrap();

        assert_eq!(images.len(), 1);
        assert_eq!(images[0].image_ref, "forge:test");
        assert_eq!(
            images[0].labels.get("forge.generation"),
            Some(&"42".to_string())
        );
        assert_eq!(docker.runner.commands[0].args[0], "image");
        assert_eq!(docker.runner.commands[0].args[1], "ls");
        assert_eq!(docker.runner.commands[1].args[0], "image");
        assert_eq!(docker.runner.commands[1].args[1], "inspect");
    }
}
