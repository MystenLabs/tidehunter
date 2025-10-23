// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

use super::{Instance, InstanceStatus, ServerProviderClient};
use crate::error::{CloudProviderError, CloudProviderResult};
use crate::settings::Settings;
use crate::ssh::SshConnectionManager;

/// Configuration for a custom machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomMachine {
    /// The hostname or IP address of the machine.
    pub host: String,
    /// The region/group this machine belongs to.
    #[serde(default = "default_region")]
    pub region: String,
    /// Optional custom SSH port (defaults to 22).
    #[serde(default = "default_ssh_port")]
    pub ssh_port: u16,
    /// Optional custom specs description.
    #[serde(default)]
    pub specs: Option<String>,
}

fn default_ssh_port() -> u16 {
    22
}

fn default_region() -> String {
    "default".to_string()
}

impl CustomMachine {
    /// Convert the hostname/IP to an Ipv4Addr, resolving if necessary.
    pub fn resolve_ip(&self) -> CloudProviderResult<Ipv4Addr> {
        // Try to parse as IP address first
        if let Ok(ip) = self.host.parse::<Ipv4Addr>() {
            return Ok(ip);
        }

        // Try to resolve hostname
        use std::net::ToSocketAddrs;
        let socket_addr = format!("{}:{}", self.host, self.ssh_port);
        match socket_addr.to_socket_addrs() {
            Ok(mut addrs) => {
                if let Some(addr) = addrs.next()
                    && let std::net::IpAddr::V4(ipv4) = addr.ip()
                {
                    return Ok(ipv4);
                }
                Err(CloudProviderError::UnexpectedResponse(format!(
                    "Could not resolve hostname '{}' to IPv4 address",
                    self.host
                )))
            }
            Err(e) => Err(CloudProviderError::RequestError(format!(
                "Failed to resolve hostname '{}': {}",
                self.host, e
            ))),
        }
    }
}

/// A custom client for direct SSH access to predefined machines.
pub struct CustomClient {
    settings: Settings,
    machines: Vec<CustomMachine>,
    ssh_manager: SshConnectionManager,
}

impl Display for CustomClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Custom SSH client ({} machines)", self.machines.len())
    }
}

impl CustomClient {
    /// Create a new custom client.
    pub fn new(settings: Settings, machines: Vec<CustomMachine>) -> Self {
        let ssh_manager = SshConnectionManager::new(
            Self::USERNAME.to_string(),
            settings.ssh_private_key_file.clone(),
        )
        .with_timeout(settings.ssh_timeout)
        .with_retries(settings.ssh_retries);

        Self {
            settings,
            machines,
            ssh_manager,
        }
    }

    /// Convert a CustomMachine to an Instance.
    fn machine_to_instance(
        &self,
        machine: &CustomMachine,
        status: InstanceStatus,
    ) -> CloudProviderResult<Instance> {
        let ip = machine.resolve_ip()?;
        let id = format!("{}:{}", machine.host, machine.ssh_port);
        let specs = machine
            .specs
            .clone()
            .unwrap_or_else(|| self.settings.specs.clone());

        Ok(Instance {
            id,
            region: machine.region.clone(),
            main_ip: ip,
            tags: vec![self.settings.testbed_id.clone()],
            specs,
            status,
        })
    }

    /// Check if a machine is reachable via SSH.
    async fn check_machine_status(&self, machine: &CustomMachine) -> InstanceStatus {
        let ip = match machine.resolve_ip() {
            Ok(ip) => ip,
            Err(_) => return InstanceStatus::Terminated,
        };

        let addr = std::net::SocketAddr::new(ip.into(), machine.ssh_port);

        // Try to establish SSH connection
        match self.ssh_manager.connect(addr).await {
            Ok(_) => InstanceStatus::Active,
            Err(_) => InstanceStatus::Inactive,
        }
    }

    /// Filter machines based on region and testbed_id.
    fn filter_machines(&self, region: Option<&str>) -> Vec<&CustomMachine> {
        self.machines
            .iter()
            .filter(|machine| {
                // Filter by region if specified
                if let Some(region_filter) = region {
                    machine.region == region_filter
                } else {
                    // If no region filter, check if machine's region is in settings
                    self.settings.regions.contains(&machine.region)
                }
            })
            .collect()
    }
}

impl ServerProviderClient for CustomClient {
    const USERNAME: &'static str = "ubuntu";

    async fn list_instances(&self) -> CloudProviderResult<Vec<Instance>> {
        let mut instances = Vec::new();

        for machine in self.filter_machines(None) {
            let status = self.check_machine_status(machine).await;
            let instance = self.machine_to_instance(machine, status)?;
            instances.push(instance);
        }

        Ok(instances)
    }

    async fn start_instances<'a, I>(&self, _instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        // For custom machines, we assume they're always running or managed externally
        // This is a no-op, but we could implement wake-on-LAN or similar here
        Ok(())
    }

    async fn stop_instances<'a, I>(&self, _instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        // For custom machines, we don't control their power state
        // This is a no-op, but we could implement shutdown commands here
        Ok(())
    }

    async fn create_instance<S>(&self, _region: S) -> CloudProviderResult<Instance>
    where
        S: Into<String> + Serialize + Send,
    {
        // Custom machines are predefined, so we can't create new instances
        Err(CloudProviderError::RequestError(
            "Cannot create instances in custom mode - machines must be predefined".to_string(),
        ))
    }

    async fn delete_instance(&self, _instance: Instance) -> CloudProviderResult<()> {
        // Custom machines are predefined, so we can't delete instances
        // This is a no-op since machines are managed externally
        Ok(())
    }

    async fn register_ssh_public_key(&self, _public_key: String) -> CloudProviderResult<()> {
        // In custom mode, SSH keys are assumed to be pre-configured
        // This is a no-op
        Ok(())
    }

    async fn instance_setup_commands(&self) -> CloudProviderResult<Vec<String>> {
        // Return generic setup commands for custom machines
        // These should be safe to run on most Ubuntu/Debian systems
        Ok(vec![])
    }
}
