// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::benchmark::BenchmarkParameters;
use crate::client::Instance;
use crate::error::{MonitorError, MonitorResult};
use crate::protocol::ProtocolMetrics;
use crate::ssh::{CommandContext, SshConnectionManager};

pub struct Monitor {
    instance: Instance,
    nodes: Vec<Instance>,
    ssh_manager: SshConnectionManager,
}

impl Monitor {
    /// Create a new monitor.
    pub fn new(
        instance: Instance,
        nodes: Vec<Instance>,
        ssh_manager: SshConnectionManager,
    ) -> Self {
        Self {
            instance,
            nodes,
            ssh_manager,
        }
    }

    /// Dependencies to install.
    pub fn dependencies(use_grafana_cloud: bool) -> Vec<String> {
        let mut commands: Vec<String> = Vec::new();
        
        if use_grafana_cloud {
            // When using Grafana Cloud, install Alloy instead of Prometheus/Grafana
            commands.extend(Alloy::install_commands().into_iter().map(String::from));
        } else {
            // Traditional setup with Prometheus and Grafana
            commands.extend(Prometheus::install_commands().into_iter().map(String::from));
            commands.extend(Grafana::install_commands().into_iter().map(String::from));
        }
        
        commands.extend(NodeExporter::install_commands());
        commands
    }

    /// Start a prometheus instance on the dedicated motoring machine.
    pub async fn start_prometheus<P: ProtocolMetrics>(
        &self,
        protocol_commands: &P,
        parameters: &BenchmarkParameters,
    ) -> MonitorResult<()> {
        if parameters.settings.use_grafana_cloud {
            // Use Alloy to forward metrics to Grafana Cloud
            self.start_alloy(protocol_commands, parameters).await?;
        } else {
            // Configure and reload prometheus.
            let instance = [self.instance.clone()];
            let commands =
                Prometheus::setup_commands(self.nodes.clone(), protocol_commands, parameters);
            self.ssh_manager
                .execute(instance, commands, CommandContext::default())
                .await?;
        }

        Ok(())
    }

    /// Start Alloy on the dedicated monitoring machine to forward metrics to Grafana Cloud.
    pub async fn start_alloy<P: ProtocolMetrics>(
        &self,
        protocol_commands: &P,
        parameters: &BenchmarkParameters,
    ) -> MonitorResult<()> {
        // Configure and start alloy.
        let instance = [self.instance.clone()];
        let commands = Alloy::setup_commands(self.nodes.clone(), protocol_commands, parameters);
        self.ssh_manager
            .execute(instance, commands, CommandContext::default())
            .await?;

        Ok(())
    }

    /// Start grafana on the dedicated motoring machine.
    pub async fn start_grafana(&self, parameters: &BenchmarkParameters) -> MonitorResult<()> {
        if !parameters.settings.use_grafana_cloud {
            // Configure and reload grafana.
            let instance = std::iter::once(self.instance.clone());
            let commands = Grafana::setup_commands();
            self.ssh_manager
                .execute(instance, commands, CommandContext::default())
                .await?;
        }
        // If using Grafana Cloud, we don't need to start local Grafana

        Ok(())
    }

    /// The public address of the grafana instance.
    pub fn grafana_address(&self) -> String {
        format!("http://{}:{}", self.instance.main_ip, Grafana::DEFAULT_PORT)
    }

    /// The public address of the prometheus instance.
    pub fn prometheus_address(&self) -> String {
        format!(
            "http://{}:{}",
            self.instance.main_ip,
            Prometheus::DEFAULT_PORT
        )
    }
}

/// Generate the commands to setup prometheus on the given instances.
pub struct Prometheus;

impl Prometheus {
    /// The default prometheus configuration path.
    const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "/etc/prometheus/prometheus.yml";
    /// The default prometheus port.
    pub const DEFAULT_PORT: u16 = 9090;

    /// The commands to install prometheus.
    pub fn install_commands() -> Vec<&'static str> {
        vec![
            "sudo apt-get -y install prometheus",
            "sudo chmod 777 -R /var/lib/prometheus/ /etc/prometheus/",
        ]
    }

    /// Generate the commands to update the prometheus configuration and restart prometheus.
    pub fn setup_commands<I, P>(nodes: I, protocol: &P, parameters: &BenchmarkParameters) -> String
    where
        I: IntoIterator<Item = Instance>,
        P: ProtocolMetrics,
    {
        // Generate the prometheus configuration.
        let mut config = vec![Self::global_configuration()];

        let nodes_metrics_path = protocol.nodes_metrics_path(nodes, parameters);
        for (i, (_, nodes_metrics_path)) in nodes_metrics_path.into_iter().enumerate() {
            let id = format!("node-{i}");
            let scrape_config = Self::scrape_configuration(&id, &nodes_metrics_path);
            config.push(scrape_config);
        }

        // Make the command to configure and restart prometheus.
        format!(
            "sudo echo \"{}\" > {} && sudo service prometheus restart",
            config.join("\n"),
            Self::DEFAULT_PROMETHEUS_CONFIG_PATH
        )
    }

    /// Generate the global prometheus configuration.
    /// NOTE: The configuration file is a yaml file so spaces are important.
    fn global_configuration() -> String {
        [
            "global:",
            "  scrape_interval: 5s",
            "  evaluation_interval: 5s",
            "scrape_configs:",
        ]
        .join("\n")
    }

    /// Generate the prometheus configuration from the given metrics path.
    /// NOTE: The configuration file is a yaml file so spaces are important.
    fn scrape_configuration(id: &str, nodes_metrics_path: &str) -> String {
        let parts: Vec<_> = nodes_metrics_path.split('/').collect();
        let address = parts[0].parse::<SocketAddr>().unwrap();
        let ip = address.ip();
        let port = address.port();
        let path = parts[1];

        [
            &format!("  - job_name: instance-{id}"),
            &format!("    metrics_path: /{path}"),
            "    static_configs:",
            "      - targets:",
            &format!("        - {ip}:{port}"),
            &format!("  - job_name: instance-node-exporter-{id}"),
            "    static_configs:",
            "      - targets:",
            &format!("        - {ip}:9200"),
        ]
        .join("\n")
    }
}

pub struct Grafana;

impl Grafana {
    /// The path to the datasources directory.
    const DATASOURCES_PATH: &'static str = "/etc/grafana/provisioning/datasources";
    /// The default grafana port.
    pub const DEFAULT_PORT: u16 = 3000;

    /// The commands to install grafana.
    pub fn install_commands() -> Vec<&'static str> {
        vec![
            "sudo apt-get install -y apt-transport-https software-properties-common wget",
            "sudo wget -q -O /etc/apt/keyrings/grafana.key https://apt.grafana.com/gpg.key",
            "(sudo rm /etc/apt/sources.list.d/grafana.list || true)",
            "echo \
                \"deb [signed-by=/etc/apt/keyrings/grafana.key] \
                https://apt.grafana.com stable main\" \
                | sudo tee -a /etc/apt/sources.list.d/grafana.list",
            "sudo apt-get update",
            "sudo apt-get install -y grafana",
            "sudo chmod 777 -R /etc/grafana/",
        ]
    }

    /// Generate the commands to update the grafana datasource and restart grafana.
    pub fn setup_commands() -> String {
        [
            &format!("(rm -r {} || true)", Self::DATASOURCES_PATH),
            &format!("mkdir -p {}", Self::DATASOURCES_PATH),
            &format!(
                "sudo echo \"{}\" > {}/testbed.yml",
                Self::datasource(),
                Self::DATASOURCES_PATH
            ),
            "sudo service grafana-server restart",
        ]
        .join(" && ")
    }

    /// Generate the content of the datasource file for the given instance.
    /// NOTE: The datasource file is a yaml file so spaces are important.
    fn datasource() -> String {
        [
            "apiVersion: 1",
            "deleteDatasources:",
            "  - name: testbed",
            "    orgId: 1",
            "datasources:",
            "  - name: testbed",
            "    type: prometheus",
            "    access: proxy",
            "    orgId: 1",
            &format!("    url: http://localhost:{}", Prometheus::DEFAULT_PORT),
            "    editable: true",
            "    uid: Fixed-UID-testbed",
        ]
        .join("\n")
    }
}

pub struct Alloy;

impl Alloy {

    /// The commands to install grafana alloy.
    pub fn install_commands() -> Vec<&'static str> {
        vec![
            "curl -fsSL https://apt.grafana.com/gpg.key | sudo apt-key add -",
            "echo \"deb https://apt.grafana.com stable main\" | sudo tee -a /etc/apt/sources.list.d/grafana.list",
            "sudo apt-get update",
            "sudo apt-get install -y alloy",
            "sudo mkdir -p /etc/alloy",
            "sudo chmod 777 -R /etc/alloy/",
        ]
    }

    /// Generate the commands to setup alloy configuration and start the service.
    pub fn setup_commands<I, P>(
        nodes: I,
        protocol: &P,
        parameters: &BenchmarkParameters,
    ) -> String
    where
        I: IntoIterator<Item = Instance>,
        P: ProtocolMetrics,
    {
        let nodes_metrics_paths = protocol
            .nodes_metrics_path(nodes, parameters)
            .into_iter()
            .map(|(_, path)| {
                // Extract just the port from the path (format is "ip:port/path")
                let parts: Vec<_> = path.split('/').collect();
                let address = parts[0].parse::<SocketAddr>().unwrap();
                format!("{}:{}", address.ip(), address.port())
            })
            .collect::<Vec<_>>();

        let config = Self::generate_config(&nodes_metrics_paths, parameters);

        [
            &format!("sudo echo '{}' > /etc/alloy/config.alloy", config.replace("'", "\\'")),
            "echo 'Generated Alloy config:'",
            "sudo cat /etc/alloy/config.alloy",
            "echo 'Validating Alloy config...'",
            "sudo alloy validate /etc/alloy/config.alloy || (echo 'Config validation failed!' && cat /etc/alloy/config.alloy && exit 1)",
            "sudo systemctl enable alloy",
            "sudo systemctl restart alloy",
            "sleep 5",
            "sudo systemctl status alloy --no-pager",
        ]
        .join(" && ")
    }

    /// Generate the alloy configuration file content.
    fn generate_config(nodes_metrics_paths: &[String], parameters: &BenchmarkParameters) -> String {
        let settings = &parameters.settings;
        
        // Safely escape strings for HCL
        fn escape_hcl_string(s: &str) -> String {
            s.replace("\\", "\\\\")
             .replace("\"", "\\\"")
             .replace("\n", "\\n")
             .replace("\r", "\\r")
             .replace("\t", "\\t")
        }
        
        // Build external labels with the specific ones requested
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
            
        let labels = vec![
            format!("        host    = \"{}\"", escape_hcl_string(&hostname)),
            format!("        system  = \"tidehunter\""),
            format!("        commit  = \"{}\"", escape_hcl_string(&settings.repository.commit)),
        ];

        let external_labels = labels.join("\n");

        // Generate basic auth section if credentials are provided
        let auth_section = if let (Some(username), Some(_url)) = (&settings.grafana_cloud_username, &settings.grafana_cloud_url) {
            let password = settings.load_grafana_cloud_password()
                .unwrap_or_default()
                .unwrap_or_else(|| "".to_string());
            
            if !username.is_empty() && !password.is_empty() {
                format!(r#"
        basic_auth {{
            username = "{}"
            password = "{}"
        }}"#, escape_hcl_string(username), escape_hcl_string(&password))
            } else {
                "".to_string()
            }
        } else {
            "".to_string()
        };

        let default_url = "https://gateway-2.mimir.sui.io/api/v1/push".to_string();
        let endpoint_url = settings.grafana_cloud_url.as_ref()
            .unwrap_or(&default_url);

        // Format targets for multiple addresses
        let targets = if nodes_metrics_paths.is_empty() {
            "        // No targets configured yet".to_string()
        } else {
            nodes_metrics_paths
                .iter()
                .map(|addr| format!("        {{__address__ = \"{}\"}},", escape_hcl_string(addr)))
                .collect::<Vec<_>>()
                .join("\n")
        };

        format!(r#"prometheus.scrape "tidehunter_benchmark" {{
    targets = [
{}
    ]
    forward_to = [prometheus.remote_write.grafana_cloud.receiver]
    job_name   = "tidehunter_benchmark"
}}

prometheus.remote_write "grafana_cloud" {{
    external_labels = {{
{}
    }}

    endpoint {{
        name = "grafana-cloud"
        url  = "{}"{}
    }}
}}"#, targets, external_labels, escape_hcl_string(endpoint_url), auth_section)
    }
}

#[allow(dead_code)] // TODO: Will be used to observe local testbeds (#8)
/// Bootstrap the grafana with datasource to connect to the given instances.
/// NOTE: Only for macOS. Grafana must be installed through homebrew (and not from source).
/// Deeper grafana configuration can be done through the grafana.ini file
/// (/opt/homebrew/etc/grafana/grafana.ini) or the plist file
/// (~/Library/LaunchAgents/homebrew.mxcl.grafana.plist).
pub struct LocalGrafana;

#[allow(dead_code)] // TODO: Will be used to observe local testbeds (#8)
impl LocalGrafana {
    /// The default grafana home directory (macOS, homebrew install).
    const DEFAULT_GRAFANA_HOME: &'static str = "/opt/homebrew/opt/grafana/share/grafana/";
    /// The path to the datasources directory.
    const DATASOURCES_PATH: &'static str = "conf/provisioning/datasources/";
    /// The default grafana port.
    pub const DEFAULT_PORT: u16 = 3000;

    /// Configure grafana to connect to the given instances. Only for macOS.
    pub fn run<I>(instances: I) -> MonitorResult<()>
    where
        I: IntoIterator<Item = Instance>,
    {
        let path: PathBuf = [Self::DEFAULT_GRAFANA_HOME, Self::DATASOURCES_PATH]
            .iter()
            .collect();

        // Remove the old datasources.
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir(&path).unwrap();

        // Create the new datasources.
        for (i, instance) in instances.into_iter().enumerate() {
            let mut file = path.clone();
            file.push(format!("instance-{}.yml", i));
            fs::write(&file, Self::datasource(&instance, i)).map_err(|e| {
                MonitorError::GrafanaError(format!("Failed to write grafana datasource ({e})"))
            })?;
        }

        // Restart grafana.
        std::process::Command::new("brew")
            .arg("services")
            .arg("restart")
            .arg("grafana")
            .arg("-q")
            .spawn()
            .map_err(|e| MonitorError::GrafanaError(e.to_string()))?;

        Ok(())
    }

    /// Generate the content of the datasource file for the given instance. This grafana instance
    /// takes one datasource per instance and assumes one prometheus server runs per instance.
    /// NOTE: The datasource file is a yaml file so spaces are important.
    fn datasource(instance: &Instance, index: usize) -> String {
        [
            "apiVersion: 1",
            "deleteDatasources:",
            &format!("  - name: instance-{index}"),
            "    orgId: 1",
            "datasources:",
            &format!("  - name: instance-{index}"),
            "    type: prometheus",
            "    access: proxy",
            "    orgId: 1",
            &format!(
                "    url: http://{}:{}",
                instance.main_ip,
                Prometheus::DEFAULT_PORT
            ),
            "    editable: true",
            &format!("    uid: UID-{index}"),
        ]
        .join("\n")
    }
}

/// Generate the commands to setup node exporter on the given instances.
struct NodeExporter;

impl NodeExporter {
    const RELEASE: &'static str = "0.18.1";
    const DEFAULT_PORT: u16 = 9200;
    const SERVICE_PATH: &'static str = "/etc/systemd/system/node_exporter.service";

    pub fn install_commands() -> Vec<String> {
        let build = format!("node_exporter-{}.linux-amd64", Self::RELEASE);
        let source = format!(
            "https://github.com/prometheus/node_exporter/releases/download/v{}/{build}.tar.gz",
            Self::RELEASE
        );

        [
            "(sudo systemctl status node_exporter && exit 0)",
            &format!("curl -LO {source}"),
            &format!(
                "tar -xvf node_exporter-{}.linux-amd64.tar.gz",
                Self::RELEASE
            ),
            &format!(
                "sudo mv node_exporter-{}.linux-amd64/node_exporter /usr/local/bin/",
                Self::RELEASE
            ),
            "sudo useradd -rs /bin/false node_exporter || true",
            "sudo chmod 777 -R /etc/systemd/system/",
            &format!(
                "sudo echo \"{}\" > {}",
                Self::service_config(),
                Self::SERVICE_PATH
            ),
            "sudo systemctl daemon-reload",
            "sudo systemctl start node_exporter",
            "sudo systemctl enable node_exporter",
        ]
        .map(|x| x.to_string())
        .to_vec()
    }

    fn service_config() -> String {
        [
            "[Unit]",
            "Description=Node Exporter",
            "After=network.target",
            "[Service]",
            "User=node_exporter",
            "Group=node_exporter",
            "Type=simple",
            &format!(
                "ExecStart=/usr/local/bin/node_exporter --web.listen-address=:{}",
                Self::DEFAULT_PORT
            ),
            "[Install]",
            "WantedBy=multi-user.target",
        ]
        .join("\n")
    }
}
