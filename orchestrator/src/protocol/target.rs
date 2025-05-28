// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};
use std::net::IpAddr;
use std::ops::Deref;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, BINARY_PATH};
use crate::benchmark::BenchmarkParameters;
use crate::client::Instance;
use crate::settings::Settings;

const TARGET_CONFIG_FILE: &str = "target_configs.yaml";

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct TargetConfigs(benchmark::configs::StressTestConfigs);

impl Deref for TargetConfigs {
    type Target = benchmark::configs::StressTestConfigs;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for TargetConfigs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[todo]")
    }
}

impl Display for TargetConfigs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[todo]")
    }
}

impl ProtocolParameters for TargetConfigs {}
impl ProtocolParameters for Vec<TargetConfigs> {}

pub struct TargetProtocol {
    working_dir: PathBuf,
}

impl ProtocolCommands for TargetProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec![
            "sudo apt -y install libfontconfig1-dev",
            "sudo apt-get install -y clang",
        ]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        vec![]
    }

    async fn genesis_command<'a, I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        // No need for genesis
        String::new()
    }

    fn node_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .zip(parameters.target_configs.iter())
            .map(|(instance, config)| {
                // Command to upload the config of the db and the stress client.
                let target_configs_string =
                    serde_yaml::to_string(&config.stress_client_parameters).unwrap();
                let target_configs_path = self.working_dir.join(TARGET_CONFIG_FILE);
                let upload_target_configs = format!(
                    "echo -e '{target_configs_string}' > {}",
                    target_configs_path.display()
                );

                // Command to run the benchmark
                let run = [
                    format!("./{BINARY_PATH}/benchmark"),
                    format!("--parameters-path {TARGET_CONFIG_FILE}"),
                ]
                .join(" ");

                // Join the commands to run on the instance.
                let command =
                    ["source $HOME/.cargo/env", &upload_target_configs, &run].join(" && ");
                (instance, command)
            })
            .collect()
    }
}

impl ProtocolMetrics for TargetProtocol {
    const BENCHMARK_DURATION: &'static str = "";
    const TOTAL_TRANSACTIONS: &'static str = "latency_s_count";
    const LATENCY_BUCKETS: &'static str = "latency_s";
    const LATENCY_SUM: &'static str = "latency_s_sum";
    const LATENCY_SQUARED_SUM: &'static str = "";

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .map(|x| {
                let ip = IpAddr::V4(x.main_ip);
                let path = format!("{ip}:{}/metrics", benchmark::configs::METRICS_PORT);
                (x, path)
            })
            .collect()
    }
}

impl TargetProtocol {
    /// Make a new instance of the target protocol commands generator.
    pub fn new(settings: &Settings) -> Self {
        Self {
            working_dir: settings.working_dir.clone(),
        }
    }
}
