// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters};
use crate::benchmark::BenchmarkParameters;
use crate::client::Instance;
use crate::settings::Settings;

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct DbParameters(tidehunter::config::Config);

impl Deref for DbParameters {
    type Target = tidehunter::config::Config;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for DbParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.frag_size)
    }
}

impl Display for DbParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Frag Size {}B", self.0.frag_size)
    }
}

impl ProtocolParameters for DbParameters {}

#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(transparent)]
pub struct StressClientParameters(benchmark::configs::StressClientParameters);

impl Deref for StressClientParameters {
    type Target = benchmark::configs::StressClientParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for StressClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.writes, self.reads)
    }
}

impl Display for StressClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} w - {} r", self.writes, self.reads)
    }
}

impl ProtocolParameters for StressClientParameters {}

impl ProtocolParameters for Vec<StressClientParameters> {}

pub struct TargetProtocol {
    working_dir: PathBuf,
}

impl ProtocolCommands for TargetProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec!["sudo apt -y install libfontconfig1-dev"]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        vec![]
    }

    async fn genesis_command<'a, I>(
        &self,
        _instances: I,
        parameters: &BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        let node_parameters = parameters.node_parameters.clone();
        let node_parameters_string = serde_yaml::to_string(&node_parameters).unwrap();
        let node_parameters_path = self.working_dir.join("node-parameters.yaml");
        let upload_node_parameters = format!(
            "echo -e '{node_parameters_string}' > {}",
            node_parameters_path.display()
        );

        let client_parameters = parameters.client_parameters.clone();
        let client_parameters_string = serde_yaml::to_string(&client_parameters).unwrap();
        let client_parameters_path = self.working_dir.join("client-parameters.yaml");
        let upload_client_parameters = format!(
            "echo -e '{client_parameters_string}' > {}",
            client_parameters_path.display()
        );

        [
            "source $HOME/.cargo/env",
            &upload_node_parameters,
            &upload_client_parameters,
        ]
        .join(" && ")
    }

    fn node_command<I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
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
        _instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
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
