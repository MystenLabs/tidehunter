// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, BINARY_PATH};
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
pub struct StressClientParameters {
    content: usize,
}

impl Deref for StressClientParameters {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        todo!()
    }
}

impl Debug for StressClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!();
    }
}

impl Display for StressClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!();
    }
}

impl ProtocolParameters for StressClientParameters {}

impl ProtocolParameters for Vec<StressClientParameters> {}

pub struct MysticetiProtocol {
    working_dir: PathBuf,
}

impl ProtocolCommands for MysticetiProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec!["sudo apt -y install libfontconfig1-dev"]
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        vec![self.working_dir.join("storage-*")]
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
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }

    fn client_command<I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // TODO: Isolate clients from the node (#9).
        vec![]
    }
}

impl ProtocolMetrics for MysticetiProtocol {
    const BENCHMARK_DURATION: &'static str = "";
    const TOTAL_TRANSACTIONS: &'static str = "latency_s_count";
    const LATENCY_BUCKETS: &'static str = "latency_s";
    const LATENCY_SUM: &'static str = "latency_s_sum";
    const LATENCY_SQUARED_SUM: &'static str = "";

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }

    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // NOTE: Hack to avoid clients metrics.
        todo!()
    }
}

impl MysticetiProtocol {
    /// Make a new instance of the Mysticeti protocol commands generator.
    pub fn new(settings: &Settings) -> Self {
        Self {
            working_dir: settings.working_dir.clone(),
        }
    }
}
