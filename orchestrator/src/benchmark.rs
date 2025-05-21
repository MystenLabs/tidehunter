// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::protocol::ProtocolParameters;
use crate::settings::Settings;
use crate::{ClientParameters, NodeParameters};

/// Shortcut avoiding to use the generic version of the benchmark parameters.
pub type BenchmarkParameters = BenchmarkParametersGeneric<NodeParameters, ClientParameters>;

/// The benchmark parameters for a run. These parameters are stored along with the performance data
/// and should be used to reproduce the results.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkParametersGeneric<N, C> {
    /// The testbed settings.
    pub settings: Settings,
    /// The node's configuration parameters.
    pub node_parameters: N,
    /// The client's configuration parameters.
    pub client_parameters: Vec<C>,
}

impl<N: Debug, C: Debug> Debug for BenchmarkParametersGeneric<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}-{:?}", self.node_parameters, self.client_parameters)
    }
}

impl<N: Display, C> Display for BenchmarkParametersGeneric<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.node_parameters)
    }
}

impl<N: ProtocolParameters, C: ProtocolParameters> BenchmarkParametersGeneric<N, C> {
    /// Make a new benchmark parameters.
    pub fn new(settings: Settings, node_parameters: N, client_parameters: Vec<C>) -> Self {
        Self {
            settings,
            node_parameters,
            client_parameters,
        }
    }

    /// Split the benchmark parameters into `machines` parts. Each part contains the same node
    /// parameters and a subset of the client parameters.
    pub fn split(self, machines: usize) -> Vec<Self> {
        self.client_parameters
            .chunks(machines)
            .map(|chunk| Self {
                settings: self.settings.clone(),
                node_parameters: self.node_parameters.clone(),
                client_parameters: chunk.to_vec(),
            })
            .collect()
    }
}

#[cfg(test)]
pub mod test {
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{Deserialize, Serialize};

    use crate::benchmark::BenchmarkParametersGeneric;

    use super::ProtocolParameters;

    /// Mock benchmark type for unit tests.
    #[derive(
        Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default,
    )]
    pub struct TestNodeConfig;

    impl Display for TestNodeConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestNodeConfig")
        }
    }

    impl FromStr for TestNodeConfig {
        type Err = ();

        fn from_str(_s: &str) -> Result<Self, Self::Err> {
            Ok(Self {})
        }
    }

    impl ProtocolParameters for TestNodeConfig {}

    #[test]
    fn split_benchmark_parameters() {
        let parameters = BenchmarkParametersGeneric::<TestNodeConfig, TestNodeConfig>::new(
            Default::default(),
            TestNodeConfig::default(),
            vec![TestNodeConfig::default(); 10],
        );
        let split = parameters.split(3);
        assert_eq!(split.len(), 4);
        assert_eq!(split[0].client_parameters.len(), 3);
        assert_eq!(split[1].client_parameters.len(), 3);
        assert_eq!(split[2].client_parameters.len(), 3);
        assert_eq!(split[3].client_parameters.len(), 1);
    }
}
