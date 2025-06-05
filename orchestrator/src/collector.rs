// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use futures::future::try_join_all;
use prometheus_http_query::response::Data;
use prometheus_http_query::Client as PrometheusClient;
use serde::Serialize;

use crate::benchmark::BenchmarkParameters;
use crate::error::{MonitorResult, TestbedResult};
use crate::settings::Settings;
use crate::Config;

type JobId = String;
type MetricLabel = String;
type Le = String;
type Buckets = HashMap<Le, Vec<Sample>>;

#[derive(Serialize)]
struct Sample {
    /// The timestamp of the sample.
    timestamp: f64,
    /// The value of the sample.
    value: f64,
}

#[derive(Serialize)]
pub struct BenchmarkResult {
    settings: Settings,
    /// The configuration used for the benchmark.
    config: Config,
    /// Id of the collector job that collected the metrics.
    collector: JobId,
    results: HashMap<MetricLabel, Buckets>,
}

impl BenchmarkResult {
    /// Save the collection of measurements as a json file.
    pub fn save<P: AsRef<Path>>(&self, path: P) {
        let json = serde_json::to_string_pretty(self).expect("Cannot serialize metrics");
        let mut file = PathBuf::from(path.as_ref());
        file.push(format!("measurements-{:?}.json", self.config));
        fs::write(file, json).unwrap();
    }
}

pub struct MetricsCollector {
    parameters: BenchmarkParameters,
    client: PrometheusClient,
    metrics: Vec<String>,
    collection: HashMap<JobId, BenchmarkResult>,
}

impl MetricsCollector {
    pub fn new(
        parameters: BenchmarkParameters,
        prometheus_address: &SocketAddr,
        metrics: Vec<String>,
    ) -> MonitorResult<Self> {
        let prometheus_url = format!("http://{prometheus_address}");
        let client = PrometheusClient::try_from(prometheus_url)?;
        Ok(Self {
            parameters,
            client,
            metrics,
            collection: HashMap::new(),
        })
    }

    fn match_config(&self, id: &str) -> &Config {
        let str = id.split('-').last().expect("Valid job id is hyphenated");
        let index = str.parse::<usize>().expect("Job id contains the node idx");
        self.parameters
            .target_configs
            .get(index)
            .expect("Each job id should match a target config")
    }

    pub async fn collect(&mut self) -> TestbedResult<()> {
        let responses = try_join_all(
            self.metrics
                .iter()
                .map(|x| self.query_prometheus(x.clone())),
        )
        .await?;

        for (metric, response) in responses {
            for (id, buckets) in response {
                let settings = self.parameters.settings.clone();
                let config = self.match_config(&id).clone();
                self.collection
                    .entry(id.clone())
                    .or_insert_with(|| BenchmarkResult {
                        settings,
                        config,
                        collector: id.clone(),
                        results: HashMap::new(),
                    })
                    .results
                    .entry(metric.clone())
                    .or_insert_with(HashMap::new)
                    .extend(buckets);
            }
        }

        Ok(())
    }

    pub async fn query_prometheus(
        &self,
        metric: String,
    ) -> MonitorResult<(MetricLabel, HashMap<JobId, Buckets>)> {
        let mut collection = HashMap::new();

        let response = self.client.query(&metric).get().await?;
        let Data::Vector(vector) = response.data() else {
            panic!("Expected vector response, got: {response:#?}");
        };

        for element in vector {
            // Extract the job id from the sample data.
            let id = element
                .metric()
                .get("job")
                .expect("Expected 'job' label in metric");

            let sample = Sample {
                timestamp: element.sample().timestamp(),
                value: element.sample().value(),
            };

            // In case of a bucket metric, we expect the 'le' label to be present.
            if let Some(le) = element.metric().get("le") {
                collection
                    .entry(id.to_string())
                    .or_insert_with(HashMap::new)
                    .insert(le.to_string(), vec![sample]);
            }
        }

        Ok((metric, collection))
    }

    pub fn save_results<P: AsRef<Path>>(&self, path: P) {
        for result in self.collection.values() {
            result.save(path.as_ref());
        }
    }
}

#[cfg(test)]
mod test {}

const test_vector: &'static str = r#"
[
    InstantVector {
        metric: {
            "instance": "13.217.104.200:9092",
            "db": "tidehunter",
            "job": "instance-node-1",
            "le": "+Inf",
            "__name__": "bench_writes_bucket",
        },
        sample: Sample {
            timestamp: 1749134611.612,
            value: 19890.0,
        },
    },
    InstantVector {
        metric: {
            "db": "tidehunter",
            "__name__": "bench_writes_bucket",
            "le": "103.30523391999995",
            "job": "instance-node-1",
            "instance": "13.217.104.200:9092",
        },
        sample: Sample {
            timestamp: 1749134611.612,
            value: 0.0,
        },
    },
    InstantVector {
        metric: {
            "job": "instance-node-1",
            "db": "tidehunter",
            "__name__": "bench_writes_bucket",
            "instance": "13.217.104.200:9092",
            "le": "1088.9766689046844",
        },
        sample: Sample {
            timestamp: 1749134611.612,
            value: 19780.0,
        },
    },
    InstantVector {
        metric: {
            "le": "11479.28464434906",
            "db": "tidehunter",
            "__name__": "bench_writes_bucket",
            "job": "instance-node-1",
            "instance": "13.217.104.200:9092",
        },
        sample: Sample {
            timestamp: 1749134611.612,
            value: 19879.0,
        },
    },
    InstantVector {
        metric: {
            "job": "instance-node-1",
            "db": "tidehunter",
            "le": "13.719999999999997",
            "instance": "13.217.104.200:9092",
            "__name__": "bench_writes_bucket",
        },
        sample: Sample {
            timestamp: 1749134611.612,
            value: 0.0,
        },
    }]
    "#;
