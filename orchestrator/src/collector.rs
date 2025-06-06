// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::fs;
use std::path::{Path, PathBuf};

use futures::future::try_join_all;
use prometheus_http_query::response::Data;
use prometheus_http_query::Client as PrometheusClient;
use serde::Serialize;

use crate::benchmark::BenchmarkParameters;
use crate::error::{MonitorError, MonitorResult};
use crate::settings::Settings;
use crate::Config;

/// A unique identifier for a job.
#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
struct JobId(String);

impl Debug for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl JobId {
    /// Extract the index from the job id.
    fn index(&self) -> usize {
        // The job id is expected to be in the format "instance-node-<index>".
        self.0
            .split('-')
            .last()
            .expect("Valid job id is hyphenated")
            .parse::<usize>()
            .expect("Job id contains the node index")
    }
}

/// A label for a metric, which is a string that identifies the metric in Prometheus.
#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
pub struct MetricLabel(pub String);

impl Display for MetricLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A label for a bucket in a histogram metric, which is a string that identifies the bucket.
#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
struct Le(String);

#[cfg(test)]
impl From<&str> for Le {
    fn from(s: &str) -> Self {
        Le(s.to_string())
    }
}

/// A collection of buckets for a metric, where each bucket is keyed by a label.
#[derive(Serialize)]
struct Buckets(HashMap<Le, Vec<Sample>>);

impl Buckets {
    /// Create a new empty collection of buckets.
    pub fn new() -> Self {
        Buckets(HashMap::new())
    }

    /// Insert a sample into the collection of buckets.
    pub fn insert(&mut self, le: Le, sample: Sample) {
        self.0.entry(le).or_default().push(sample);
    }

    /// Extend the collection with another collection of buckets.
    pub fn extend(&mut self, other: Buckets) {
        for (le, samples) in other.0 {
            self.0.entry(le).or_default().extend(samples);
        }
    }

    /// Get the samples for a specific label.
    #[cfg(test)]
    fn get(&self, le: &Le) -> Option<&Vec<Sample>> {
        self.0.get(le)
    }
}

/// A sample of a metric, containing a timestamp and a value.
#[derive(Clone, Serialize)]
struct Sample {
    /// The timestamp of the sample.
    timestamp: f64,
    /// The value of the sample.
    value: f64,
}

/// The result of a benchmark run.
#[derive(Serialize)]
pub struct BenchmarkResult {
    /// Unique identifier for the benchmark run.
    uid: String,
    /// The settings used for the benchmark.
    settings: Settings,
    /// The configuration used for the benchmark.
    config: Config,
    /// The results of the benchmark, keyed by metric label.
    results: HashMap<MetricLabel, Buckets>,
}

impl BenchmarkResult {
    /// Save the collection of measurements as a json file.
    pub fn save<P: AsRef<Path>>(&self, path: P) {
        let json = serde_json::to_string_pretty(self).expect("Cannot serialize metrics");
        let mut file = PathBuf::from(path.as_ref());
        file.push(format!("measurements-{}.json", self.uid));
        fs::write(file, json).unwrap();
    }
}

/// A collector for metrics from Prometheus. It collects metrics for a set of jobs and stores them
/// in a collection. The metrics are identified by their labels and the job ids.
pub struct MetricsCollector {
    /// The benchmark parameters for the run.
    parameters: BenchmarkParameters,
    /// The unique identifier for this collector instance.
    collector_id: usize,
    /// The Prometheus client used to query the metrics.
    client: PrometheusClient,
    /// The metrics to collect from Prometheus.
    metrics: Vec<MetricLabel>,
    /// The collection of metrics, keyed by job id.
    collection: HashMap<JobId, BenchmarkResult>,
}

impl MetricsCollector {
    /// Create a new metrics collector with the given parameters, Prometheus address, and metrics to collect.
    pub fn new(
        parameters: BenchmarkParameters,
        collector_id: usize,
        prometheus_address: &str,
        metrics: Vec<MetricLabel>,
    ) -> MonitorResult<Self> {
        let client = PrometheusClient::try_from(prometheus_address)?;
        Ok(Self {
            parameters,
            collector_id,
            client,
            metrics,
            collection: HashMap::new(),
        })
    }

    /// Collect the metrics from Prometheus for the specified job ids.
    pub async fn collect(&mut self) -> MonitorResult<()> {
        let responses = try_join_all(
            self.metrics
                .iter()
                .map(|metric| self.client.query(&metric).get()),
        )
        .await?;

        let metrics = self.metrics.clone().into_iter();
        for (response, metric) in responses.iter().zip(metrics) {
            let response_data = response.data().clone();
            let collection = Self::parse_response(response_data)?;
            self.update_collector(metric, collection);
        }

        Ok(())
    }

    /// Parse the response from Prometheus into a collection of job ids and their corresponding buckets.
    fn parse_response(response: Data) -> MonitorResult<HashMap<JobId, Buckets>> {
        let Data::Vector(vector) = response else {
            return Err(MonitorError::UnexpectedPrometheusResponse(response));
        };

        let mut collection = HashMap::new();
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
                    .entry(JobId(id.to_string()))
                    .or_insert_with(Buckets::new)
                    .insert(Le(le.to_string()), sample);
            }
        }

        Ok(collection)
    }

    /// Update the collector with the new metric data.
    fn update_collector(&mut self, metric: MetricLabel, collection: HashMap<JobId, Buckets>) {
        for (job_id, buckets) in collection {
            let settings = self.parameters.settings.clone();
            let config = self.match_config(&job_id).clone();
            self.collection
                .entry(job_id.clone())
                .or_insert_with(|| BenchmarkResult {
                    uid: format!("{}-{:?}", self.collector_id, &job_id),
                    settings,
                    config,
                    results: HashMap::new(),
                })
                .results
                .entry(metric.clone())
                .or_insert_with(Buckets::new)
                .extend(buckets);
        }
    }

    /// Match the job id with the corresponding target configuration.
    fn match_config(&self, id: &JobId) -> &Config {
        self.parameters
            .target_configs
            .get(id.index())
            .expect("Each job id should match a target config")
    }

    /// Save the collected results to the specified path.
    pub fn save_results<P: AsRef<Path>>(&self, path: P) {
        for result in self.collection.values() {
            result.save(path.as_ref());
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use prometheus_http_query::response::{Data, InstantVector};
    use prometheus_http_query::Client as PrometheusClient;

    use crate::benchmark::BenchmarkParameters;
    use crate::collector::{JobId, MetricLabel, MetricsCollector};

    /// Test data for the metrics collector.
    const TEST_VECTOR: &'static str = r#"
[
  {
    "metric": {
      "instance": "13.217.104.200:9092",
      "db": "tidehunter",
      "job": "instance-node-1",
      "le": "+Inf",
      "__name__": "bench_writes_bucket"
    },
    "sample": {
      "timestamp": 1749134611.612,
      "value": 19890
    }
  },
  {
    "metric": {
      "db": "tidehunter",
      "__name__": "bench_writes_bucket",
      "le": "103.30523391999995",
      "job": "instance-node-1",
      "instance": "13.217.104.200:9092"
    },
    "sample": {
      "timestamp": 1749134611.612,
      "value": 0
    }
  },
  {
    "metric": {
      "job": "instance-node-1",
      "db": "tidehunter",
      "__name__": "bench_writes_bucket",
      "instance": "13.217.104.200:9092",
      "le": "1088.9766689046844"
    },
    "sample": {
      "timestamp": 1749134611.612,
      "value": 19780
    }
  },
  {
    "metric": {
      "le": "11479.28464434906",
      "db": "tidehunter",
      "__name__": "bench_writes_bucket",
      "job": "instance-node-1",
      "instance": "13.217.104.200:9092"
    },
    "sample": {
      "timestamp": 1749134611.612,
      "value": 19879
    }
  },
  {
    "metric": {
      "job": "instance-node-0",
      "db": "tidehunter",
      "le": "13.719999999999997",
      "instance": "13.217.104.200:9092",
      "__name__": "bench_writes_bucket"
    },
    "sample": {
      "timestamp": 1749134611.612,
      "value": 0
    }
  }
]
    "#;

    #[test]
    fn parse_response() {
        let vector: Vec<InstantVector> = serde_json::from_str(TEST_VECTOR).unwrap();
        let data = Data::Vector(vector);
        let result = MetricsCollector::parse_response(data).unwrap();

        assert!(!result.is_empty());

        // Check results for instance-node-0
        let job_0 = JobId("instance-node-0".to_string());
        assert!(result.contains_key(&job_0));
        let buckets_0 = result.get(&job_0).unwrap();
        let bucket_0_1 = buckets_0.get(&"13.719999999999997".into()).unwrap();
        assert_eq!(bucket_0_1.len(), 1);
        assert_eq!(bucket_0_1[0].value, 0.0);
        assert_eq!(bucket_0_1[0].timestamp, 1749134611.612);

        // Check results for instance-node-1
        let job_1 = JobId("instance-node-1".to_string());
        assert!(result.contains_key(&job_1));
        let buckets_1 = result.get(&job_1).unwrap();
        let bucket_1_0 = buckets_1.get(&"+Inf".into()).unwrap();
        assert_eq!(bucket_1_0.len(), 1);
        assert_eq!(bucket_1_0[0].value, 19890.0);
        assert_eq!(bucket_1_0[0].timestamp, 1749134611.612);

        let bucket_1_1 = buckets_1.get(&"103.30523391999995".into()).unwrap();
        assert_eq!(bucket_1_1.len(), 1);
        assert_eq!(bucket_1_1[0].value, 0.0);
        assert_eq!(bucket_1_1[0].timestamp, 1749134611.612);

        let bucket_1_2 = buckets_1.get(&"1088.9766689046844".into()).unwrap();
        assert_eq!(bucket_1_2.len(), 1);
        assert_eq!(bucket_1_2[0].value, 19780.0);
        assert_eq!(bucket_1_2[0].timestamp, 1749134611.612);

        let bucket_1_3 = buckets_1.get(&"11479.28464434906".into()).unwrap();
        assert_eq!(bucket_1_3.len(), 1);
        assert_eq!(bucket_1_3[0].value, 19879.0);
        assert_eq!(bucket_1_3[0].timestamp, 1749134611.612);
    }

    #[test]
    fn update_collector() {
        let metric_label = MetricLabel("bench_writes_bucket".to_string());
        let mut collector = MetricsCollector {
            parameters: BenchmarkParameters::new_for_test(),
            collector_id: 1,
            client: PrometheusClient::default(),
            metrics: vec![metric_label.clone()],
            collection: HashMap::new(),
        };

        let vector: Vec<InstantVector> = serde_json::from_str(TEST_VECTOR).unwrap();
        let data = Data::Vector(vector);
        let response_data = MetricsCollector::parse_response(data).unwrap();
        collector.update_collector(metric_label.clone(), response_data);

        assert_eq!(collector.collection.len(), 2);

        // Check the results for instance-node-0
        let job_0 = JobId("instance-node-0".to_string());
        assert!(collector.collection.contains_key(&job_0));
        let result_0 = collector
            .collection
            .get(&job_0)
            .unwrap()
            .results
            .get(&metric_label)
            .unwrap();

        let buckets_0 = result_0.get(&"13.719999999999997".into()).unwrap();
        assert_eq!(buckets_0.len(), 1);
        assert_eq!(buckets_0[0].value, 0.0);
        assert_eq!(buckets_0[0].timestamp, 1749134611.612);

        // Check the results for instance-node-1
        let job_1 = JobId("instance-node-1".to_string());
        assert!(collector.collection.contains_key(&job_1));
        let result_1 = collector
            .collection
            .get(&job_1)
            .unwrap()
            .results
            .get(&metric_label)
            .unwrap();
        let buckets_1 = result_1.get(&"+Inf".into()).unwrap();
        assert_eq!(buckets_1.len(), 1);
        assert_eq!(buckets_1[0].value, 19890.0);
        assert_eq!(buckets_1[0].timestamp, 1749134611.612);

        let bucket_1_1 = result_1.get(&"103.30523391999995".into()).unwrap();
        assert_eq!(bucket_1_1.len(), 1);
        assert_eq!(bucket_1_1[0].value, 0.0);
        assert_eq!(bucket_1_1[0].timestamp, 1749134611.612);

        let bucket_1_2 = result_1.get(&"1088.9766689046844".into()).unwrap();
        assert_eq!(bucket_1_2.len(), 1);
        assert_eq!(bucket_1_2[0].value, 19780.0);
        assert_eq!(bucket_1_2[0].timestamp, 1749134611.612);

        let bucket_1_3 = result_1.get(&"11479.28464434906".into()).unwrap();
        assert_eq!(bucket_1_3.len(), 1);
        assert_eq!(bucket_1_3[0].value, 19879.0);
        assert_eq!(bucket_1_3[0].timestamp, 1749134611.612);
    }
}
