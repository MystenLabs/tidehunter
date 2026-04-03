use anyhow::Result;
use benchmark::configs::{Backend, StressTestConfigs};
use std::fs;
use std::path::PathBuf;

const ONE_TB: usize = 1024 * 1024 * 1024 * 1024;

fn main() -> Result<()> {
    // Base config from Tidehunter defaults + benchmark defaults
    let mut base_item = StressTestConfigs::default();

    base_item.stress_client_parameters.mixed_threads = 36;
    base_item.stress_client_parameters.write_threads = 36;
    base_item.stress_client_parameters.write_size = 64;
    base_item.stress_client_parameters.key_len = 32;
    base_item.stress_client_parameters.writes = 83_000_000;
    base_item.stress_client_parameters.mixed_duration_secs = 600;
    base_item.stress_client_parameters.background_writes = 0;
    base_item.stress_client_parameters.no_snapshot = false;
    base_item.stress_client_parameters.report = true;
    base_item.stress_client_parameters.key_layout = benchmark::configs::KeyLayout::Uniform;
    base_item.stress_client_parameters.tldr = String::new();
    base_item.stress_client_parameters.preserve = false;
    base_item.stress_client_parameters.zipf_exponent = 0.0;
    base_item.stress_client_parameters.path = Some("/opt/sui/db/".to_string());
    base_item.db_parameters.max_maps = 128;
    base_item.db_parameters.max_dirty_keys = 1024;
    base_item.db_parameters.num_flusher_threads = 12;
    base_item.db_parameters.metrics_enabled = false;

    // Place all parameters we want to set/vary below, either as single values or in nested for loops
    base_item.db_parameters.direct_io = false;
    base_item.stress_client_parameters.relocation = None;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for backend in [Backend::Rocksdb, Backend::Blobdb] {
        for value_size in [128, 1024] {
            for zipf_exponent in [0.0, 2.0] {
                for read_percentage in [0, 50, 100] {
                    let mut item = base_item.clone();
                    item.stress_client_parameters.backend = backend.clone();
                    item.stress_client_parameters.write_size = value_size;
                    item.stress_client_parameters.zipf_exponent = zipf_exponent;
                    item.stress_client_parameters.read_percentage = read_percentage;

                    item.stress_client_parameters.writes = ONE_TB
                        / (item.stress_client_parameters.write_threads
                            * (item.stress_client_parameters.key_len
                                + item.stress_client_parameters.write_size));
                    let yaml = serde_yaml::to_string(&item)?;
                    println!("{yaml}");
                    items.push(item);
                }
            }
        }
    }

    // Write YAML list to orchestrator/assets/target_configs.yml
    let out_path = PathBuf::from("orchestrator/assets/target_configs.yml");
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let yaml_list = serde_yaml::to_string(&items)?;
    fs::write(&out_path, yaml_list)?;

    println!(
        "Generated {} configurations in: {}",
        items.len(),
        out_path.display()
    );

    Ok(())
}
