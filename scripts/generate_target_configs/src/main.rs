use anyhow::Result;
use benchmark::configs::StressClientParameters;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize)]
struct TargetConfigOutput {
    db_parameters: tidehunter::config::Config,
    stress_client_parameters: StressClientParameters,
}

fn main() -> Result<()> {
    // Base config from Tidehunter defaults + benchmark defaults
    let base_db: tidehunter::config::Config = tidehunter::config::Config {
        ..Default::default()
    };
    let base_client: StressClientParameters = StressClientParameters {
        mixed_threads: 36,
        write_threads: 36,
        write_size: 64,
        key_len: 32,
        writes: 50_000_000,
        operations: 10_000_000,
        background_writes: 0,
        no_snapshot: false,
        report: true,
        key_layout: benchmark::configs::KeyLayout::Uniform,
        tldr: String::new(),
        preserve: false,
        read_percentage: 100,
        zipf_exponent: 0.0,
        path: Some("/home/ubuntu/working_dir".to_string()),
        ..Default::default()
    };

    // Serialize base to YAML string once, then produce combinations via string replacements.
    let base_item = TargetConfigOutput {
        db_parameters: base_db.clone(),
        stress_client_parameters: base_client.clone(),
    };
    let base_yaml = serde_yaml::to_string(&base_item)?;

    // Parameter combinations using YAML paths. Add new params here only.
    // Keys must be of the form "section.key" where section is one of the top-level maps.
    let parameter_combinations: Vec<(&str, Vec<&str>)> = vec![
        (
            "stress_client_parameters.backend",
            vec!["Tidehunter", "Rocksdb"],
        ),
        ("db_parameters.direct_io", vec!["false"]),
        ("stress_client_parameters.read_percentage", vec!["100"]),
        ("stress_client_parameters.zipf_exponent", vec!["2"]),
        (
            "stress_client_parameters.read_mode",
            vec!["Get", "Exists", "!Lt 1"],
        ),
        ("stress_client_parameters.path", vec!["/opt/sui/db/"]),
    ];

    println!("Generating configurations with the following parameter combinations:");
    for (param, values) in &parameter_combinations {
        println!("  {}: {:?}", param, values);
    }

    let mut total_configs: usize = 1;
    for (_, values) in &parameter_combinations {
        total_configs *= values.len();
    }
    println!("\nTotal configurations to generate: {}", total_configs);

    // Compute cartesian product over arbitrary number of parameter lists.
    let mut combos: Vec<Vec<(&str, &str)>> = vec![Vec::new()];
    for (path, values) in &parameter_combinations {
        let mut next: Vec<Vec<(&str, &str)>> = Vec::with_capacity(combos.len() * values.len());
        for combo in combos.into_iter() {
            for v in values {
                let mut c = combo.clone();
                c.push((*path, *v));
                next.push(c);
            }
        }
        combos = next;
    }

    // Build final YAML as a list
    let mut file_contents = String::new();
    for assignment in &combos {
        let mut item_yaml = base_yaml.clone();
        for (path, value) in assignment {
            let (section, key) = path
                .split_once('.')
                .expect("parameter path must be of form 'section.key'");
            let rendered = format_yaml_value(value);
            item_yaml = replace_in_section(&item_yaml, section, key, &rendered);
        }
        file_contents.push_str(&emit_list_item(&item_yaml));
    }

    // Write YAML list to orchestrator/assets/target_configs.yml
    let out_path = PathBuf::from("orchestrator/assets/target_configs.yml");
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&out_path, file_contents)?;

    println!(
        "Generated {} configurations in: {}",
        combos.len(),
        out_path.display()
    );

    // Preview first 2 assignments
    println!("\nPreview of first 2 configurations:");
    for (idx, assignment) in combos.iter().take(2).enumerate() {
        println!("\nConfiguration {}:", idx + 1);
        for (path, value) in assignment {
            println!("  {}: {}", path, value);
        }
    }

    Ok(())
}

// --- Helpers for YAML string replacements ---

fn format_yaml_value(v: &str) -> String {
    if v.starts_with('!') {
        // YAML tag, emit as-is (e.g., !Lt 1)
        v.to_string()
    } else if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("false") {
        v.to_ascii_lowercase()
    } else if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        v.to_string()
    } else if v.is_empty() || v.contains(' ') || v.starts_with('/') {
        format!("\"{}\"", v.replace('"', "\\\""))
    } else {
        v.to_string()
    }
}

fn replace_in_section(yaml: &str, section: &str, key: &str, new_value: &str) -> String {
    let mut out = String::with_capacity(yaml.len());
    let mut in_section = false;
    let mut section_indent: usize = 0;
    for line in yaml.lines() {
        let indent = line.chars().take_while(|c| *c == ' ').count();
        let trimmed = line.trim_start();

        if trimmed == format!("{}:", section) {
            in_section = true;
            section_indent = indent;
            out.push_str(line);
            out.push('\n');
            continue;
        }

        if in_section {
            let leaving_section =
                indent <= section_indent || (trimmed.ends_with(':') && indent == section_indent);
            if leaving_section {
                in_section = false;
                // fall-through to normal write below
            } else {
                let key_prefix = format!("{}:", key);
                if trimmed.starts_with(&key_prefix) {
                    let new_line = format!("{}{}: {}", " ".repeat(indent), key, new_value);
                    out.push_str(&new_line);
                    out.push('\n');
                    continue;
                }
            }
        }

        out.push_str(line);
        out.push('\n');
    }
    out
}

fn emit_list_item(item_yaml: &str) -> String {
    let mut out = String::new();
    let mut lines = item_yaml.lines();
    if let Some(first) = lines.next() {
        out.push_str("- ");
        out.push_str(first.trim_start());
        out.push('\n');
        for l in lines {
            out.push_str("  ");
            out.push_str(l);
            out.push('\n');
        }
    }
    out
}
