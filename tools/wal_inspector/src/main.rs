use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::KeyShape;

mod force_snapshot;
mod stat;
mod verify;

/// Context containing database configuration and metadata for inspector commands
pub struct InspectorContext {
    pub db_path: PathBuf,
    pub config: Config,
    pub key_shape: KeyShape,
    pub verbose: bool,
}

impl InspectorContext {
    /// Load database context from the given path
    pub fn load(db_path: PathBuf, verbose: bool) -> Result<Self> {
        // Load the database configuration, fallback to defaults if not found
        let config = Db::load_config(&db_path).unwrap_or_else(|e| {
            if verbose {
                println!("Could not load config file ({:?}), using defaults", e);
            }
            Config::default()
        });

        // Load the key shape from the database
        let key_shape = Db::load_key_shape(&db_path)
            .map_err(|e| anyhow::anyhow!("Failed to load key shape from database: {:?}", e))?;

        if verbose {
            println!("Loaded database context:");
            println!("  DB path: {:?}", db_path);
            println!("  Fragment size: {} MB", config.frag_size / (1024 * 1024));
            println!(
                "  WAL file size: {} GB",
                config.wal_file_size / (1024 * 1024 * 1024)
            );
            println!("  Key shape loaded successfully");
            println!();
        }

        Ok(Self {
            db_path,
            config,
            key_shape,
            verbose,
        })
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "wal_inspector",
    about = "Tool for inspecting and verifying TideHunter WAL files"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Verify that all keys in a database's WAL file are accessible from the database
    Verify {
        /// Path to the database directory containing the WAL file
        #[arg(short = 'd', long)]
        db_path: PathBuf,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },
    /// Analyze WAL file and display statistics about entry types and space usage
    Stat {
        /// Path to the database directory containing the WAL file
        #[arg(short = 'd', long)]
        db_path: PathBuf,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },
    /// Force a snapshot by rebuilding the control region with all dirty entries flushed
    ForceSnapshot {
        /// Path to the database directory
        #[arg(short = 'd', long)]
        db_path: PathBuf,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Verify { db_path, verbose } => {
            let context = InspectorContext::load(db_path, verbose)?;
            verify::verify_command(&context)
        }
        Commands::Stat { db_path, verbose } => {
            let context = InspectorContext::load(db_path, verbose)?;
            stat::stat_command(&context)
        }
        Commands::ForceSnapshot { db_path, verbose } => {
            let context = InspectorContext::load(db_path, verbose)?;
            force_snapshot::force_snapshot_command(&context)
        }
    }
}
