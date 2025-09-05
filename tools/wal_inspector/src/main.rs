use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod force_snapshot;
mod stat;
mod verify;

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
        Commands::Verify { db_path, verbose } => verify::verify_command(db_path, verbose),
        Commands::Stat { db_path, verbose } => stat::stat_command(db_path, verbose),
        Commands::ForceSnapshot { db_path, verbose } => {
            force_snapshot::force_snapshot_command(db_path, verbose)
        }
    }
}
