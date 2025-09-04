use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Verify { db_path, verbose } => verify::verify_command(db_path, verbose),
    }
}
