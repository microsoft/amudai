use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;
mod utils;

#[derive(Parser)]
#[command(name = "amudai-cmd")]
#[command(about = "Command-line utility for Amudai format operations")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Ingest one or more source files into a shard
    Ingest {
        /// Path to the JSON file containing hierarchical table schema
        #[arg(long)]
        schema: Option<String>,

        /// Source file(s) to ingest (can be specified multiple times)
        #[arg(short, long, required = true)]
        file: Vec<String>,

        /// Output shard URL or path
        shard_path: String,
    },

    /// Inspect a shard and display summary information
    Inspect {
        /// Increase verbosity (-v for verbose, -vv for very verbose)
        #[arg(short, long, action = clap::ArgAction::Count)]
        verbose: u8,

        /// Shard URL or path to inspect
        shard_path: String,
    },

    /// Infer schema from source files
    InferSchema {
        /// Source file(s) to analyze for schema inference
        #[arg(short, long, required = true)]
        file: Vec<String>,

        /// Output file for the generated schema (defaults to stdout if not specified)
        #[arg(short, long)]
        output: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ingest {
            schema,
            file,
            shard_path,
        } => commands::ingest::run(schema, file, shard_path),
        Commands::Inspect {
            verbose,
            shard_path,
        } => commands::inspect::run(verbose, shard_path),
        Commands::InferSchema { file, output } => commands::inferschema::run(file, output),
    }
}
