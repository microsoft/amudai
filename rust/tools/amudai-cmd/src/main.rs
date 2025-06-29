use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

mod commands;
mod schema_parser;
mod utils;

#[derive(ValueEnum, Clone, Debug)]
enum ShardFileOrganization {
    /// Single file organization
    Single,
    /// Two-level file organization (default)
    Twolevel,
}

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
    /// Ingest one or more source files into a shard. The shard will be stored in the specified shard path.
    Ingest {
        /// Path to the JSON file containing hierarchical table schema
        #[arg(long, conflicts_with = "schema")]
        schema_file: Option<String>,

        /// Schema string in format: (field1: type1, field2: type2, ...)
        /// Supported types: string, int/int32/i32, long/int64/i64, double/float64/f64, datetime
        #[arg(long, conflicts_with = "schema_file")]
        schema: Option<String>,

        /// Source file(s) to ingest (can be specified multiple times)
        #[arg(short, long, required = true)]
        file: Vec<String>,

        /// Shard file organization structure
        #[arg(long, value_enum, default_value = "twolevel")]
        shard_file_organization: ShardFileOrganization,

        /// Output shard URL or path
        shard_path: String,
    },

    /// Inspect a shard and display summary information
    Inspect {
        /// Increase verbosity (-v for verbose, -vv more verbose, -vvv for very verbose)
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

    /// Consume and read a shard to measure performance
    Consume {
        /// Number of records to read (if not specified, reads entire shard)
        #[arg(long)]
        count: Option<u64>,

        /// Number of iterations
        #[arg(long)]
        iterations: Option<u64>,

        /// Shard URL or path to consume
        shard_path: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ingest {
            schema_file,
            schema,
            file,
            shard_file_organization: shard_file_structure,
            shard_path,
        } => commands::ingest::run(schema_file, schema, file, shard_file_structure, shard_path),
        Commands::Inspect {
            verbose,
            shard_path,
        } => commands::inspect::run(verbose, shard_path),
        Commands::InferSchema { file, output } => commands::inferschema::run(file, output),
        Commands::Consume {
            count,
            iterations,
            shard_path,
        } => commands::consume::run(count, iterations, shard_path),
    }
}
