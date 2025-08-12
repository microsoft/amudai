use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod pos_set_tests;
pub mod run_commands;
pub mod utils;

#[derive(Parser)]
#[command(name = "amudai-scratchpad")]
#[command(about = "A place for quick trials and perf experiments.")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the test case
    Run {
        /// Test case name
        name: String,

        /// Args
        args: Vec<String>,
    },

    /// List the test cases
    List {},
}

fn main() -> Result<()> {
    // amudai_workflow::eager_pool::EagerPool::configure_global_pool_size(32);

    let cli = Cli::parse();

    match cli.command {
        Commands::Run { name, args } => {
            println!("running {name} with {args:?}");
            let arg_refs: Vec<Option<&str>> = args.iter().map(|s| Some(s.as_str())).collect();
            if let Some(cmd) = run_commands::get(&name) {
                (cmd.run)(&arg_refs)?;
            } else {
                eprintln!("Unknown run command: {name}");
                eprintln!("Use `list` to see available commands.");
            }
        }
        Commands::List {} => {
            println!("Available run commands:");
            for c in run_commands::all() {
                println!("- {}: {}", c.name, c.about);
            }
        }
    }
    Ok(())
}
