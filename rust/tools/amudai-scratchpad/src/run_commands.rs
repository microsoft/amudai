use anyhow::Result;

use crate::pos_set_tests::{collect_positions, set_ops};

pub type RunFn = fn(&[Option<&str>]) -> Result<()>;

pub struct RunCommand {
    pub name: &'static str,
    pub about: &'static str,
    pub run: RunFn,
}

impl RunCommand {
    pub const fn new(name: &'static str, about: &'static str, run: RunFn) -> Self {
        Self { name, about, run }
    }
}

fn echo(args: &[Option<&str>]) -> Result<()> {
    for (i, a) in args.iter().enumerate() {
        match a {
            Some(s) => println!("{i}: {s}"),
            None => println!("{i}: <none>"),
        }
    }
    Ok(())
}

// To add a new run command:
// 1) Define a function with signature `fn(&[Option<&str>]) -> anyhow::Result<()>`
// 2) Add an entry below with its name, description, and function pointer
pub static RUN_COMMANDS: &[RunCommand] = &[
    RunCommand::new("echo", "Echo the provided args", echo),
    RunCommand::new("pos-set-ops", "Test position set building and ops", set_ops),
    RunCommand::new(
        "pos-set-collect",
        "Test collect positions",
        collect_positions,
    ),
];

pub fn all() -> &'static [RunCommand] {
    RUN_COMMANDS
}

pub fn get(name: &str) -> Option<&'static RunCommand> {
    RUN_COMMANDS
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
}
