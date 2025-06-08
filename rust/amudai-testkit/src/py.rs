//! Python interpreter detection and command utilities.
//!
//! Functionality for detecting and configuring Python interpreters on
//! different operating systems. It ensures that a Python 3
//! interpreter is available and properly configured for executing Python
//! scripts from Rust code.

use std::process::Command;

/// Detects and returns a configured Python 3 command.
///
/// This function searches for a suitable Python 3 interpreter on the system
/// by trying various common command names and validating that they point to
/// Python 3.
///
/// The function tries the following candidates in order:
/// - On Windows: `py` (with `-3` flag), `python.exe`, `python`
/// - On Unix-like systems: `python3`, `python`
///
/// For each candidate, it verifies that the interpreter is Python 3 by
/// executing a version check script.
///
/// # Returns
///
/// Returns a configured `Command` object that can be used to execute Python
/// scripts. The command is already set up with the appropriate Python 3
/// interpreter and any necessary flags.
///
/// # Platform Notes
///
/// - **Windows**: Uses the `py` launcher with `-3` flag when available,
///   which is the recommended way to ensure Python 3 on Windows systems
/// - **Unix-like systems**: Prefers `python3` over `python` to avoid
///   potential conflicts with Python 2 installations
pub fn get_cmd() -> anyhow::Result<Command> {
    let candidates: &[&str] = if cfg!(windows) {
        &["py", "python.exe", "python"]
    } else {
        &["python3", "python"]
    };

    let version_check_script = "import sys; print(sys.version_info.major)";

    for cmd_name in candidates {
        let mut command = Command::new(cmd_name);
        let mut ret_cmd = Command::new(cmd_name);

        if cfg!(windows) && *cmd_name == "py" {
            command.arg("-3");
            ret_cmd.arg("-3");
        }

        command.arg("-c");
        command.arg(version_check_script);

        match command.output() {
            Ok(output) => {
                if output.status.success() {
                    let stdout_str = String::from_utf8_lossy(&output.stdout);
                    let version_major_str = stdout_str.trim();
                    if version_major_str == "3" {
                        return Ok(ret_cmd);
                    }
                }
            }
            Err(_) => (),
        }
    }

    Err(anyhow::anyhow!(
        "Could not find a Python 3 interpreter. Checked candidates: {:?}. \
        Please ensure Python 3 is installed and accessible via one of these names, \
        or that 'py -3' (on Windows) can find a Python 3 installation.",
        candidates
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_get_py_cmd() {
        let cmd = super::get_cmd().unwrap();
        println!("{:?}", cmd);
    }
}
