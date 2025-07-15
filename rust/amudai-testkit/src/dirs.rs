//! Directory and path utilities for test resources.
//!
//! Provides functions for locating test resources, sample files, and scripts
//! within the Amudai project structure. Handles path resolution relative
//! to the crate's source directory and validates that required directories
//! and files exist.

use std::path::PathBuf;

/// The source directory path for the `amudai-testkit`, set at compile time
/// (provided by the build script).
pub const TESTKIT_SRC_DIR_STR: &str = env!("TESTKIT_SRC_DIR");

/// Returns the path to the testkit source directory.
pub fn get_testkit_src_dir() -> anyhow::Result<PathBuf> {
    let res = PathBuf::from(TESTKIT_SRC_DIR_STR);
    if !res.is_dir() {
        anyhow::bail!("{} not found", res.display());
    }
    Ok(res)
}

/// Returns the path to the test samples directory (`$repo_root/test/samples`).
pub fn get_test_samples_dir() -> anyhow::Result<PathBuf> {
    let src_dir = get_testkit_src_dir()?;
    let samples_dir = src_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} parent", src_dir.display()))?
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} parent->parent", src_dir.display()))?
        .join("test")
        .join("samples");
    if !samples_dir.is_dir() {
        anyhow::bail!("{} not found", samples_dir.display());
    }
    Ok(samples_dir)
}

/// Returns the path to the qlog generator Python script
/// (`$repo_root/test/samples/qlog_generate_entries.py`).
pub fn get_qlog_generator_script_path() -> anyhow::Result<PathBuf> {
    let path = get_test_samples_dir()?.join("qlog_generate_entries.py");
    if !path.is_file() {
        anyhow::bail!("{} not found", path.display());
    }
    Ok(path)
}

/// Returns the path to the "qlog" record schema (json-serialized Arrow schema)
pub fn get_qlog_schema_path() -> anyhow::Result<PathBuf> {
    let path = get_test_samples_dir()?.join("qlog_schema.json");
    if !path.is_file() {
        anyhow::bail!("{} not found", path.display());
    }
    Ok(path)
}

/// Returns the path to the inventory generator Python script
/// (`$repo_root/test/samples/inventory_generate_csv.py`).
pub fn get_inventory_generator_script_path() -> anyhow::Result<PathBuf> {
    let path = get_test_samples_dir()?.join("inventory_generate_csv.py");
    if !path.is_file() {
        anyhow::bail!("{} not found", path.display());
    }
    Ok(path)
}

/// Returns the path to the "inventory" record schema (json-serialized Arrow schema)
pub fn get_inventory_schema_path() -> anyhow::Result<PathBuf> {
    let path = get_test_samples_dir()?.join("inventory_schema.json");
    if !path.is_file() {
        anyhow::bail!("{} not found", path.display());
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_dirs() {
        assert!(super::get_testkit_src_dir().is_ok());
        assert!(super::get_qlog_generator_script_path().is_ok());
        assert!(super::get_inventory_generator_script_path().is_ok());
    }
}
