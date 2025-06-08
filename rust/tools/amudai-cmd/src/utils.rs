//! Common utilities for amudai-cmd

use anyhow::Result;
use std::path::Path;

/// Checks if a file exists and is readable
pub fn validate_file_exists(path: &str) -> Result<()> {
    let file_path = Path::new(path);
    if !file_path.exists() {
        anyhow::bail!("File does not exist: {}", path);
    }
    if !file_path.is_file() {
        anyhow::bail!("Path is not a file: {}", path);
    }
    Ok(())
}

/// Formats file size in human-readable format
pub fn format_size(size: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = size as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", size as u64, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}
