//! Command implementations for amudai-cmd

use amudai_objectstore::url::ObjectUrl;
use anyhow::{Context, Result};
use std::env;
use std::path::Path;
use url::Url;

pub mod consume;
pub mod inferschema;
pub mod ingest;
pub mod inspect;

/// Converts a file path string to an `ObjectUrl`.
///
/// If the input string is already a URL, it validates and returns it.
/// If the input is a file path (absolute or relative), it converts it to a file:// URL.
pub fn file_path_to_object_url(path_or_url: &str) -> Result<ObjectUrl> {
    if let Ok(object_url) = ObjectUrl::parse(path_or_url) {
        return Ok(object_url);
    }

    let path = Path::new(path_or_url);
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .with_context(|| "Failed to get current directory")?
            .join(path)
    };

    let file_url = Url::from_file_path(&absolute_path).map_err(|()| {
        anyhow::anyhow!("Failed to convert path to URL: {}", absolute_path.display())
    })?;

    ObjectUrl::new(file_url)
        .with_context(|| format!("Invalid file URL for path: {}", absolute_path.display()))
}
