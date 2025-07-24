pub mod mmap_buffer;

#[cfg_attr(any(target_os = "linux"), path = "mmap_linux.rs")]
#[cfg_attr(windows, path = "mmap_win.rs")]
#[cfg_attr(not(any(target_os = "linux", windows)), path = "mmap_fallback.rs")]
pub mod mmap;

#[cfg(test)]
mod tests;
