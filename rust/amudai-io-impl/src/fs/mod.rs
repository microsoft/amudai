#[cfg_attr(any(unix, target_os = "redox", target_os = "wasi"), path = "unix.rs")]
#[cfg_attr(windows, path = "windows.rs")]
mod platform;

pub use platform::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Buffered,
    Unbuffered,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_open() {}
}
