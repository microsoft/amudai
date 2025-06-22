#[cfg_attr(
    any(unix, target_os = "redox", target_os = "wasi"),
    path = "platform_unix.rs"
)]
#[cfg_attr(windows, path = "platform_windows.rs")]
mod platform;
