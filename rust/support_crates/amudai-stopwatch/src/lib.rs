#[cfg_attr(any(target_os = "linux"), path = "stopwatch_linux.rs")]
#[cfg_attr(windows, path = "stopwatch_win.rs")]
pub mod stopwatch;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
compile_error!("stopwatch is only implemented for Linux and Windows");
