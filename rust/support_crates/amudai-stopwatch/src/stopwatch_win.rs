use windows_sys::Win32::Foundation::FILETIME;
use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};

/// A snapshot of thread CPU time at a specific moment.
///
/// This struct captures the cumulative user-mode and kernel-mode CPU time
/// spent by the current thread. Times are measured in nanoseconds. Snapshots
/// can be compared to calculate elapsed CPU time intervals.
///
/// # Fields
///
/// * `user_ns` - Cumulative user-mode CPU time in nanoseconds
/// * `kernel_ns` - Cumulative kernel-mode CPU time in nanoseconds
#[derive(Default, Copy, Clone, Debug)]
pub struct ThreadTimesSnapshot {
    pub user_ns: u64,
    pub kernel_ns: u64,
}

/// A stopwatch for measuring thread CPU time (user and kernel modes).
///
/// This stopwatch tracks the cumulative CPU time (both user-mode and kernel-mode)
/// consumed by the current thread. It can be started, stopped, and queried for
/// elapsed time. This is useful for performance profiling and resource tracking.
///
/// # Example
///
/// ```ignore
/// use amudai_stopwatch::stopwatch::ThreadTimesStopwatch;
///
/// let mut stopwatch = ThreadTimesStopwatch::new();
/// stopwatch.start();
/// // ... do some work ...
/// stopwatch.stop();
/// let elapsed = stopwatch.elapsed();
/// println!("User time: {}ns, Kernel time: {}ns", elapsed.user_ns, elapsed.kernel_ns);
/// ```
///
/// # Note
///
/// The stopwatch uses Windows API calls to retrieve thread CPU times. Times are
/// measured in 100-nanosecond intervals and converted to nanoseconds.
#[derive(Default, Clone, Debug)]
pub struct ThreadTimesStopwatch {
    start: ThreadTimesSnapshot,
    elapsed: ThreadTimesSnapshot,
    running: bool,
}

impl ThreadTimesStopwatch {
    /// Creates a new stopwatch in the stopped state with zero elapsed time.
    ///
    /// # Returns
    ///
    /// A new `ThreadTimesStopwatch` instance ready to be started.
    pub fn new() -> ThreadTimesStopwatch {
        ThreadTimesStopwatch {
            start: ThreadTimesSnapshot::default(),
            elapsed: ThreadTimesSnapshot::default(),
            running: false,
        }
    }

    /// Starts the stopwatch.
    ///
    /// If the stopwatch is already running, this is a no-op. If it's stopped,
    /// this captures the current thread CPU time and resumes measurement.
    pub fn start(&mut self) {
        if !self.running {
            self.running = true;
            self.start = unsafe { ThreadTimesSnapshot::take() };
        }
    }

    /// Stops the stopwatch.
    ///
    /// If the stopwatch is running, this freezes the elapsed time at the current
    /// value. If it's already stopped, this is a no-op. The elapsed time accumulated
    /// so far can be retrieved via [`elapsed`](Self::elapsed).
    pub fn stop(&mut self) {
        if self.running {
            self.elapsed = self.elapsed();
            self.running = false;
        }
    }

    /// Returns the elapsed CPU time so far.
    ///
    /// If the stopwatch is currently running, this computes the elapsed time from
    /// the start point to the current moment. If stopped, returns the frozen elapsed
    /// time from when [`stop`](Self::stop) was called.
    ///
    /// # Returns
    ///
    /// A [`ThreadTimesSnapshot`] containing user-mode and kernel-mode CPU time in nanoseconds.
    pub fn elapsed(&self) -> ThreadTimesSnapshot {
        let mut elapsed = self.elapsed;
        if self.running {
            elapsed.add(&unsafe { ThreadTimesSnapshot::take().diff(&self.start) });
        }
        elapsed
    }

    /// Manually adds CPU time to the elapsed counter.
    ///
    /// This is useful for accumulating time from external measurements or
    /// combining time from multiple sources.
    ///
    /// # Arguments
    ///
    /// * `user` - User-mode CPU time to add (nanoseconds)
    /// * `kernel` - Kernel-mode CPU time to add (nanoseconds)
    pub fn accumulate(&mut self, user: u64, kernel: u64) {
        self.elapsed.user_ns += user;
        self.elapsed.kernel_ns += kernel;
    }

    /// Adds the elapsed time from another stopwatch to this one.
    ///
    /// The other stopwatch must be stopped before calling this method.
    ///
    /// # Arguments
    ///
    /// * `other` - A stopped `ThreadTimesStopwatch` whose elapsed time will be added
    ///
    /// # Panics
    ///
    /// Panics if `other` is currently running.
    pub fn add(&mut self, other: &ThreadTimesStopwatch) {
        assert!(!other.running);
        self.elapsed.add(&other.elapsed);
    }
}

impl ThreadTimesSnapshot {
    /// Captures the current thread's CPU time.
    ///
    /// This unsafe function calls the Windows API to retrieve the current thread's
    /// user-mode and kernel-mode CPU time counters. Times are obtained from the system
    /// and are measured in 100-nanosecond intervals, then converted to nanoseconds.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it calls Windows API functions that may have
    /// platform-specific requirements and could fail. However, it handles failures
    /// gracefully by returning a zero-initialized snapshot.
    ///
    /// # Returns
    ///
    /// A `ThreadTimesSnapshot` with the current thread's CPU times in nanoseconds.
    /// If the system call fails, returns a snapshot with both times set to 0.
    pub unsafe fn take() -> ThreadTimesSnapshot {
        let mut snapshot = ThreadTimesSnapshot {
            user_ns: 0,
            kernel_ns: 0,
        };

        let thread = unsafe { GetCurrentThread() };

        let mut creation_time: FILETIME = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut exit_time: FILETIME = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut user_ticks_ft: FILETIME = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut kernel_ticks_ft: FILETIME = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };

        if unsafe {
            GetThreadTimes(
                thread,
                &mut creation_time,
                &mut exit_time,
                &mut kernel_ticks_ft,
                &mut user_ticks_ft,
            )
        } == 0
        {
            return snapshot;
        }

        fn filetime_to_u64(ft: &FILETIME) -> u64 {
            (ft.dwLowDateTime as u64) | ((ft.dwHighDateTime as u64) << 32)
        }

        snapshot.kernel_ns = filetime_to_u64(&kernel_ticks_ft) * 100;
        snapshot.user_ns = filetime_to_u64(&user_ticks_ft) * 100;

        snapshot
    }

    /// Computes the difference between this and another snapshot.
    ///
    /// Returns a new `ThreadTimesSnapshot` where each field is the difference
    /// between the corresponding fields in `self` and `other`. If the result would be
    /// negative (i.e., `other` is greater than `self`), the field is clamped to 0.
    ///
    /// # Arguments
    ///
    /// * `other` - The snapshot to subtract from this one
    ///
    /// # Returns
    ///
    /// A new `ThreadTimesSnapshot` with `self - other`, clamped to non-negative values.
    pub fn diff(&self, other: &ThreadTimesSnapshot) -> ThreadTimesSnapshot {
        ThreadTimesSnapshot {
            user_ns: if self.user_ns > other.user_ns {
                self.user_ns - other.user_ns
            } else {
                0
            },
            kernel_ns: if self.kernel_ns > other.kernel_ns {
                self.kernel_ns - other.kernel_ns
            } else {
                0
            },
        }
    }

    /// Adds the CPU times from another snapshot to this one.
    ///
    /// This is useful for accumulating CPU time measurements from multiple sources
    /// or combining snapshots.
    ///
    /// # Arguments
    ///
    /// * `other` - The snapshot whose times will be added to this one
    pub fn add(&mut self, other: &ThreadTimesSnapshot) {
        self.kernel_ns += other.kernel_ns;
        self.user_ns += other.user_ns;
    }
}
