use libc::{clock_gettime, timespec, CLOCK_THREAD_CPUTIME_ID};

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
///
/// # Note
///
/// On Linux, both user and kernel time are combined from the thread clock,
/// so `kernel_ns` represents the portion of time spent in system calls.
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
/// The stopwatch uses Linux `clock_gettime` with `CLOCK_THREAD_CPUTIME_ID` to retrieve
/// thread CPU times. On Linux, thread CPU time includes both user-mode and kernel-mode
/// time combined into a single clock.
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
    /// This unsafe function calls `clock_gettime` with `CLOCK_THREAD_CPUTIME_ID` to retrieve
    /// the current thread's CPU time counter. On Linux, this represents the total CPU time
    /// (user + kernel) spent by the thread. The time is returned in nanosecond precision.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it calls libc functions. However, it handles failures
    /// gracefully by returning a zero-initialized snapshot if the system call fails.
    ///
    /// # Returns
    ///
    /// A `ThreadTimesSnapshot` with the current thread's CPU time in nanoseconds.
    /// If the system call fails, returns a snapshot with both times set to 0.
    ///
    /// # Note
    ///
    /// On Linux, `kernel_ns` is typically zero since the thread clock provides combined
    /// user+kernel time. The `user_ns` field contains the total thread CPU time.
    pub unsafe fn take() -> ThreadTimesSnapshot {
        let mut snapshot = ThreadTimesSnapshot {
            user_ns: 0,
            kernel_ns: 0,
        };

        let mut ts: timespec = std::mem::zeroed();

        if unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
            return snapshot;
        }

        // Convert timespec to nanoseconds
        snapshot.user_ns = (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64);

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
