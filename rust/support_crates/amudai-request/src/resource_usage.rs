use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks resource usage metrics for a request or operation.
///
/// This struct captures various resource consumption metrics including CPU time,
/// I/O operations, and data transfer volumes. All fields use atomic operations to
/// support safe concurrent updates from multiple threads.
///
/// # Fields
///
/// * `cpu_user_ns` - CPU time spent in user mode (nanoseconds)
/// * `cpu_kernel_ns` - CPU time spent in kernel mode (nanoseconds)
/// * `read_iops` - Number of logical read I/O operations
/// * `write_iops` - Number of logical write I/O operations
/// * `read_bytes` - Total bytes read from storage
/// * `write_bytes` - Total bytes written to storage
#[derive(Debug, Default)]
pub struct ResourceUsage {
    /// CPU time spent in user mode (nanoseconds)
    pub cpu_user_ns: AtomicU64,
    /// CPU time spent in kernel mode (nanoseconds)
    pub cpu_kernel_ns: AtomicU64,

    /// Logical read I/O operations
    pub read_iops: AtomicU64,
    /// Logical write I/O operations
    pub write_iops: AtomicU64,

    /// Bytes read from storage
    pub read_bytes: AtomicU64,
    /// Bytes written to storage
    pub write_bytes: AtomicU64,
}

impl ResourceUsage {
    #[inline(always)]
    fn fetch_add(dst: &AtomicU64, src: &AtomicU64) {
        dst.fetch_add(src.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    #[inline(always)]
    fn fetch_sub(dst: &AtomicU64, src: &AtomicU64) -> AtomicU64 {
        AtomicU64::new(dst.fetch_sub(src.load(Ordering::Relaxed), Ordering::Relaxed))
    }

    /// Creates a new `ResourceUsage` instance with all metrics initialized to zero.
    ///
    /// # Returns
    ///
    /// A new `ResourceUsage` struct with all atomic counters set to 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Accumulates resource usage from another `ResourceUsage` into this one.
    ///
    /// This method atomically adds all metrics from `other` to the corresponding
    /// metrics in this instance. This is useful for aggregating resource usage
    /// across multiple operations or child tasks.
    ///
    /// # Arguments
    ///
    /// * `other` - The resource usage metrics to add to this instance
    pub fn add(&self, other: &ResourceUsage) {
        Self::fetch_add(&self.cpu_user_ns, &other.cpu_user_ns);
        Self::fetch_add(&self.cpu_kernel_ns, &other.cpu_kernel_ns);
        Self::fetch_add(&self.read_iops, &other.read_iops);
        Self::fetch_add(&self.write_iops, &other.write_iops);
        Self::fetch_add(&self.read_bytes, &other.read_bytes);
        Self::fetch_add(&self.write_bytes, &other.write_bytes);
    }

    /// Computes the difference in resource usage between this and a previous snapshot.
    ///
    /// This method calculates the delta by subtracting the metrics in `prev` from
    /// the current metrics. This is useful for determining the resource consumption
    /// for a specific time interval or operation window.
    ///
    /// # Arguments
    ///
    /// * `prev` - The previous resource usage snapshot to subtract from the current values
    ///
    /// # Returns
    ///
    /// A new `ResourceUsage` instance containing the differences (current - previous).
    pub fn diff(&self, prev: &ResourceUsage) -> ResourceUsage {
        ResourceUsage {
            cpu_user_ns: Self::fetch_sub(&self.cpu_user_ns, &prev.cpu_user_ns),
            cpu_kernel_ns: Self::fetch_sub(&self.cpu_kernel_ns, &prev.cpu_kernel_ns),
            read_iops: Self::fetch_sub(&self.read_iops, &prev.read_iops),
            write_iops: Self::fetch_sub(&self.write_iops, &prev.write_iops),
            read_bytes: Self::fetch_sub(&self.read_bytes, &prev.read_bytes),
            write_bytes: Self::fetch_sub(&self.write_bytes, &prev.write_bytes),
        }
    }
}

impl Clone for ResourceUsage {
    fn clone(&self) -> Self {
        ResourceUsage {
            cpu_user_ns: AtomicU64::new(self.cpu_user_ns.load(Ordering::Relaxed)),
            cpu_kernel_ns: AtomicU64::new(self.cpu_kernel_ns.load(Ordering::Relaxed)),
            read_iops: AtomicU64::new(self.read_iops.load(Ordering::Relaxed)),
            write_iops: AtomicU64::new(self.write_iops.load(Ordering::Relaxed)),
            read_bytes: AtomicU64::new(self.read_bytes.load(Ordering::Relaxed)),
            write_bytes: AtomicU64::new(self.write_bytes.load(Ordering::Relaxed)),
        }
    }
}
