//! Request tracking and resource usage monitoring.
//!
//! This module provides a request context system for tracking resource consumption
//! across operations. It supports hierarchical request structures with parent-child
//! relationships, allowing resource usage to be folded from child requests back
//! to their parents.
//!
//! # Overview
//!
//! - [`RequestContext`] - The main request tracking object that can be set as active
//!   for a thread and used to monitor resource usage.
//! - [`RequestGuard`] - An RAII guard that automatically clears the active request
//!   when dropped and folds resource usage to the parent.
//!
//! # Example
//!
//! ```ignore
//! use amudai_request::RequestContext;
//! use amudai_guid::Guid;
//!
//! let request = RequestContext::new(Guid::new_v4());
//! let _guard = RequestContext::set_scoped(request.clone());
//!
//! // Report I/O operations
//! RequestContext::report_read(1024);
//! RequestContext::report_write(512);
//!
//! // Spawn child request
//! let child = request.spawn(Guid::new_v4());
//! ```

pub mod resource_usage;

use amudai_guid::Guid;
use std::sync::Arc;

use crate::resource_usage::ResourceUsage;
use amudai_stopwatch::stopwatch::ThreadTimesStopwatch;

thread_local! {
    static CURRENT_REQUEST: std::cell::RefCell<Option<RequestContext>> = std::cell::RefCell::new(None);
}

struct RequestContextCore {
    pub id: Guid,
    pub resource_usage: ResourceUsage,
    pub parent: Option<RequestContext>,
    stopwatch: ThreadTimesStopwatch,
}

/// A request context for tracking resource usage and operations.
///
/// `RequestContext` provides a way to monitor and aggregate resource consumption
/// across a logical request or operation. It tracks CPU time, I/O operations, and
/// data transfer volumes. Requests can be nested, with child requests automatically
/// aggregating their resource usage back to their parent when they complete.
///
/// # Thread-Local Storage
///
/// Only one `RequestContext` can be active per thread at a time. Use [`set_scoped`](Self::set_scoped)
/// to set the active request, which returns a [`RequestGuard`] that automatically clears
/// it when dropped.
///
/// # Resource Tracking
///
/// Resource usage is tracked through:
/// - CPU time (user and kernel mode)
/// - I/O operations (reads and writes)
/// - Data transfer (bytes read and written)
///
/// Use [`report_read`](Self::report_read) and [`report_write`](Self::report_write) to manually
/// report I/O operations, or access the [`resource_usage`](Self::resource_usage) for detailed metrics.
#[derive(Clone)]
pub struct RequestContext(Arc<RequestContextCore>);

// Instance operations
impl RequestContext {
    /// Creates a new `RequestContext` with the given ID.
    ///
    /// The request starts a CPU time stopwatch immediately upon creation.
    /// The stopwatch is stopped when the request is dropped or when it's
    /// removed from thread-local storage.
    pub fn new(id: Guid) -> Self {
        let mut stopwatch = ThreadTimesStopwatch::new();
        stopwatch.start();
        Self(Arc::new(RequestContextCore {
            id,
            resource_usage: ResourceUsage::new(),
            parent: None,
            stopwatch,
        }))
    }

    /// Returns the ID of this request.
    pub fn id(&self) -> Guid {
        self.0.id
    }

    /// Returns a reference to the resource usage statistics for this request.
    ///
    /// The returned reference provides access to all accumulated resource metrics
    /// including CPU time, I/O operations, and data transfer statistics.
    ///
    /// # Returns
    ///
    /// A reference to the [`ResourceUsage`] struct containing current metrics.
    pub fn resource_usage(&self) -> &ResourceUsage {
        &self.0.resource_usage
    }

    /// Creates a child request with the given ID.
    ///
    /// The child request starts its own CPU time stopwatch and will accumulate
    /// its resource usage to the parent when dropped. This is useful for tracking
    /// resource consumption of specific operations or subtasks within a larger request.
    ///
    /// # Arguments
    ///
    /// * `id` - A unique identifier for the child request
    ///
    /// # Returns
    ///
    /// A new `RequestContext` with this request as its parent.
    pub fn spawn(&self, id: Guid) -> Self {
        let mut stopwatch = ThreadTimesStopwatch::new();
        stopwatch.start();
        Self(Arc::new(RequestContextCore {
            id,
            resource_usage: ResourceUsage::new(),
            parent: Some(self.clone()),
            stopwatch,
        }))
    }
}

// Static/thread-local operations
impl RequestContext {
    /// Sets the given `RequestContext` as the active request for this thread.
    ///
    /// Only one request can be active per thread at a time. When a new request is set,
    /// any previously active request is replaced. The returned [`RequestGuard`] will
    /// automatically clear the active request when dropped, triggering resource aggregation
    /// to the parent if this is a child request.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to set as active
    ///
    /// # Returns
    ///
    /// A [`RequestGuard`] that will clear the active request upon drop.
    pub fn set_scoped(request: RequestContext) -> RequestGuard {
        CURRENT_REQUEST.with(|req| {
            let mut borrowed = req.borrow_mut();
            *borrowed = Some(request);
        });
        RequestGuard
    }

    /// Returns the currently active `RequestContext` on this thread, if any.
    ///
    /// This retrieves the request stored in thread-local storage, if present.
    /// Returns `None` if no request has been set as active or if the previous
    /// active request's guard has been dropped.
    ///
    /// # Returns
    ///
    /// `Some(RequestContext)` if a request is currently active, `None` otherwise.
    pub fn active() -> Option<RequestContext> {
        CURRENT_REQUEST.with(|req| req.borrow().clone())
    }

    /// Executes a closure with mutable access to the currently active `RequestContext`.
    ///
    /// This is a convenience method for operations that need mutable access to the
    /// active request without taking ownership. The closure receives a mutable reference
    /// to the active request context if one exists.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that accepts a mutable reference to `RequestContext` and returns
    ///   a value of type `R`
    ///
    /// # Type Parameters
    ///
    /// * `F` - The closure type
    /// * `R` - The return type of the closure
    ///
    /// # Returns
    ///
    /// `Some(R)` containing the result of the closure if a request is active, `None` otherwise.
    pub fn with<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut RequestContext) -> R,
    {
        CURRENT_REQUEST.with(|req| {
            let mut borrowed = req.borrow_mut();
            borrowed.as_mut().map(f)
        })
    }

    /// Reports a read I/O operation to the active request's resource usage tracker.
    ///
    /// This method should be called whenever a read I/O operation completes to track
    /// resource usage. If there is no active request, this is a no-op and does nothing.
    ///
    /// The method atomically updates:
    /// - The total bytes read counter
    /// - The read I/O operation counter (incremented by 1)
    ///
    /// # Arguments
    ///
    /// * `bytes` - The number of bytes read in this operation
    pub fn report_read(bytes: u64) {
        Self::with(|req| {
            use std::sync::atomic::Ordering;
            req.0
                .resource_usage
                .read_bytes
                .fetch_add(bytes, Ordering::Relaxed);
            req.0
                .resource_usage
                .read_iops
                .fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Reports a write I/O operation to the active request's resource usage tracker.
    ///
    /// This method should be called whenever a write I/O operation completes to track
    /// resource usage. If there is no active request, this is a no-op and does nothing.
    ///
    /// The method atomically updates:
    /// - The total bytes written counter
    /// - The write I/O operation counter (incremented by 1)
    ///
    /// # Arguments
    ///
    /// * `bytes` - The number of bytes written in this operation
    pub fn report_write(bytes: u64) {
        Self::with(|req| {
            use std::sync::atomic::Ordering;
            req.0
                .resource_usage
                .write_bytes
                .fetch_add(bytes, Ordering::Relaxed);
            req.0
                .resource_usage
                .write_iops
                .fetch_add(1, Ordering::Relaxed);
        });
    }
}

/// An RAII guard for managing active request context cleanup.
///
/// `RequestGuard` is returned by [`RequestContext::set_scoped`] and automatically
/// performs cleanup when dropped. Specifically, it:
///
/// 1. Stops the CPU time stopwatch for the active request
/// 2. Records the CPU time (user and kernel) to the request's resource usage
/// 3. If the active request is a child, aggregates its resource usage to the parent
/// 4. Clears the active request from thread-local storage
///
/// This guard ensures proper resource cleanup via RAII semantics, so resource
/// aggregation happens automatically even if exceptions or early returns occur.
pub struct RequestGuard;

impl Drop for RequestGuard {
    fn drop(&mut self) {
        RequestContext::with(|req| {
            let elapsed = req.0.stopwatch.elapsed();

            req.0
                .resource_usage
                .cpu_user_ns
                .fetch_add(elapsed.user_ns, std::sync::atomic::Ordering::Relaxed);
            req.0
                .resource_usage
                .cpu_kernel_ns
                .fetch_add(elapsed.kernel_ns, std::sync::atomic::Ordering::Relaxed);

            if let Some(parent) = &req.0.parent {
                parent.0.resource_usage.add(&req.0.resource_usage);
            }
        });
    }
}
