//! Join handle implementations for thread pool task results.
//!
//! This module provides handle types that allow waiting for and retrieving results from
//! tasks executed on thread pools. It offers both unrestricted lifetime (`JoinHandle`)
//! and scoped lifetime (`ScopedJoinHandle`) variants to support different execution patterns.
//!
//! ## Handle Types
//!
//! - [`JoinHandle<R>`]: For tasks with `'static` lifetimes that can be stored and passed around freely
//! - [`ScopedJoinHandle<'scope, R>`]: For tasks with limited lifetimes that must complete within a scope
//!
//! Both handle types provide the same core functionality for checking readiness and waiting for results,
//! but with different lifetime constraints to ensure memory safety.

use crate::oneshot::{self, OneshotReceiver};

/// A handle for waiting on the result of a task with `'static` lifetime.
///
/// `JoinHandle` represents a task that has been submitted for execution and provides
/// methods to check if the task is complete and to retrieve its result. The handle
/// is based on a oneshot channel receiver, allowing thread-safe communication between
/// the task execution context and the waiting context.
///
/// ## Lifecycle
///
/// 1. **Created**: When a task is spawned, a `JoinHandle` is returned
/// 2. **Pending**: The task is running or queued for execution
/// 3. **Ready**: The task has completed and the result is available
/// 4. **Consumed**: The result has been retrieved via [`join()`](Self::join)
pub struct JoinHandle<R>(OneshotReceiver<R>);

impl<R> JoinHandle<R> {
    /// Creates a new `JoinHandle` from a oneshot receiver.
    ///
    /// This method is used internally by thread pools and execution contexts
    /// to create handles for submitted tasks. The receiver will receive the
    /// result when the task completes.
    ///
    /// # Arguments
    ///
    /// * `rx` - The oneshot receiver that will receive the task result
    pub(crate) fn new(rx: OneshotReceiver<R>) -> JoinHandle<R> {
        JoinHandle(rx)
    }

    /// Creates a `JoinHandle` that is immediately ready with the given result.
    ///
    /// # Arguments
    ///
    /// * `res` - The result value that the handle should immediately provide
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that is ready and will return `res` when joined.
    pub fn ready(res: R) -> Self {
        Self(oneshot::ready(res))
    }

    /// Checks if the task result is ready without blocking.
    ///
    /// # Returns
    ///
    /// `true` if the task has completed and the result is ready to be retrieved,
    /// `false` if the task is still running.
    pub fn is_ready(&self) -> bool {
        !self.0.is_pending()
    }

    /// Waits for the task to complete and returns its result.
    ///
    /// This method blocks the current thread until the task completes and then
    /// returns the result. If the task has already completed, this returns
    /// immediately. This method consumes the handle, so it can only be called once.
    ///
    /// # Returns
    ///
    /// The result value produced by the task.
    pub fn join(self) -> R {
        self.0.recv().expect("recv")
    }

    /// Waits for all handles to complete and collects their results into a vector.
    ///
    /// This method takes an iterator of `JoinHandle`s and waits for all of them
    /// to complete, collecting their results in the same order as the input handles.
    /// This is a convenience method for waiting on multiple tasks simultaneously.
    ///
    /// # Arguments
    ///
    /// * `handles` - An iterator of `JoinHandle<R>` instances to wait for
    ///
    /// # Returns
    ///
    /// A `Vec<R>` containing the results from all handles in the same order
    /// as they were provided.
    pub fn join_all(handles: impl IntoIterator<Item = JoinHandle<R>>) -> Vec<R> {
        handles.into_iter().map(|h| h.join()).collect()
    }
}

/// A handle for waiting on the result of a scoped task with limited lifetime.
///
/// `ScopedJoinHandle` is similar to [`JoinHandle`] but includes a lifetime parameter
/// that ensures the handle cannot outlive the scope in which it was created. This
/// enables safe execution of tasks that borrow data from the local environment,
/// such as references to stack-allocated variables.
///
/// The key difference from `JoinHandle` is the `'scope` lifetime parameter that
/// ties the handle to a specific execution scope, preventing it from being moved
/// outside that scope and ensuring memory safety.
pub struct ScopedJoinHandle<'scope, R>(OneshotReceiver<R>, std::marker::PhantomData<&'scope ()>);

impl<'scope, R> ScopedJoinHandle<'scope, R> {
    /// Creates a new `ScopedJoinHandle` from a oneshot receiver.
    ///
    /// This method is used internally by scoped execution contexts to create
    /// handles for submitted tasks. The receiver will receive the result when
    /// the task completes, and the lifetime parameter ensures the handle cannot
    /// outlive its scope.
    ///
    /// # Arguments
    ///
    /// * `rx` - The oneshot receiver that will receive the task result
    pub(crate) fn new(rx: OneshotReceiver<R>) -> ScopedJoinHandle<'scope, R> {
        ScopedJoinHandle(rx, Default::default())
    }

    /// Creates a `ScopedJoinHandle` that is immediately ready with the given result.
    ///
    /// # Arguments
    ///
    /// * `res` - The result value that the handle should immediately provide
    ///
    /// # Returns
    ///
    /// A `ScopedJoinHandle` that is ready and will return `res` when joined.
    pub fn ready(res: R) -> Self {
        Self(oneshot::ready(res), Default::default())
    }

    /// Checks if the task result is ready without blocking.
    ///
    /// This method allows non-blocking checks to see if the scoped task has
    /// completed and a result is available. It behaves identically to
    /// [`JoinHandle::is_ready()`] but maintains the scope lifetime constraint.
    ///
    /// # Returns
    ///
    /// `true` if the task has completed and the result is ready to be retrieved,
    /// `false` if the task is still running or pending.
    pub fn is_ready(&self) -> bool {
        !self.0.is_pending()
    }

    /// Waits for the scoped task to complete and returns its result.
    ///
    /// This method blocks the current thread until the scoped task completes and
    /// then returns the result. If the task has already completed, this returns
    /// immediately. This method consumes the handle, so it can only be called once.
    ///
    /// # Returns
    ///
    /// The result value produced by the scoped task.
    pub fn join(self) -> R {
        self.0.recv().expect("recv")
    }
}
