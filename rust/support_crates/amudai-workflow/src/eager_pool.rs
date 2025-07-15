//! Eager thread pool implementation with graceful fallback to synchronous execution.
//!
//! This module provides the [`EagerPool`] thread pool implementation that offers eager execution
//! semantics with automatic fallback to synchronous execution when all worker threads are busy.
//! Unlike traditional thread pools that queue work, `EagerPool` ensures immediate execution by
//! running tasks synchronously on the caller's thread when necessary.
//!
//! This strategy simplifies nested fork-join parallelism in non-async contexts: when a work item
//! running on the pool spawns nested work items (perhaps indirectly) and waits for their
//! results, this can lead to deadlocks due to thread pool starvation. This eager pool prevents such
//! situations.
//!
//! # Key Features
//!
//! - **Eager Execution**: Tasks execute immediately on available worker threads
//! - **Graceful Fallback**: When workers are busy, tasks run synchronously without blocking
//! - **No Queuing**: Work is never queued or rejected - it always executes immediately
//! - **Scoped Execution**: Support for both `'static` and scoped (non-`'static`) task execution
//! - **Global Pool**: Convenient global pool instance with lazy initialization
//!
//! # Execution Strategy
//!
//! The pool employs the following execution strategy:
//!
//! 1. **Check availability**: Look for an available worker thread
//! 2. **Dispatch to worker**: If available, send the task to a worker thread
//! 3. **Fallback to sync**: If all workers are busy, execute synchronously on the caller's thread

use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{Receiver, SyncSender},
    },
    usize,
};

use amudai_collections::atomic_bit_set::AtomicBitSet;

use crate::{
    join_handle::{JoinHandle, ScopedJoinHandle},
    oneshot,
};

/// Creates a scope for executing non-`'static` closures using the global thread pool.
///
/// This is a convenience function that creates a scope on the global [`EagerPool`] instance
/// for executing closures that capture references to local variables. All tasks spawned
/// within the scope are guaranteed to complete before this function returns.
///
/// # Arguments
///
/// * `f` - A closure that receives a [`Scope`] reference and can spawn tasks within that scope.
///   The closure can capture references to local variables that live at least as long as the
///   environment lifetime `'env`.
///
/// # Returns
///
/// The return value of the closure `f`.
pub fn scope<'env, F, R>(f: F) -> R
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> R,
{
    EagerPool::global().scope(f)
}

/// Executes a `'static` closure on the global thread pool.
///
/// This is a convenience function that executes a closure on the global [`EagerPool`] instance.
/// If an available worker thread exists, the closure will be executed on that thread.
/// If all worker threads are busy, the closure will be executed synchronously on the
/// caller's thread to ensure immediate execution without queuing.
///
/// # Arguments
///
/// * `f` - A closure that will be executed. Must be `Send + 'static` and return
///   a value that is also `Send + 'static`.
///
/// # Returns
///
/// A [`JoinHandle<R>`] that can be used to wait for the task completion and
/// retrieve the result.
pub fn spawn<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    EagerPool::global().spawn(f)
}

/// Executes two closures concurrently and returns their results as a tuple.
///
/// This is a convenience function that creates a scope and executes two closures
/// in parallel using the global thread pool. The function waits for both closures
/// to complete before returning their results.
///
/// # Arguments
///
/// * `fn_a` - The first closure to execute. Must be `Send` and return a `Send` value.
/// * `fn_b` - The second closure to execute. Must be `Send` and return a `Send` value.
///
/// # Returns
///
/// A tuple `(RA, RB)` containing the results of both closures.
pub fn join<A, B, RA, RB>(fn_a: A, fn_b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    scope(|scope| {
        let res_a = scope.spawn(fn_a);
        let res_b = scope.spawn(fn_b);
        (res_a.join(), res_b.join())
    })
}

/// Executes three closures concurrently and returns their results as a tuple.
pub fn join3<A, B, C, RA, RB, RC>(fn_a: A, fn_b: B, fn_c: C) -> (RA, RB, RC)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    C: FnOnce() -> RC + Send,
    RA: Send,
    RB: Send,
    RC: Send,
{
    scope(|scope| {
        let res_a = scope.spawn(fn_a);
        let res_b = scope.spawn(fn_b);
        let res_c = scope.spawn(fn_c);
        (res_a.join(), res_b.join(), res_c.join())
    })
}

/// Executes four closures concurrently and returns their results as a tuple.
pub fn join4<A, B, C, D, RA, RB, RC, RD>(fn_a: A, fn_b: B, fn_c: C, fn_d: D) -> (RA, RB, RC, RD)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    C: FnOnce() -> RC + Send,
    D: FnOnce() -> RD + Send,
    RA: Send,
    RB: Send,
    RC: Send,
    RD: Send,
{
    scope(|scope| {
        let res_a = scope.spawn(fn_a);
        let res_b = scope.spawn(fn_b);
        let res_c = scope.spawn(fn_c);
        let res_d = scope.spawn(fn_d);
        (res_a.join(), res_b.join(), res_c.join(), res_d.join())
    })
}

/// A thread pool that provides eager execution semantics with graceful fallback
/// to synchronous execution.
///
/// `EagerPool` is designed to opportunistically utilize available worker threads
/// while never blocking or queuing work when all threads are busy. Instead, it falls
/// back to executing tasks synchronously on the caller's thread, ensuring that work
/// is never rejected or delayed due to thread pool saturation.
///
/// ## Key Features
///
/// - Eager Execution: Tasks are executed immediately on available worker threads
/// - Graceful Fallback: When all workers are busy, tasks run synchronously on the
///   caller's thread
/// - No Queuing: Work is never queued or rejected - it always executes immediately
/// - Scoped Execution: Supports both 'static and scoped (non-'static) task execution
///
/// ## Execution Modes
///
/// The pool supports two primary execution modes:
///
/// 1. **Global Execution**: For `'static` closures using [`execute`](Self::execute)
/// 2. **Scoped Execution**: For non-`'static` closures using [`scope`](Self::scope)
///    and [`restricted_scope`](Self::restricted_scope)
///
/// ## Usage Examples
///
/// ### Basic Usage
///
/// ```rust,no_run
/// use amudai_workflow::eager_pool::EagerPool;
///
/// let pool = EagerPool::new(4);
///
/// // Execute a task that returns a value
/// let handle = pool.spawn(|| {
///     // Some computation
///     42
/// });
///
/// let result = handle.join(); // Wait for completion and get result
/// assert_eq!(result, 42);
/// ```
///
/// ### Scoped Execution
///
/// ```rust,no_run
/// use amudai_workflow::eager_pool::EagerPool;
///
/// let pool = EagerPool::new(4);
/// let data = vec![1, 2, 3, 4, 5];
///
/// pool.scope(|scope| {
///     let handle1 = scope.spawn(|| data.iter().sum::<i32>());
///     let handle2 = scope.spawn(|| data.len());
///     
///     let sum = handle1.join();
///     let len = handle2.join();
///     
///     println!("Sum: {}, Length: {}", sum, len);
/// });
/// ```
///
/// ### Restricted Parallelism
///
/// ```rust,no_run
/// use amudai_workflow::eager_pool::EagerPool;
///
/// let pool = EagerPool::new(8);
///
/// // Limit concurrent tasks to 3, even though pool has 8 threads
/// pool.restricted_scope(3, |scope| {
///     for i in 0..10 {
///         scope.spawn(move || {
///             println!("Task {}", i);
///         }).join();
///     }
/// });
/// ```
///
/// ## Global Pool
///
/// A global pool instance is available via [`global()`](Self::global), which is lazily initialized
/// with a default size based on system parallelism. The global pool size can be configured using
/// [`configure_global_pool_size()`](Self::configure_global_pool_size).
///
/// ## Drop Behavior
///
/// When an `EagerPool` is dropped, it signals all worker threads to terminate gracefully.
/// Any in-flight tasks will complete.
#[derive(Clone)]
pub struct EagerPool(Arc<Workers>);

impl EagerPool {
    /// Creates a new `EagerPool` with the specified number of worker threads.
    ///
    /// # Arguments
    ///
    /// * `num_threads` - The number of worker threads to spawn. Must be greater than 0.
    pub fn new(num_threads: usize) -> EagerPool {
        EagerPool(Workers::new(num_threads))
    }

    /// Configures the size of the global thread pool.
    ///
    /// This method sets the number of threads that will be used when the global pool
    /// is lazily initialized via [`global()`](Self::global). This configuration must
    /// be called before the first call to `global()` to take effect.
    ///
    /// # Arguments
    ///
    /// * `pool_size` - The desired number of threads for the global pool.
    ///   Values less than 1 will be clamped to 1.
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and can be called from multiple threads concurrently.
    /// However, only the first call before global pool initialization will have effect.
    pub fn configure_global_pool_size(pool_size: usize) {
        let pool_size = pool_size.max(1);
        GLOBAL_POOL_SIZE.store(pool_size, Ordering::SeqCst);
    }

    /// Returns a reference to the global `EagerPool` instance.
    ///
    /// The global pool is lazily initialized on the first call to this method.
    /// The pool size is determined by:
    /// 1. The value set by [`configure_global_pool_size()`](Self::configure_global_pool_size)
    ///    if called before first access
    /// 2. Otherwise, a default based on system parallelism: `(available_parallelism * 3 + 1) / 2`
    /// 3. Falls back to 8 threads if system parallelism cannot be determined
    pub fn global() -> &'static EagerPool {
        static POOL: OnceLock<EagerPool> = OnceLock::new();
        POOL.get_or_init(|| EagerPool::new(Self::get_global_pool_size()))
    }

    /// Executes a closure on an available worker thread or falls back to synchronous execution.
    ///
    /// This method attempts to execute the provided closure on an available worker thread.
    /// If all worker threads are busy, the closure is executed synchronously on the
    /// caller's thread.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that will be executed. Must be `Send + 'static` and return
    ///   a value that is also `Send + 'static`.
    ///
    /// # Returns
    ///
    /// A [`JoinHandle<R>`] that can be used to wait for the task completion and
    /// retrieve the result.
    pub fn spawn<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if let Some(index) = self.0.try_reserve() {
            let (tx, rx) = crate::oneshot::channel::<R>();
            let work_fn = move || {
                let res = f();
                let _ = tx.send(res);
            };
            self.0.spawn(index, Box::new(work_fn));
            JoinHandle::new(rx)
        } else {
            let res = f();
            JoinHandle::ready(res)
        }
    }

    /// Creates a scope for executing non-`'static` closures with automatic cleanup.
    ///
    /// This method creates a scope that allows executing closures that capture
    /// references to local variables. All tasks spawned within the scope are
    /// guaranteed to complete before this method returns.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that receives a [`Scope`] reference and can spawn tasks
    ///   within that scope.
    ///
    /// # Returns
    ///
    /// The return value of the closure `f`.
    ///
    /// # Lifetime Safety
    ///
    /// The scope ensures that all spawned tasks complete before any borrowed
    /// data goes out of scope.
    pub fn scope<'env, F, R>(&self, f: F) -> R
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> R,
    {
        let scope = Scope {
            workers: self.0.clone(),
            tracker: ScopeTracker::new(),
            scope: std::marker::PhantomData,
            env: std::marker::PhantomData,
        };
        let res = f(&scope);
        scope.tracker.wait();
        res
    }

    /// Creates a scope with a limit on the number of concurrently executing tasks.
    ///
    /// This method is similar to [`scope`](Self::scope) but adds a restriction on
    /// the maximum number of tasks that can execute concurrently on worker threads.
    /// Tasks beyond this limit will execute synchronously on the caller's thread.
    ///
    /// # Arguments
    ///
    /// * `max_parallel_tasks` - The maximum number of tasks that can execute
    ///   concurrently on worker threads. Must be at least 1.
    /// * `f` - A closure that receives a [`Scope`] reference and can spawn tasks
    ///   within that scope.
    ///
    /// # Returns
    ///
    /// The return value of the closure `f`.
    ///
    /// # Use Cases
    ///
    /// This is useful when you want to limit resource usage or when tasks have dependencies
    /// that require controlled parallelism.
    pub fn restricted_scope<'env, F, R>(&self, max_parallel_tasks: usize, f: F) -> R
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> R,
    {
        // Since it's always possible to run a work item on the caller's thread, the actual
        // max parallelism of the scope tracker is one less the specified.
        let max_parallel_tasks = std::cmp::max(max_parallel_tasks, 1) - 1;
        let scope = Scope {
            workers: self.0.clone(),
            tracker: ScopeTracker::restricted(max_parallel_tasks),
            scope: std::marker::PhantomData,
            env: std::marker::PhantomData,
        };
        let res = f(&scope);
        scope.tracker.wait();
        res
    }

    /// Returns the total number of tasks that have been spawned on worker threads.
    ///
    /// This counter tracks the cumulative number of tasks that have been successfully
    /// dispatched to worker threads (as opposed to being executed synchronously).
    /// It does not include tasks that were executed synchronously due to all worker
    /// threads being busy.
    ///
    /// # Returns
    ///
    /// The total number of tasks spawned on worker threads since the pool was created.
    ///
    /// # Use Cases
    ///
    /// This is primarily useful for testing, debugging, and performance monitoring
    /// to understand how effectively the pool is utilizing worker threads.
    pub fn spawn_counter(&self) -> usize {
        self.0.spawn_counter.load(Ordering::Relaxed)
    }

    /// Determines the size of the global thread pool based on configuration and system resources.
    ///
    /// This internal method calculates the appropriate pool size using the following logic:
    /// 1. If a size was explicitly set via [`configure_global_pool_size()`](Self::configure_global_pool_size),
    ///    use that value
    /// 2. Otherwise, calculate based on system parallelism: `(available_parallelism * 3 + 1) / 2`
    /// 3. Fall back to 8 threads if system parallelism cannot be determined
    ///
    /// # Returns
    ///
    /// The number of threads to use for the global pool.
    fn get_global_pool_size() -> usize {
        let size = GLOBAL_POOL_SIZE.load(Ordering::SeqCst);
        if size == 0 {
            std::thread::available_parallelism()
                .map(|n| (n.get() * 3 + 1) / 2)
                .unwrap_or(8)
        } else {
            size
        }
    }
}

impl Drop for EagerPool {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// A scope for executing non-`'static` closures with automatic cleanup and lifetime safety.
///
/// `Scope` provides a safe way to execute closures that capture references to local variables
/// by ensuring all spawned tasks complete before any borrowed data goes out of scope.
/// This enables parallel execution of tasks that reference stack-allocated data without
/// the lifetime restrictions of `'static` closures.
///
/// ## Task Execution Strategy
///
/// When executing tasks via [`execute`](Self::execute), the scope uses the following strategy:
///
/// 1. **Try to reserve a slot**: Check if the scope's parallelism limit allows another task
/// 2. **Try to reserve a worker**: Attempt to get an available worker thread
/// 3. **Execute on worker**: If both reservations succeed, dispatch to a worker thread
/// 4. **Fallback to sync**: If either reservation fails, execute synchronously on the caller's thread
pub struct Scope<'scope, 'env: 'scope> {
    workers: Arc<Workers>,
    tracker: Arc<ScopeTracker>,
    scope: std::marker::PhantomData<&'scope mut &'scope ()>,
    env: std::marker::PhantomData<&'env mut &'env ()>,
}

impl<'scope, 'env> Scope<'scope, 'env> {
    /// Executes a closure on an available worker thread or falls back to synchronous execution.
    ///
    /// This method attempts to execute the provided closure on an available worker thread
    /// within the scope's parallelism constraints. If no worker thread is available or the
    /// scope's parallelism limit is reached, the closure is executed synchronously on the
    /// caller's thread.
    ///
    /// ## Arguments
    ///
    /// * `f` - A closure that will be executed. The closure can capture references to local
    ///   variables that live at least as long as the scope's environment lifetime `'env`.
    ///   Must be `Send` to allow transfer to worker threads.
    ///
    /// ## Returns
    ///
    /// A [`ScopedJoinHandle<'scope, R>`] that can be used to wait for task completion and
    /// retrieve the result. The handle is tied to the scope's lifetime, ensuring it cannot
    /// outlive the scope.
    pub fn spawn<F, R>(&'scope self, f: F) -> ScopedJoinHandle<'scope, R>
    where
        F: FnOnce() -> R + Send + 'scope,
        R: Send + 'scope,
    {
        if self.tracker.try_reserve() {
            if let Some(index) = self.workers.try_reserve() {
                let tracker = self.tracker.clone();
                tracker.task_spawned();
                let (tx, rx) = oneshot::channel::<R>();
                let work_fn = move || {
                    let res = f();
                    let _ = tx.send(res);
                    tracker.task_completed();
                };
                let work_fn = Box::into_raw(Box::new(work_fn) as Box<dyn FnOnce() + Send>);
                // casting away the 'scope lifetime, pretending our F is 'static.
                let work_fn =
                    unsafe { Box::from_raw(work_fn as *mut (dyn FnOnce() + Send + 'static)) };
                self.workers.spawn(index, work_fn);
                return ScopedJoinHandle::new(rx);
            }
        }

        let res = f();
        ScopedJoinHandle::ready(res)
    }
}

static GLOBAL_POOL_SIZE: AtomicUsize = AtomicUsize::new(0);

type WorkItem = Box<dyn FnOnce() + Send + 'static>;

struct Workers {
    threads: Vec<Worker>,
    mask: AtomicBitSet,
    spawn_counter: AtomicUsize,
}

impl Workers {
    fn new(num_threads: usize) -> Arc<Workers> {
        let (threads, channels) = (0..num_threads)
            .map(|_| Worker::new())
            .unzip::<_, _, Vec<_>, Vec<_>>();
        let this = Arc::new(Workers {
            threads,
            mask: AtomicBitSet::new(num_threads),
            spawn_counter: AtomicUsize::new(0),
        });
        channels.into_iter().enumerate().for_each(|(i, rx)| {
            let this = this.clone();
            std::thread::spawn(move || Self::thread_fn(this, i, rx));
        });
        this
    }

    fn try_reserve(&self) -> Option<usize> {
        self.mask.try_claim_bit()
    }

    fn spawn(&self, reserved_index: usize, work: WorkItem) {
        assert!(self.mask.get(reserved_index, Ordering::SeqCst));
        self.spawn_counter.fetch_add(1, Ordering::Relaxed);
        self.threads[reserved_index]
            .tx
            .send(Message::Work(work))
            .expect("send");
    }

    fn stop(&self) {
        self.threads.iter().for_each(|t| {
            let _ = t.tx.send(Message::Stop);
        });
    }

    fn thread_fn(workers: Arc<Workers>, index: usize, rx: Receiver<Message>) {
        loop {
            let msg = rx.recv().expect("recv");
            match msg {
                Message::Work(f) => {
                    f();
                    workers.mask.reset(index);
                }
                Message::Stop => return,
            }
        }
    }
}

struct Worker {
    tx: SyncSender<Message>,
}

impl Worker {
    fn new() -> (Worker, Receiver<Message>) {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Message>(4);
        let this = Worker { tx };
        (this, rx)
    }
}

enum Message {
    Work(WorkItem),
    Stop,
}

struct ScopeTracker {
    /// Counter of running tasks combined with the `WAIT_STATE`.
    state: AtomicU64,
    /// Completion "event"
    completion: OnceLock<()>,
    /// Counter of running (parallel) tasks for the `max_tasks` enforcement.
    current_tasks: AtomicUsize,
    /// Max number of running (parallel) tasks.
    max_tasks: usize,
}

impl ScopeTracker {
    const WAIT_STATE: u64 = 0x8000000000000000;

    fn new() -> Arc<ScopeTracker> {
        Self::restricted(usize::MAX)
    }

    fn restricted(max_tasks: usize) -> Arc<ScopeTracker> {
        Arc::new(ScopeTracker {
            state: AtomicU64::new(0),
            completion: OnceLock::new(),
            current_tasks: AtomicUsize::new(0),
            max_tasks,
        })
    }

    fn try_reserve(&self) -> bool {
        if self.current_tasks.fetch_add(1, Ordering::Relaxed) + 1 > self.max_tasks {
            self.current_tasks.fetch_sub(1, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    fn task_spawned(&self) {
        let prev_state = self.state.fetch_add(1, Ordering::SeqCst);
        assert!(prev_state < Self::WAIT_STATE);
    }

    fn task_completed(&self) {
        self.current_tasks.fetch_sub(1, Ordering::Relaxed);
        let prev_state = self.state.fetch_sub(1, Ordering::SeqCst);
        assert_ne!(prev_state, 0);
        assert_ne!(prev_state, Self::WAIT_STATE);
        if prev_state > Self::WAIT_STATE {
            if prev_state - Self::WAIT_STATE == 1 {
                let _ = self.completion.set(());
            }
        }
    }

    fn wait(&self) {
        let prev_state = self.state.fetch_add(Self::WAIT_STATE, Ordering::SeqCst);
        assert!(prev_state < Self::WAIT_STATE);
        if prev_state == 0 {
            return;
        }
        self.completion.wait();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::eager_pool::JoinHandle;

    use super::EagerPool;

    #[test]
    fn test_eager_pool() {
        let pool = EagerPool::new(4);
        let current_id = std::thread::current().id();
        let worker_id = pool.spawn(|| std::thread::current().id()).join();
        assert_ne!(current_id, worker_id);

        let h = (0..4)
            .map(|_| pool.spawn(|| std::thread::sleep(Duration::from_millis(50))))
            .collect::<Vec<_>>();
        let worker_id = pool.spawn(|| std::thread::current().id()).join();
        JoinHandle::join_all(h);
        assert_eq!(worker_id, current_id);

        pool.scope(|scope| {
            let h0 = scope.spawn(|| std::thread::sleep(Duration::from_millis(50)));
            let h1 = scope.spawn(|| std::thread::sleep(Duration::from_millis(50)));
            h0.join();
            h1.join();
        });

        let a = vec![10u32; 50];
        let mut b = vec![0u32; 100];
        pool.scope(|scope| {
            let (b0, b1) = b.split_at_mut(50);
            scope.spawn(|| b0.copy_from_slice(&a));
            scope.spawn(|| b1.copy_from_slice(&a));
        });
        assert_eq!(&a, &b[0..50]);
        assert_eq!(&a, &b[50..100]);

        let prev_spawned = pool.spawn_counter();
        pool.restricted_scope(2, |scope| {
            scope.spawn(|| std::thread::sleep(Duration::from_millis(50)));
            scope.spawn(|| std::thread::sleep(Duration::from_millis(50)));
        });
        let spawned = pool.spawn_counter();
        assert_eq!(spawned - prev_spawned, 1);
    }
}
