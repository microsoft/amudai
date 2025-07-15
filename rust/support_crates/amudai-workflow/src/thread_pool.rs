//! Thread pool implementation for concurrent task execution.
//!
//! This module provides a simple thread pool implementation that allows spawning
//! functions on worker threads. Work items can be submitted for execution and
//! either waited on for completion (using [`JoinHandle`]) or executed in a
//! fire-and-forget manner.

use crate::{
    join_handle::JoinHandle,
    oneshot,
    simple_mpmc::{Receiver, Sender},
};
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

/// A simple thread pool for executing concurrent tasks.
///
/// `ThreadPool` manages a pool of worker threads that execute tasks (functions)
/// submitted through the [`spawn`](Self::spawn) and [`spawn_detached`](Self::spawn_detached)
/// methods. The implementation uses an internal multi-producer, multi-consumer channel
/// to distribute tasks among worker threads.
///
/// ## Cloning
///
/// `ThreadPool` implements [`Clone`] and all clones share the same underlying
/// thread pool. This allows multiple parts of an application to submit tasks
/// to the same pool of worker threads.
///
/// ## Thread Safety
///
/// All methods on `ThreadPool` are thread-safe and can be called concurrently
/// from multiple threads.
#[derive(Clone)]
pub struct ThreadPool(Sender<TaskFn>);

/// A boxed function that can be executed by a worker thread.
///
/// This type represents a task that can be sent to worker threads for execution.
/// The task must be `Send + 'static` to ensure it can be safely transferred
/// between threads and doesn't contain any references with limited lifetimes.
type TaskFn = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
    /// Creates a new `ThreadPool` with the specified number of worker threads.
    ///
    /// This constructor spawns `num_threads` worker threads that will process
    /// tasks submitted to the pool. Each worker thread runs in a loop, waiting
    /// for tasks and executing them as they become available.
    ///
    /// # Arguments
    ///
    /// * `num_threads` - The number of worker threads to spawn. Must be greater than 0.
    ///
    /// # Panics
    ///
    /// Panics if `num_threads` is 0.
    pub fn new(num_threads: usize) -> Self {
        Self::with_thread_name(num_threads, |_| String::new())
    }

    /// Creates a new `ThreadPool` with the specified number of worker threads and custom
    /// thread names.
    ///
    /// This constructor spawns `num_threads` worker threads that will process tasks
    /// submitted to the pool. Each worker thread runs in a loop, waiting for tasks
    /// and executing them as they become available. The provided `thread_name` function
    /// is called for each thread with its index to generate a custom name.
    ///
    /// Thread names are useful for debugging and profiling purposes, as they appear in
    /// debuggers, profilers, and system monitoring tools. If the `thread_name` function
    /// returns an empty string for a given thread index, that thread will not have
    /// a custom name set.
    ///
    /// # Arguments
    ///
    /// * `num_threads` - The number of worker threads to spawn. Must be greater than 0.
    /// * `thread_name` - A function that takes a thread index (0-based) and returns
    ///   the name for that thread.
    ///
    /// # Panics
    ///
    /// Panics if `num_threads` is 0.
    pub fn with_thread_name(num_threads: usize, thread_name: impl Fn(usize) -> String) -> Self {
        assert_ne!(num_threads, 0);

        let (tx, rx) = crate::simple_mpmc::channel::<TaskFn>();
        for i in 0..num_threads {
            let rx = rx.clone();
            let mut builder = thread::Builder::new();
            let name = thread_name(i);
            if !name.is_empty() {
                builder = builder.name(name);
            }
            builder
                .spawn(move || Self::thread_fn(rx))
                .expect("spawn thread");
        }

        ThreadPool(tx)
    }

    /// Configures the size of the global `ThreadPool` thread pool.
    ///
    /// This method sets the number of threads that will be used when the global
    /// `ThreadPool` is lazily initialized via [`global()`](Self::global). This configuration
    /// must be called before the first call to `global()` to take effect.
    ///
    /// # Arguments
    ///
    /// * `pool_size` - The desired number of threads for the global `ThreadPool`.
    ///   Values less than 1 will be clamped to 1.
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and can be called from multiple threads concurrently.
    /// However, only the first call before global `ThreadPool` initialization will have
    /// effect. Subsequent calls after the global instance is created will be ignored.
    pub fn configure_global_pool_size(pool_size: usize) {
        let pool_size = pool_size.max(1);
        GLOBAL_POOL_SIZE.store(pool_size, Ordering::SeqCst);
    }

    /// Returns a reference to the global `ThreadPool` instance.
    ///
    /// The global `ThreadPool` is lazily initialized on the first call to this method.
    /// The number of worker threads is determined by:
    /// 1. A value previously set via [`configure_global_pool_size()`](Self::configure_global_pool_size), or
    /// 2. A calculated value based on system CPU count: `(available_parallelism * 3 + 1) / 2`, or
    /// 3. A fallback value of 8 threads if CPU count cannot be determined
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe. Multiple concurrent calls will return the same
    /// instance, and the initialization is guaranteed to happen only once.
    pub fn global() -> &'static ThreadPool {
        static QUEUE: OnceLock<ThreadPool> = OnceLock::new();
        QUEUE.get_or_init(|| ThreadPool::new(Self::get_global_pool_size()))
    }

    /// Creates a new `ThreadPool` with a default number of worker threads.
    ///
    /// The number of worker threads is determined automatically based on the number
    /// of logical CPUs available on the system. If the CPU count cannot be determined,
    /// defaults to 8 threads.
    ///
    /// This is equivalent to calling [`new()`](Self::new) with the result of
    /// `std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)`.
    pub fn with_default_threads() -> Self {
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        Self::new(num_threads)
    }

    /// Spawns a task on the thread pool and returns a handle to wait for the result.
    ///
    /// The provided function `f` will be executed on one of the worker threads.
    /// This method returns a [`JoinHandle`] that can be used to wait for the task
    /// to complete and retrieve its result.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The type of the function to execute. Must be `FnOnce() -> R + Send + 'static`.
    /// * `R` - The return type of the function. Must be `Send + 'static`.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to execute on a worker thread.
    ///
    /// # Returns
    ///
    /// A [`JoinHandle<R>`] that can be used to wait for completion and retrieve the result.
    pub fn spawn<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx_result, rx_result) = oneshot::channel::<R>();
        self.spawn_detached({
            move || {
                let result = f();
                let _ = tx_result.send(result); // Ignore send errors if receiver is dropped
            }
        });
        JoinHandle::new(rx_result)
    }

    /// Spawns a task on the thread pool without waiting for the result.
    ///
    /// The provided function `f` will be executed on one of the worker threads.
    /// Unlike [`spawn()`](Self::spawn), this method does not return a handle to wait
    /// for completion. Use this for fire-and-forget tasks where you don't need
    /// to wait for the result or know when the task completes.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The type of the function to execute. Must be `FnOnce() + Send + 'static`.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to execute on a worker thread.
    pub fn spawn_detached<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let task = Box::new(f);
        self.0
            .send(task)
            .expect("must have listening worker threads");
    }

    /// Determines the size of the global `ThreadPool` thread pool based on configuration
    /// and system resources.
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
                .map(|n| (n.get() * 3).div_ceil(2))
                .unwrap_or(8)
        } else {
            size
        }
    }
}

impl ThreadPool {
    /// Worker thread function that processes tasks from the queue.
    ///
    /// # Arguments
    ///
    /// * `rx` - The receiver end of the task channel.
    fn thread_fn(rx: Receiver<TaskFn>) {
        while let Ok(task) = rx.recv() {
            task();
        }
    }
}

impl Default for ThreadPool {
    /// Creates a default `ThreadPool` instance.
    ///
    /// This is equivalent to calling [`with_default_threads()`](Self::with_default_threads),
    /// which creates a thread pool with a number of worker threads based on the
    /// system's available parallelism.
    fn default() -> Self {
        Self::with_default_threads()
    }
}

/// Global configuration for the default thread pool size.
///
/// This atomic variable stores the configured size for the global thread pool.
/// A value of 0 indicates that no explicit size has been configured, and the
/// system should use the calculated default based on CPU count.
static GLOBAL_POOL_SIZE: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    #[test]
    fn test_new_thread_pool() {
        let pool = ThreadPool::new(2);
        drop(pool);
    }

    #[test]
    #[should_panic]
    fn test_new_thread_pool_zero_threads() {
        ThreadPool::new(0);
    }

    #[test]
    fn test_with_default_threads() {
        let pool = ThreadPool::with_default_threads();
        // Should create successfully
        drop(pool);
    }

    #[test]
    fn test_spawn_simple_task() {
        let pool = ThreadPool::new(2);
        let handle = pool.spawn(|| 42);
        let result = handle.join();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_spawn_multiple_tasks() {
        let pool = ThreadPool::new(2);
        let handles: Vec<_> = (0..10).map(|i| pool.spawn(move || i * 2)).collect();

        let results: Vec<_> = JoinHandle::join_all(handles);
        for (i, result) in results.into_iter().enumerate() {
            assert_eq!(result, i * 2);
        }
    }

    #[test]
    fn test_spawn_detached() {
        let pool = ThreadPool::new(2);
        let counter = Arc::new(Mutex::new(0));

        // Spawn multiple detached tasks
        let num_tasks = 10;
        for _ in 0..num_tasks {
            let counter = counter.clone();
            pool.spawn_detached(move || {
                let mut count = counter.lock().unwrap();
                *count += 1;
            });
        }

        // Wait a bit for tasks to complete
        std::thread::sleep(Duration::from_millis(100));

        let final_count = *counter.lock().unwrap();
        assert_eq!(final_count, num_tasks);
    }

    #[test]
    fn test_concurrent_task_execution() {
        let pool = ThreadPool::new(4);
        let start_time = Instant::now();
        let sleep_duration = Duration::from_millis(50);

        // Spawn tasks that sleep, should run concurrently
        let handles: Vec<_> = (0..4)
            .map(|_| {
                pool.spawn(move || {
                    std::thread::sleep(sleep_duration);
                    42
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.join(), 42);
        }

        let elapsed = start_time.elapsed();
        // With 4 threads running 4 tasks concurrently, should take roughly sleep_duration
        // Allow some overhead but should be much less than 4 * sleep_duration
        assert!(elapsed < sleep_duration * 2);
    }

    #[test]
    fn test_thread_pool_clone() {
        let queue1 = ThreadPool::new(2);
        let queue2 = queue1.clone();

        let handle1 = queue1.spawn(|| "from queue1");
        let handle2 = queue2.spawn(|| "from queue2");

        assert_eq!(handle1.join(), "from queue1");
        assert_eq!(handle2.join(), "from queue2");
    }

    #[test]
    fn test_task_with_captured_variables() {
        let pool = ThreadPool::new(2);
        let value = 100;
        let handle = pool.spawn(move || value * 2);
        assert_eq!(handle.join(), 200);
    }

    #[test]
    fn test_task_returning_different_types() {
        let pool = ThreadPool::new(2);

        let handle_int = pool.spawn(|| 42i32);
        let handle_string = pool.spawn(|| "hello".to_string());
        let handle_vec = pool.spawn(|| vec![1, 2, 3]);

        assert_eq!(handle_int.join(), 42);
        assert_eq!(handle_string.join(), "hello");
        assert_eq!(handle_vec.join(), vec![1, 2, 3]);
    }

    #[test]
    fn test_many_small_tasks() {
        let pool = ThreadPool::new(4);
        let num_tasks = 1000;

        let handles: Vec<_> = (0..num_tasks).map(|i| pool.spawn(move || i)).collect();

        for (expected, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.join(), expected);
        }
    }

    #[test]
    fn test_global_thread_pool() {
        // Test that we can get the global instance
        let global1 = ThreadPool::global();
        let global2 = ThreadPool::global();

        // Both should be the same instance (same address)
        assert!(std::ptr::eq(global1, global2));

        // Test that we can use the global pool
        let handle = global1.spawn(|| "global task");
        assert_eq!(handle.join(), "global task");
    }

    #[test]
    fn test_task_ordering_not_guaranteed() {
        let pool = ThreadPool::new(1); // Single thread to make timing more predictable
        let results = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let results = results.clone();
                pool.spawn(move || {
                    results.lock().unwrap().push(i);
                    i
                })
            })
            .collect();

        // Wait for all tasks to complete
        for handle in handles {
            handle.join();
        }

        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), 5);
        // Results should contain all numbers 0-4, but order is not guaranteed
        let mut sorted_results = final_results.clone();
        sorted_results.sort();
        assert_eq!(sorted_results, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_complex_computation() {
        let pool = ThreadPool::new(4);

        // Fibonacci calculation as an example of a more complex task
        fn fibonacci(n: u32) -> u64 {
            match n {
                0 => 0,
                1 => 1,
                _ => fibonacci(n - 1) + fibonacci(n - 2),
            }
        }

        let handles: Vec<_> = (0..10).map(|i| pool.spawn(move || fibonacci(i))).collect();

        let expected = vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34];
        for (expected_val, handle) in expected.into_iter().zip(handles) {
            assert_eq!(handle.join(), expected_val);
        }
    }

    #[test]
    fn test_mixed_spawn_and_spawn_detached() {
        let pool = ThreadPool::new(3);
        let counter = Arc::new(Mutex::new(0));

        // Mix of spawn and spawn_detached
        let handle1 = pool.spawn(|| 42);

        let counter_clone = counter.clone();
        pool.spawn_detached(move || {
            *counter_clone.lock().unwrap() += 10;
        });

        let handle2 = pool.spawn(|| 84);

        let counter_clone = counter.clone();
        pool.spawn_detached(move || {
            *counter_clone.lock().unwrap() += 20;
        });

        // Wait for spawned tasks
        assert_eq!(handle1.join(), 42);
        assert_eq!(handle2.join(), 84);

        // Give detached tasks time to complete
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(*counter.lock().unwrap(), 30);
    }

    #[test]
    fn test_stress_with_single_thread() {
        let pool = ThreadPool::new(1);
        let num_tasks = 100;

        let handles: Vec<_> = (0..num_tasks).map(|i| pool.spawn(move || i * i)).collect();

        for (i, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.join(), i * i);
        }
    }

    #[test]
    fn test_spawn_detached_with_shared_state() {
        let pool = ThreadPool::new(4);
        let shared_vec = Arc::new(Mutex::new(Vec::new()));

        let num_tasks = 20;
        for i in 0..num_tasks {
            let shared_vec = shared_vec.clone();
            pool.spawn_detached(move || {
                shared_vec.lock().unwrap().push(i);
            });
        }

        // Wait for all detached tasks to complete
        std::thread::sleep(Duration::from_millis(100));

        let final_vec = shared_vec.lock().unwrap();
        assert_eq!(final_vec.len(), num_tasks);

        // Verify all values are present (order may vary)
        let mut sorted_vec = final_vec.clone();
        sorted_vec.sort();
        let expected: Vec<_> = (0..num_tasks).collect();
        assert_eq!(sorted_vec, expected);
    }

    #[test]
    fn test_recursive_task_spawning() {
        let pool = ThreadPool::new(2);
        let queue_clone = pool.clone();

        let handle = pool.spawn(move || {
            // Spawn another task from within a task
            let inner_handle = queue_clone.spawn(|| 100);
            inner_handle.join() + 50
        });

        assert_eq!(handle.join(), 150);
    }
}
