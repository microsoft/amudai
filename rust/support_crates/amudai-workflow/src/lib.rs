//! Workflow execution utilities for parallel and concurrent processing.
//!
//! This crate provides a set of tools for executing work in parallel and managing
//! concurrent tasks. It includes a few high-level parallel processing utilities
//! designed to simplify concurrent programming patterns.
//!
//! # Key Components
//!
//! ## Thread Pools
//!
//! - [`thread_pool::ThreadPool`] - A traditional thread pool for task execution
//! - [`eager_pool::EagerPool`] - An eager thread pool that prevents deadlocks by
//!   falling back to synchronous execution when all workers are busy
//!
//! ## Communication Channels
//!
//! - [`oneshot`] - Single-value communication between threads
//! - [`simple_mpmc`] - Multi-producer, multi-consumer channels (temporary until
//!   `std::sync::mpmc` is stabilized)
//!
//! ## High-Level Parallel Processing
//!
//! - [`data_parallel`] - Parallel `map` and `for_each` operations with automatic
//!   sequential fallback for small datasets
//!
//! ## Task Management
//!
//! - [`join_handle`] - Handles for waiting on task results with both static and
//!   scoped lifetime variants
//!
//! # Design Philosophy
//!
//! This crate emphasizes ease of use and deadlock prevention in concurrent programming.
//! The eager execution strategy ensures that work is never queued indefinitely, making
//! it safe to use in nested parallel contexts where traditional thread pools might
//! deadlock due to thread starvation.

pub mod data_parallel;
pub mod eager_pool;
pub mod join_handle;
pub mod oneshot;
pub mod simple_mpmc;
pub mod thread_pool;
