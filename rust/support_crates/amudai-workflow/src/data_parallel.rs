//! Parallel data processing utilities.
//!
//! This module provides functions for processing collections of data either sequentially
//! or in parallel, depending on the size of the collection and the specified degree of
//! parallelism. The functions automatically choose the most appropriate execution strategy.
//!
//! The module includes:
//! - [`for_each`] - Execute a function for each item in an iterator
//! - [`map`] - Transform each item in an iterator and collect results
//!
//! Both functions use the global eager thread pool ([`EagerPool`]) for parallel execution
//! and fall back to sequential processing for small collections or when parallelism is disabled.

use crate::eager_pool::EagerPool;
use amudai_request::RequestContext;

/// Executes a function for each item in an iterator, optionally in parallel.
///
/// This function provides a parallel alternative to the standard `for_each` operation.
/// It automatically decides whether to execute sequentially or in parallel based on
/// the number of items and the specified maximum degree of parallelism.
///
/// # Arguments
///
/// * `max_degree` - Optional maximum number of parallel threads to use. If `None`,
///   uses all available threads. If 1 or less, forces sequential execution.
/// * `items` - An iterator of items to process
/// * `f` - The function to apply to each item
///
/// # Type Parameters
///
/// * `T` - The type of items in the iterator, must implement `Send`
/// * `F` - The function type, must implement `Fn(T) + Send + Sync`
///
/// # Behavior
///
/// - If there's only 1 item or fewer to process, executes sequentially
/// - If `max_degree` is 1 or less, executes sequentially
/// - Otherwise, executes in parallel using the eager thread pool
pub fn for_each<T, F>(max_degree: Option<usize>, items: impl IntoIterator<Item = T>, f: F)
where
    F: Fn(T) + Send + Sync,
    T: Send,
{
    let items = items.into_iter();
    if items.size_hint().1.map(|hint| hint <= 1).unwrap_or(false)
        || max_degree.unwrap_or(usize::MAX) <= 1
    {
        for item in items {
            f(item);
        }
    } else {
        let parent_req = RequestContext::active();
        EagerPool::global().restricted_scope(max_degree.unwrap_or(usize::MAX), |scope| {
            for item in items {
                let parent = parent_req.clone();
                scope.spawn(|| {
                    let _guard = parent
                        .map(|p| p.spawn(amudai_guid::Guid::new_v4()))
                        .map(RequestContext::set_scoped);
                    f(item);
                });
            }
        });
    }
}

/// Maps a function over an iterator, optionally in parallel, returning an iterator of results.
///
/// This function provides a parallel alternative to the standard `map` operation.
/// It automatically decides whether to execute sequentially or in parallel based on
/// the number of items and the specified maximum degree of parallelism.
///
/// # Arguments
///
/// * `max_degree` - Optional maximum number of parallel threads to use. If `None`,
///   uses all available threads. If 1 or less, forces sequential execution.
/// * `items` - An iterator of items to process
/// * `f` - The function to apply to each item, transforming `T` to `R`
///
/// # Type Parameters
///
/// * `T` - The type of input items, must implement `Send`
/// * `F` - The function type, must implement `Fn(T) -> R + Send + Sync`
/// * `R` - The type of output items, must implement `Send`
///
/// # Returns
///
/// An iterator yielding the results of applying `f` to each input item.
///
/// # Behavior
///
/// - If there's only 1 item or fewer to process, executes sequentially
/// - If `max_degree` is 1 or less, executes sequentially
/// - Otherwise, executes in parallel using the eager thread pool
/// - In parallel mode, all results are collected before returning the iterator
///
/// # Performance Considerations
///
/// - For small collections or simple operations, sequential execution may be faster
/// - Parallel execution is most beneficial for CPU-intensive transformations
/// - The function `f` should be thread-safe as it may be called concurrently
pub fn map<T, F, R>(
    max_degree: Option<usize>,
    items: impl IntoIterator<Item = T>,
    f: F,
) -> impl Iterator<Item = R>
where
    F: Fn(T) -> R + Send + Sync,
    T: Send,
    R: Send,
{
    let items = items.into_iter();
    if items.size_hint().1.map(|hint| hint <= 1).unwrap_or(false)
        || max_degree.unwrap_or(usize::MAX) <= 1
    {
        let mut results = Vec::new();
        for item in items {
            let res = f(item);
            results.push(res);
        }
        results.into_iter()
    } else {
        let parent_req = RequestContext::active();

        EagerPool::global().restricted_scope(max_degree.unwrap_or(usize::MAX), |scope| {
            let mut deferred = Vec::new();
            for item in items {
                let parent = parent_req.clone();
                let res = scope.spawn(|| {
                    let _guard = parent
                        .map(|p| p.spawn(amudai_guid::Guid::new_v4()))
                        .map(RequestContext::set_scoped);
                    f(item)
                });
                deferred.push(res);
            }
            deferred
                .into_iter()
                .map(|d| d.join())
                .collect::<Vec<_>>()
                .into_iter()
        })
    }
}
