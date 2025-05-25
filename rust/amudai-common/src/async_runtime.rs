//! Task spawning abstractions for the async runtime.
//! **Note**: at the moment, these are simple functions with baked-in tokio-based
//! implementation.

pub use impls::{block_on, spawn, spawn_blocking, JoinHandle};

#[cfg(feature = "tokio")]
mod impls {
    use std::future::Future;

    pub type JoinHandle<T> = tokio::task::JoinHandle<T>;

    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::task::spawn(future)
    }

    pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio::task::spawn_blocking(f)
    }

    pub fn block_on<F: Future>(f: F) -> F::Output {
        tokio::runtime::Handle::current().block_on(f)
    }
}
