//! Global registry for index types (index providers) in the Amudai indexing system.
//!
//! This module provides a thread-safe, global registry for managing different index type
//! implementations. The registry serves as a central repository where index types can be
//! registered at runtime and retrieved by name when needed.
//!
//! # Overview
//!
//! The registry pattern allows for dynamic registration of index implementations, enabling:
//! - Plugin-style architecture for index types
//! - Runtime discovery of available index types
//! - Decoupling of index implementations from their consumers
//!
//! # Thread Safety
//!
//! The registry uses a `RwLock` to ensure thread-safe access, allowing multiple concurrent
//! readers while ensuring exclusive access during registration.

use std::sync::{Arc, RwLock};

use amudai_common::{Result, error::Error};

use crate::IndexType;

/// Registers a new index type in the global registry.
///
/// This function adds an index type implementation to the registry, making it available
/// for retrieval by name. The index type is stored as an `Arc<dyn IndexType>` to allow
/// shared ownership across multiple consumers.
///
/// # Arguments
///
/// * `index_type` - An index type implementation that can be converted into `Arc<dyn IndexType>`.
///   This includes raw `IndexType` implementations, `Box<dyn IndexType>`, or
///   existing `Arc<dyn IndexType>` instances.
///
pub fn add(index_type: impl Into<Arc<dyn IndexType>>) {
    let index_type = index_type.into();
    let name = index_type.name().to_string();
    REGISTRY.write().unwrap().insert(name, index_type);
}

/// Retrieves an index type from the registry by name.
///
/// This function looks up an index type implementation by its registered name and returns
/// a cloned `Arc` reference to it.
///
/// # Arguments
///
/// * `name` - The name of the index type to retrieve. This should match the name returned
///   by the index type's [`IndexType::name`] method.
///
/// # Returns
///
/// Returns `Ok(Arc<dyn IndexType>)` if an index type with the given name is found in the
/// registry, or an error if no such index type exists.
///
/// # Errors
///
/// Returns an `Error::invalid_arg` if no index type with the specified name is registered.
pub fn get(name: impl AsRef<str>) -> Result<Arc<dyn IndexType>> {
    let name = name.as_ref();
    let index_type = REGISTRY.read().unwrap().get(name).cloned();
    index_type.ok_or_else(|| {
        Error::invalid_arg("index name", format!("Index provider '{name}' not found"))
    })
}

/// Global registry for index types.
///
/// This static variable holds the global registry of index types, protected by a read-write
/// lock for thread-safe access. The registry uses `ahash::HashMap` for lookups with a fixed
/// state to allow `const fn` static construction.
///
/// The registry maps index type names (as strings) to their implementations (as `Arc<dyn IndexType>`).
/// The registry is initialized as an empty map and populated at runtime
static REGISTRY: RwLock<ahash::HashMap<String, Arc<dyn IndexType>>> =
    RwLock::new(ahash::HashMap::with_hasher(ahash::RandomState::with_seeds(
        65423554, 7123564654, 911002456, 3711888456,
    )));
