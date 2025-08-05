use std::any::Any;

/// A marker trait that enables type-erased handling of prepared shards across crate
/// boundaries.
///
/// `PreparedShard` represents the intermediate stage between building a shard with
/// [`ShardBuilder`] and creating a [`SealedShard`] that is committed to persistent storage.
/// It contains all the prepared stripe data and metadata needed to finalize the shard,
/// but hasn't yet been written to the object store.
///
/// This trait serves as a bridge to avoid circular dependencies between `amudai-index-core`
/// and `amudai-shard` crates. It allows the index core to work with `PreparedShard` concept
/// without directly depending on its concrete implementations.
///
/// # Purpose
///
/// The primary purpose of `DynPreparedShard` is to provide a mechanism for downcasting
/// type-erased prepared shard objects back to their concrete type (`PreparedShard`) when
/// needed.
pub trait DynPreparedShard: Send + Sync + 'static {
    fn type_id(&self) -> std::any::TypeId;

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync + 'static>;
}

/// A marker trait for type-erased handling of open shard instances across crate boundaries.
///
/// This trait serves as a bridge to avoid circular dependencies between `amudai-index-core`
/// and `amudai-shard` crates. It allows the index core to work with `Shard` concept without
/// directly depending on its implementations.
///
/// # Purpose
///
/// `DynShard` enables the index core to accept references to open shard instances while
/// maintaining crate separation.
///
/// # Usage
///
/// This trait is implemented by the `Shard` struct in the `amudai-shard` crate.
/// Index implementations can then work with `Box<dyn DynShard>` to accept shard instances
/// without `amudai-index-core` depending on `amudai-shard`.
pub trait DynShard: Send + Sync + 'static {
    fn type_id(&self) -> std::any::TypeId;

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync + 'static>;
}
