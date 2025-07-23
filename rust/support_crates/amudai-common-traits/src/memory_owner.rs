//! `MemoryOwner`: A trait for types that own aligned memory buffers.

/// A trait for types that own aligned memory buffers.
///
/// # Safety
///
/// Implementors must guarantee that:
/// - The memory returned by `memory()` remains valid and immutable
///   for the entire lifetime of the owner.
/// - Memory is exclusively owned by the `MemoryOwner` instance,
///   with no shared ownership or cloning of the underlying buffer.
/// - The memory is at least 64-byte aligned.
/// - The reported length and capacity are accurate.
/// - The capacity is a multiple of 64 bytes.
pub unsafe trait MemoryOwner {
    /// Returns information about the owned memory block.
    fn memory(&self) -> MemoryAllocation;
}

/// Represents a block of allocated memory with its size information.
#[derive(Debug, Clone)]
pub struct MemoryAllocation {
    /// Pointer to the start of the allocated memory.
    pub ptr: *const u8,
    /// Current length of the allocated memory in bytes.
    pub len: usize,
    /// Total capacity of the allocated memory in bytes.
    pub capacity: usize,
    /// Formal alignment of the memory buffer.
    pub alignment: usize,
}
