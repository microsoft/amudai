//! Memory-mapped buffer implementation with support for large pages.
//!
//! This module provides `MmapBuffer`, a low-level memory buffer backed by memory-mapped
//! pages. It supports both regular memory pages and large pages (also known as huge pages)
//! for improved performance in scenarios with large memory allocations.
//!
//! # Large Pages
//!
//! Large pages can significantly improve performance by reducing TLB (Translation Lookaside Buffer)
//! misses. The module automatically attempts to enable large page support when requested and
//! falls back to regular pages if large pages are unavailable.
//!
//! # Safety
//!
//! While `MmapBuffer` implements `Send` and `Sync`, users must ensure that any data written
//! to the buffer is properly synchronized when accessed from multiple threads.

use std::sync::OnceLock;

use amudai_common_traits::memory_owner::{MemoryAllocation, MemoryOwner};

use crate::mmap;

/// A memory-mapped buffer that provides raw memory allocation.
///
/// `MmapBuffer` wraps platform-specific memory mapping functionality and provides
/// a safe interface for allocating and deallocating memory pages. It supports both
/// regular pages and large pages for improved performance.
pub struct MmapBuffer {
    /// Raw pointer to the allocated memory region.
    ptr: *mut u8,
    /// The requested size of the buffer in bytes.
    len: usize,
    /// The actual allocated capacity, which may be larger than `len` due to page
    /// alignment.
    capacity: usize,
    /// Whether this buffer was allocated using large pages.
    uses_large_pages: bool,
    /// Buffer alignment (this is effectively the page size used during allocation)
    alignment: usize,
}

impl MmapBuffer {
    /// Returns the size of a regular memory page on the current system.
    ///
    /// This value represents the standard page size used by the operating system's
    /// virtual memory subsystem.
    pub fn regular_page_size() -> usize {
        mmap::get_page_size()
    }

    /// Returns the size of a large (huge) memory page on the current system.
    ///
    /// Large pages are an optimization feature that can reduce TLB (Translation
    /// Lookaside Buffer) pressure for applications with large memory footprints.
    ///
    /// # Platform Notes
    ///
    /// - **Windows**: Typically 2MB, requires "Lock Pages in Memory" privilege
    /// - **Linux**: Usually 2MB or 1GB, requires hugepage configuration
    /// - **macOS**: Large pages may not be available on all versions
    ///
    /// The actual availability of large pages depends on:
    /// - System configuration
    /// - Available memory
    /// - Process privileges
    pub fn large_page_size() -> usize {
        mmap::get_large_page_size()
    }

    /// Allocates a memory buffer with automatic fallback from large pages to regular pages.
    ///
    /// This method first attempts to allocate using large pages for better performance.
    /// If large page allocation fails (e.g., due to insufficient privileges or system
    /// configuration), it automatically falls back to regular page allocation.
    ///
    /// # Arguments
    ///
    /// * `size` - The desired size of the buffer in bytes. Will be rounded up to the
    ///   nearest page boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if both large page and regular page allocation fail.
    pub fn allocate_with_fallback(size: usize) -> std::io::Result<MmapBuffer> {
        if let Ok(buf) = Self::allocate_large_pages(size) {
            return Ok(buf);
        }
        Self::allocate_regular(size)
    }

    /// Allocates a memory buffer using large pages.
    ///
    /// Large pages can improve performance for applications with large memory footprints
    /// by reducing TLB misses. This method requires appropriate system privileges and
    /// configuration.
    ///
    /// # Arguments
    ///
    /// * `size` - The desired size of the buffer in bytes. Will be rounded up to the
    ///   nearest large page boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Large page support cannot be enabled
    /// - The system lacks sufficient continuous memory for large pages
    /// - The process lacks necessary privileges
    pub fn allocate_large_pages(size: usize) -> std::io::Result<MmapBuffer> {
        check_and_enable_large_page_support()?;

        let (ptr, capacity) = mmap::allocate_large_pages(size.max(1))?;
        assert!((ptr as usize).is_multiple_of(Self::large_page_size()));
        Ok(MmapBuffer {
            ptr: ptr as _,
            len: size,
            capacity,
            uses_large_pages: true,
            alignment: mmap::get_large_page_size(),
        })
    }

    /// Allocates a memory buffer using regular pages.
    ///
    /// This is the standard allocation method that works on all systems without special
    /// configuration or privileges.
    ///
    /// # Arguments
    ///
    /// * `size` - The desired size of the buffer in bytes. Will be rounded up to the
    ///   nearest page boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if the system cannot allocate the requested memory.
    pub fn allocate_regular(size: usize) -> std::io::Result<MmapBuffer> {
        let (ptr, capacity) = mmap::allocate(size.max(1))?;
        assert!((ptr as usize).is_multiple_of(Self::regular_page_size()));
        Ok(MmapBuffer {
            ptr: ptr as _,
            len: size,
            capacity,
            uses_large_pages: false,
            alignment: mmap::get_page_size(),
        })
    }

    /// Returns the length of the buffer in bytes.
    ///
    /// This is the size that was requested during allocation, not the actual
    /// allocated capacity which may be larger due to page alignment.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the buffer has a length of 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the actual allocated capacity in bytes.
    ///
    /// The capacity is always at least as large as the requested length and is
    /// aligned to page boundaries (regular or large page boundaries depending on
    /// the allocation type).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns a raw pointer to the beginning of the allocated memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The pointer is not used after the `MmapBuffer` is dropped
    /// - Any access to the memory region is within bounds (0..len)
    /// - Proper synchronization is used for concurrent access
    #[inline]
    pub fn ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// Returns `true` if this buffer was allocated using large pages.
    ///
    /// This can be useful for diagnostics or for making decisions about
    /// memory access patterns.
    #[inline]
    pub fn uses_large_pages(&self) -> bool {
        self.uses_large_pages
    }

    /// Returns the alignment of the buffer in bytes.
    ///
    /// The alignment value corresponds to the page size used during allocation:
    /// - For regular page allocations, this is typically 4KB (4096 bytes)
    ///   on most systems
    /// - For large page allocations, this is typically 2MB or 1GB depending
    ///   on the system
    #[inline]
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    /// Resizes the logical length of the buffer.
    ///
    /// This method adjusts the buffer's reported length without actually reallocating
    /// memory. The new length is clamped to the buffer's allocated capacity - it cannot
    /// exceed the capacity that was allocated when the buffer was created.
    ///
    /// # Arguments
    ///
    /// * `new_len` - The desired new length in bytes. If this value exceeds the buffer's
    ///   capacity, the length will be set to the capacity instead.
    ///
    /// # Important Notes
    ///
    /// - This method does **not** allocate or deallocate memory
    /// - When shrinking, the memory beyond the new length remains allocated but will
    ///   not be accessible through the buffer's slice methods
    /// - When growing (up to capacity), the contents of the newly accessible memory
    ///   region are unspecified and may contain arbitrary data (the buffer is zeroed
    ///   out upon initial allocation).
    pub fn resize(&mut self, new_len: usize) {
        self.len = std::cmp::min(new_len, self.capacity);
    }

    /// Returns an immutable byte slice view of the buffer contents.
    ///
    /// This method provides access to the buffer's memory as a byte slice.
    /// The slice covers the entire requested length of the buffer, not the
    /// allocated capacity.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Returns a mutable byte slice view of the buffer contents.
    ///
    /// This method provides mutable access to the buffer's memory as a byte slice.
    /// The slice covers the entire requested length of the buffer, not the
    /// allocated capacity.
    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Returns an immutable slice of type `T` from the buffer's memory.
    ///
    /// This method reinterprets the buffer's bytes as a slice of type `T`. The number
    /// of elements in the returned slice depends on the size of `T` and will be
    /// `self.len() / size_of::<T>()`.
    ///
    /// # Type Requirements
    ///
    /// The type `T` must implement `bytemuck::AnyBitPattern`, which ensures that any
    /// bit pattern is a valid value for the type. This is typically true for primitive
    /// types and simple structs without padding.
    ///
    /// # Panics
    ///
    /// Panics if the buffer's length is not evenly divisible by the size of `T`.
    #[inline]
    pub fn as_slice<T>(&self) -> &[T]
    where
        T: bytemuck::AnyBitPattern,
    {
        bytemuck::cast_slice(self.as_bytes())
    }

    /// Returns a mutable slice of type `T` from the buffer's memory.
    ///
    /// This method reinterprets the buffer's bytes as a mutable slice of type `T`.
    /// The number of elements in the returned slice depends on the size of `T` and
    /// will be `self.len() / size_of::<T>()`.
    ///
    /// # Type Requirements
    ///
    /// The type `T` must implement both:
    /// - `bytemuck::AnyBitPattern`: ensures any bit pattern is a valid value
    /// - `bytemuck::NoUninit`: ensures the type has no uninitialized bytes (padding)
    ///
    /// These constraints guarantee safe mutable access to the reinterpreted memory.
    ///
    /// # Panics
    ///
    /// Panics if the buffer's length is not evenly divisible by the size of `T`.
    #[inline]
    pub fn as_mut_slice<T>(&mut self) -> &mut [T]
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        bytemuck::cast_slice_mut(self.as_bytes_mut())
    }
}

impl std::ops::Deref for MmapBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl std::ops::DerefMut for MmapBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_bytes_mut()
    }
}

impl AsRef<[u8]> for MmapBuffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsMut<[u8]> for MmapBuffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_bytes_mut()
    }
}

impl AsRef<[u64]> for MmapBuffer {
    #[inline]
    fn as_ref(&self) -> &[u64] {
        debug_assert_eq!(self.len, self.len.next_multiple_of(8));
        unsafe { std::slice::from_raw_parts(self.ptr as *const u64, self.len / 8) }
    }
}

impl AsMut<[u64]> for MmapBuffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u64] {
        debug_assert_eq!(self.len, self.len.next_multiple_of(8));
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u64, self.len / 8) }
    }
}

unsafe impl MemoryOwner for MmapBuffer {
    fn memory(&self) -> MemoryAllocation {
        MemoryAllocation {
            ptr: self.ptr,
            len: self.len,
            capacity: self.capacity,
            alignment: self.alignment,
        }
    }
}

impl Drop for MmapBuffer {
    /// Releases the allocated memory back to the system.
    ///
    /// The appropriate deallocation function is called based on whether the buffer
    /// was allocated with large pages or regular pages.
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            if self.uses_large_pages {
                let _ = unsafe { mmap::free_large_pages(self.ptr as _, self.capacity) };
            } else {
                let _ = unsafe { mmap::free(self.ptr as _, self.capacity) };
            }
        }
    }
}

// SAFETY: MmapBuffer can be safely sent between threads as it owns the memory region
// and properly deallocates it on drop.
unsafe impl Send for MmapBuffer {}

// SAFETY: MmapBuffer can be safely shared between threads. However, users must ensure
// proper synchronization when accessing the underlying memory.
unsafe impl Sync for MmapBuffer {}

impl std::fmt::Debug for MmapBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapBuffer")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .finish()
    }
}

/// Checks if large page support is available and attempts to enable it if necessary.
///
/// This function is called automatically when attempting to allocate large pages.
/// The result is cached using `OnceLock` to avoid repeated system calls.
///
/// # Errors
///
/// Returns an error if large pages are not supported or cannot be enabled on the system.
/// Common reasons include:
/// - Insufficient privileges (may require administrator/root access)
/// - Large pages not configured in the system
/// - Platform doesn't support large pages
pub fn check_and_enable_large_page_support() -> std::io::Result<()> {
    static RESULT: OnceLock<std::io::Result<()>> = OnceLock::new();
    let res = RESULT.get_or_init(enable_large_page_support);
    match res {
        Ok(()) => Ok(()),
        Err(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
    }
}

/// Attempts to enable large page support on the system.
///
/// This function first checks if large pages are already available by attempting
/// a test allocation. If that fails, it tries to enable large page support through
/// platform-specific mechanisms and then verifies the enablement succeeded.
fn enable_large_page_support() -> std::io::Result<()> {
    if check_large_page_allocation().is_err() {
        mmap::try_enable_large_pages()?;
        check_large_page_allocation()
    } else {
        Ok(())
    }
}

/// Verifies that large page allocation is working by performing a test allocation.
///
/// This function allocates a minimal large page and immediately frees it to ensure
/// the system can successfully allocate large pages.
fn check_large_page_allocation() -> std::io::Result<()> {
    let (ptr, size) = mmap::allocate_large_pages(1)?;
    let _ = unsafe { mmap::free_large_pages(ptr, size) };
    Ok(())
}
