//! Performant data spilling utilities for large-scale data processing.
//!
//! `amudai-spill-data` provides a number of data structures with support for efficient
//! serialization and deserialization to external storage when memory becomes constrained.
//! The crate is designed around the concept of "spilling" data structures from memory
//! to storage with minimal overhead, enabling the processing of datasets that exceed
//! available memory.
//!
//! **NOTE**: This crate focuses on simple and performant "dump"/"load" functionality for
//! dedicated data structures, rather than sophisticated demand paging and caching systems.
//! It provides building blocks that can be used by appropriately designed higher-level
//! flows through techniques like partitioning.
//!
//! # Core Concepts
//!
//! ## Memory Image Serialization
//!
//! Unlike traditional serialization libraries that use complex encoding formats, this crate
//! writes data structures as "memory images" - layouts that closely resemble their in-memory
//! representation. This approach provides:
//!
//! - **Zero-copy loading**: Data can be accessed directly from storage buffers
//! - **Minimal CPU overhead**: No complex encoding/decoding steps
//! - **Parallel processing**: Multiple regions can be processed concurrently
//! - **High throughput**: Optimized for data-intensive workloads
//!
//! ## Two-Phase Serialization
//!
//! The library uses a two-phase approach that enables efficient parallel processing:
//!
//! 1. **Size computation**: [`Dump::compute_size()`] calculates storage requirements
//! 2. **Memory image writing**: [`Dump::dump()`] performs the actual serialization
//!
//! This pattern allows for optimal storage allocation and parallel dumping of large datasets.
//!
//! ## Manifest System
//!
//! Each serialized data structure produces a [`Manifest`] containing:
//! - Metadata about the serialized data (sizes, counts, type information)
//! - Layout information for efficient loading
//! - Child manifests for nested structures
//!
//! The manifest enables the [`Load`] trait to conveniently reconstruct data structures
//! without parsing the entire serialized data.
//!
//! # Data Structures
//!
//! The library provides several collection types optimized for spilling:
//!
//! ## Fixed-Size Elements
//!
//! - **[`pod_vec::PodCollector`]**: For Plain Old Data types (numbers, structs with no pointers)
//! - **[`pod_vec::PodView`]**: Read-only view of POD data loaded from storage
//!
//! ## Variable-Size Elements
//!
//! - **[`bytes_vec::BytesCollector`]**: For collections of byte slices
//! - **[`bytes_vec::BytesView`]**: Read-only view of byte slice collections
//!
//! ## Advanced Collections
//!
//! - **[`segmented_vec::SegmentedVec`]**: Collections built from uniform-sized segments
//!   that can be processed independently and in parallel.

use amudai_io::{ReadAt, SlicedFile, WriteAt};

use crate::manifest::Manifest;

pub mod bytes_vec;
pub mod manifest;
pub mod pod_vec;
pub mod segmented_vec;
mod std_impls;

/// Trait for types that can be "dehydrated" to external storage.
///
/// The `Dump` trait enables efficient spilling of data structures to external storage
/// by writing out an almost direct "memory image" of the data with minimal to no encoding.
/// This approach prioritizes performance and enables parallel processing of large datasets.
///
/// # Design Philosophy
///
/// The trait is designed around the concept of "spilling" raw memory layouts from memory
/// to external storage when memory becomes constrained, while maintaining the ability to
/// efficiently reload the data later with minimal deserialization overhead. The serialization
/// process writes data structures in a format that closely mirrors their in-memory representation,
/// avoiding expensive encoding/decoding steps.
///
/// Key principles:
/// - **Memory image serialization**: Data is written in a format that closely resembles
///   its in-memory layout, enabling fast reconstruction
/// - **Minimal encoding**: No complex serialization formats - data is written almost
///   directly as stored in memory
/// - **Alignment to I/O granularity**: Each contiguous stored buffer position is aligned
///   to a page boundary, making the process friendly to unbuffered (direct) I/O.
/// - **Parallel-friendly**: The two-phase approach enables parallel dumping of large
///   datasets across multiple storage regions
/// - **Zero-copy reconstruction**: The resulting data can often be loaded with minimal
///   copying or transformation
///
/// This is particularly useful for:
/// - Large-scale data processing where datasets exceed available memory
/// - High-performance data pipelines that cannot afford serialization overhead
/// - Parallel processing of datasets that can be chunked and processed independently
///
/// # Two-Phase Serialization
///
/// The dumping process follows a two-phase pattern that enables parallel processing:
///
/// 1. **Size Computation**: [`compute_size()`](Dump::compute_size) calculates the maximum number
///    of bytes needed for the memory image, providing a tight upper bound that allows the storage
///    system to allocate adequate space and potentially distribute work across multiple threads
///    or concurrent I/O operations.
/// 2. **Memory Image Writing**: [`dump()`](Dump::dump) performs the actual memory image
///    serialization to the pre-allocated storage region, writing at most the number of bytes
///    returned by [`compute_size()`](Dump::compute_size). This step can often be parallelized
///    for large data structures.
///
/// This approach enables:
/// - **Parallel dumping**: Large datasets can be split across multiple storage regions
///   and dumped concurrently
/// - **Optimal storage allocation**: Exact size calculation prevents waste and enables
///   efficient storage planning
/// - **Zero-copy operations**: Direct memory-to-storage transfers where possible
/// - **Efficient nested structures**: Child structures can be dumped independently
///   and possibly in parallel
///
/// # Manifest System
///
/// Each dump operation produces a [`Manifest`] that contains:
/// - Metadata about the serialized data (sizes, counts, type information)
/// - Child manifests for nested structures
/// - Custom properties specific to the data type
/// - Layout information for efficient loading
///
/// This manifest serves as a dehydrated representation of the data structure.
///
/// The manifest enables the [`Load`] trait to reconstruct the original data structure
/// efficiently without needing to parse the entire serialized data.
///
/// # Implementation Requirements
///
/// Implementors must ensure:
/// - [`compute_size()`](Dump::compute_size) returns the maximum number of bytes that
///   [`dump()`](Dump::dump) may write, providing as tight a bound as possible
/// - All writes respect the provided [`SlicedFile`] boundaries
/// - The returned manifest contains sufficient information for reconstruction
/// - The dumped data combined with the manifest is self-contained and can be loaded
///   by the corresponding [`Load`] implementation
pub trait Dump {
    /// Computes the maximum number of bytes required for serialization.
    ///
    /// This method must return the maximum number of bytes that [`dump()`](Dump::dump)
    /// may write to storage, serving as an upper bound for storage allocation.
    /// The number returned must be a multiple of [`PAGE_SIZE`](manifest::PAGE_SIZE).
    /// The storage system uses this value to allocate sufficient space before calling
    /// [`dump()`](Dump::dump). Implementations should strive to make this bound as tight
    /// as possible to minimize storage waste.
    ///
    /// # Implementation Notes
    ///
    /// - The returned size should be the maximum possible bytes needed, accounting for
    ///   any padding, alignment requirements, or variable-size encoding overhead
    /// - For variable-size data, compute the maximum size based on current contents
    /// - The size should be deterministic - calling this method multiple times
    ///   should return the same value if the data hasn't changed
    /// - Strive for tight bounds to minimize storage waste while ensuring sufficient space
    ///
    /// # Returns
    ///
    /// The maximum number of bytes that may be written during serialization.
    fn compute_size(&self) -> u64;

    /// Serializes the data to the provided storage writer.
    ///
    /// This method performs the actual serialization of the data structure to external
    /// storage. It must write at most [`compute_size()`](Dump::compute_size) bytes and
    /// produce a manifest that enables the data to be reconstructed via [`Load::load()`].
    ///
    /// # Parameters
    ///
    /// * `writer` - A [`SlicedFile`] representing the storage region allocated for this
    ///   data structure. The size of this region equals the value returned by
    ///   [`compute_size()`](Dump::compute_size).
    ///
    /// # Returns
    ///
    /// A [`Manifest`] containing metadata necessary for loading the data back.
    /// This typically includes:
    /// - Data layout information (sizes, offsets, counts)
    /// - Type-specific metadata
    /// - Child manifests for nested structures. **NOTE**: The range of each
    ///   [`NestedManifest`](manifest::NestedManifest) is **relative** to the
    ///   parent's [`SlicedFile`].
    ///
    /// # Implementation Requirements
    ///
    /// - Must write at most [`compute_size()`](Dump::compute_size) bytes
    /// - All writes must be within the bounds of the provided [`SlicedFile`]
    /// - The manifest must contain sufficient information for [`Load::load()`]
    ///   to reconstruct the data structure
    /// - Should be deterministic - the same data should produce equivalent
    ///   serialized output
    fn dump<W>(&self, writer: SlicedFile<W>) -> std::io::Result<Manifest>
    where
        W: WriteAt + Clone;

    /// Convenience method that dumps data and seals it with a manifest footer.
    ///
    /// This method combines the dumping process with manifest serialization,
    /// creating a self-contained storage file that can be loaded using
    /// [`Load::load_sealed()`]. It:
    ///
    /// 1. Calls [`compute_size()`](Dump::compute_size) to determine space requirements
    /// 2. Calls [`dump()`](Dump::dump) to serialize the data
    /// 3. Serializes the manifest as a footer at the end of the file
    /// 4. Returns the total file size
    ///
    /// # Parameters
    ///
    /// * `writer` - A storage writer that can grow to accommodate the data and manifest
    ///
    /// # Returns
    ///
    /// The final size of the storage file after dumping and sealing.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] for any I/O failures during the dump or seal process.
    fn dump_and_seal<W>(&self, writer: W) -> std::io::Result<u64>
    where
        W: WriteAt + Clone,
    {
        let size = self.compute_size();
        let manifest = self.dump(SlicedFile::new(writer.clone(), 0..size))?;
        let footer = manifest.save_as_footer(writer, size)?;
        Ok(footer.end)
    }
}

/// Trait for types that can be loaded ("rehydrated") from external storage.
///
/// The `Load` trait enables efficient deserialization of data structures from memory images
/// that were previously serialized using the [`Dump`] trait. It provides zero-copy loading
/// by directly interpreting storage buffers as memory layouts with minimal transformation.
///
/// # Design Philosophy
///
/// The trait is designed for fast reconstruction of data structures from external storage
/// by treating stored data as direct memory images rather than encoded representations.
/// This approach minimizes CPU overhead and enables high-throughput data access patterns.
///
/// Key principles:
/// - **Memory image interpretation**: Stored data is treated as a direct memory layout
///   that can be interpreted with minimal transformation
/// - **Zero-copy access**: Data is accessed directly from storage buffers without
///   intermediate copying or complex deserialization
/// - **Parallel loading**: Multiple data regions can be loaded concurrently
/// - **Lazy reconstruction**: Only requested portions need to be accessed, enabling
///   efficient partial loading of large datasets
/// - **Type safety guardrails**: The manifest system provides data integrity while
///   maintaining performance through minimal validation overhead
///
/// # Memory Image Loading
///
/// The loading process interprets raw storage data as structured memory layouts:
/// - **Direct buffer interpretation**: Storage [`Bytes`](amudai_bytes::Bytes) returned
///   from [`ReadAt::read_at`] are directly cast or interpreted as the target data types
///   where possible
/// - **Minimal validation**: Only essential checks are performed to ensure data integrity
/// - **Shared buffer access**: Multiple views can reference the same underlying storage
///   without duplication
/// - **Parallel reconstruction**: Large structures can be loaded across multiple threads
///   by dividing storage regions
///
/// # Loading Modes
///
/// The trait supports two primary loading modes:
///
/// 1. **Sealed Loading**: [`load_sealed()`](Load::load_sealed) loads from files created
///    by [`Dump::dump_and_seal()`], where the manifest footer describes the memory layout
/// 2. **Direct Loading**: [`load()`](Load::load) loads from a specific storage region
///    using an explicitly provided manifest that describes the memory image structure
///
/// # Manifest-Driven Reconstruction
///
/// The loading process is driven by the [`Manifest`] created during dumping:
/// - **Layout information**: Describes how raw bytes should be interpreted as data structures
/// - **Type metadata**: Ensures compatibility between dump and load operations
///
/// # Memory Management
///
/// Load implementations prioritize zero-copy access patterns:
/// - Use [`amudai_bytes::Bytes`] for shared, immutable byte buffers that can be
///   directly interpreted as data structures
/// - Avoid redundant allocations by working directly with storage buffers
/// - Support efficient slicing and sub-views without copying underlying data
/// - Enable multiple concurrent views into the same memory regions
///
/// # Error Handling
///
/// Load operations can fail due to:
/// - I/O errors reading from storage ([`std::io::Error`])
/// - Incompatible manifest data (wrong types, sizes, versions)
/// - Corrupted storage data
/// - Missing required manifest properties
/// - Storage size mismatches
///
/// # Type Safety and Versioning
///
/// Implementations should:
/// - Verify that stored data matches the expected type
/// - Check size compatibility for POD types
/// - Fail fast on incompatible data rather than producing incorrect results
pub trait Load: Sized {
    /// Loads a data structure from storage using the provided manifest.
    ///
    /// This method reconstructs the data structure from its serialized form using
    /// the metadata contained in the manifest. It should efficiently handle the
    /// loading process, preferring zero-copy access where possible.
    ///
    /// # Parameters
    ///
    /// * `reader` - A [`SlicedFile`] providing access to the storage region containing
    ///   the serialized data. The size and bounds of this reader correspond to the
    ///   storage allocated during dumping.
    /// * `manifest` - The [`Manifest`] containing metadata about the serialized data,
    ///   including layout information, sizes, and type-specific properties.
    ///
    /// # Returns
    ///
    /// A fully reconstructed instance of the data structure.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if:
    /// - I/O errors occur while reading from storage
    /// - The manifest contains invalid or incompatible data
    /// - Required manifest properties are missing
    /// - Data size mismatches are detected
    ///
    /// # Implementation Guidelines
    ///
    /// - Validate manifest data before attempting to read storage
    /// - Use zero-copy approaches (like [`amudai_bytes::Bytes`]) when possible
    /// - Check data integrity using manifest metadata
    /// - Handle nested structures by recursively loading child manifests
    /// - Fail fast on incompatible data rather than producing incorrect results
    ///
    /// # Type Safety
    ///
    /// Implementations should verify:
    /// - Item sizes match the expected type (for POD data)
    /// - Data lengths are consistent with manifest metadata
    /// - Required manifest properties are present and valid
    /// - Target data structure invariants are upheld upon loading
    fn load<R>(reader: SlicedFile<R>, manifest: &Manifest) -> std::io::Result<Self>
    where
        R: ReadAt + Clone;

    /// Convenience method for loading from files created with [`Dump::dump_and_seal()`].
    ///
    /// This method handles the complete loading process for sealed files by:
    /// 1. Reading the manifest footer from the end of the file
    /// 2. Parsing the manifest to understand the data layout
    /// 3. Creating a [`SlicedFile`] for the data region (excluding the footer)
    /// 4. Calling [`load()`](Load::load) to reconstruct the data structure
    ///
    /// # Parameters
    ///
    /// * `reader` - A storage reader providing access to a sealed file created by
    ///   [`Dump::dump_and_seal()`]. The file must contain both the serialized data
    ///   and a manifest footer.
    ///
    /// # Returns
    ///
    /// A fully reconstructed instance of the data structure.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if:
    /// - The file is not properly sealed (missing or invalid footer)
    /// - I/O errors occur during reading
    /// - The manifest is corrupted or incompatible
    /// - Any errors from the underlying [`load()`](Load::load) call
    fn load_sealed<R>(reader: R) -> std::io::Result<Self>
    where
        R: ReadAt + Clone,
    {
        let (manifest, reader) = Manifest::load_from_footer(reader)?;
        Self::load(reader, &manifest)
    }
}

/// Trait for collections that provide basic length information in the context of
/// [`Dump`] and [`Load`] implementations.
///
/// The `Collection` trait represents the most basic interface for collection-like
/// data structures. It provides only essential size information, making it suitable
/// as a foundation for both mutable and immutable collection types.
///
/// # Design Philosophy
///
/// This trait is intentionally minimal to:
/// - Serve as a common base for all collection types in the library
/// - Enable generic functions that only need to know collection sizes
/// - Provide a foundation for building more complex collection traits
/// - Support both owned and view-based collection implementations
///
/// # Usage Patterns
///
/// The trait is commonly used in:
/// - Generic functions that need to check if collections are empty
/// - Algorithms that need to know collection sizes for allocation
/// - Trait bounds where only basic size information is needed
/// - Building more complex collection interfaces
///
/// # Implementation Notes
///
/// Implementations should ensure that:
/// - [`len()`](Collection::len) returns the current number of elements
/// - The length is efficiently computable (typically O(1))
/// - Length changes are immediately reflected in subsequent calls
/// - The length represents logical elements, not underlying storage size
pub trait Collection {
    /// Returns the number of elements in the collection.
    ///
    /// This method should return the current count of logical elements in the
    /// collection, not the underlying storage capacity or byte size.
    ///
    /// # Returns
    ///
    /// The number of elements currently stored in the collection.
    ///
    /// # Performance
    ///
    /// This operation should be O(1) - implementations should cache the length
    /// or design their data structures so that the length can be computed efficiently.
    fn len(&self) -> usize;

    /// Checks whether the collection length is zero.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Trait for mutable collections that support appending elements.
///
/// The `Collector` trait extends [`Collection`] with mutation capabilities,
/// specifically focusing on append-style operations that are optimized for
/// building large datasets incrementally. It's designed around the common pattern
/// of constructing collections by adding elements one at a time.
///
/// # Design Philosophy
///
/// The trait is designed for:
/// - **Append-optimized collections**: Collections that are primarily built by
///   adding elements to the end, which is the most common pattern in data processing
/// - **Memory efficiency**: Includes capacity management to reduce allocation overhead
/// - **Storage preparation**: Built-in support for preparing collections that will
///   eventually be dumped to external storage
///
/// # Memory Management
///
/// The trait provides two types of capacity reservation:
/// - [`reserve_items()`](Collector::reserve_items): Reserve space for a specific number of elements
/// - [`reserve_data()`](Collector::reserve_data): Reserve space for a specific number of bytes
///
/// This dual approach accommodates both fixed-size elements (where count-based
/// reservation is natural) and variable-size elements (where byte-based reservation
/// is more appropriate).
///
/// # Element Types
///
/// The `Item` associated type uses `?Sized` to support both:
/// - **Owned types**: `i32`, `String`, custom structs, etc.
/// - **Unsized types**: `[u8]`, `str`, and other dynamically-sized types
///
/// # Performance Considerations
///
/// - Use [`with_item_capacity()`](Collector::with_item_capacity) when the final size is known
///   to avoid repeated reallocations
/// - Call [`reserve_items()`](Collector::reserve_items) or [`reserve_data()`](Collector::reserve_data)
///   before bulk operations
/// - Prefer batch operations when available to amortize allocation costs
pub trait Collector: Collection {
    /// The type of elements stored in this collection.
    ///
    /// # Examples
    ///
    /// - `PodCollector<u64>` has `Item = u64`
    /// - `BytesCollector` has `Item = [u8]`
    type Item: ?Sized;

    /// Creates a new collector with the specified capacity
    /// (number of logical elements).
    ///
    /// # Parameters
    ///
    /// * `capacity` - The number of elements to pre-allocate space for
    ///
    /// # Returns
    ///
    /// A new, empty collector with at least the specified capacity.
    fn with_item_capacity(capacity: usize) -> Self;

    /// Creates a new collector with the specified data capacity expressed in bytes.
    ///
    /// This method is particularly useful for collections storing variable-length
    /// data where the number of elements is unknown but the total data size can
    /// be estimated. It allows the collection to optimize its internal storage
    /// layout based on expected data volume.
    ///
    /// # Parameters
    ///
    /// * `capacity_bytes` - The number of bytes to pre-allocate space for
    ///
    /// # Returns
    ///
    /// A new, empty collector with at least the specified byte capacity.
    fn with_data_capacity(capacity_bytes: usize) -> Self;

    /// Reserves capacity for at least the specified number of additional elements.
    ///
    /// # Parameters
    ///
    /// * `additional` - The number of additional elements to reserve space for
    ///
    fn reserve_items(&mut self, additional: usize);

    /// Reserves capacity for at least the specified number of additional bytes.
    ///
    /// This method is particularly useful for collections storing variable-length
    /// data where the number of elements is unknown but the total data size can
    /// be estimated. It allows the collection to optimize its internal storage
    /// layout based on expected data volume.
    ///
    /// # Parameters
    ///
    /// * `additional_bytes` - The number of additional bytes of data to reserve space for
    fn reserve_data(&mut self, additional_bytes: usize);

    /// Appends an element to the back of the collection.
    ///
    /// This method adds a new element to the end of the collection, growing the
    /// collection as needed. It takes a reference to the item to support both
    /// owned and unsized types efficiently.
    ///
    /// # Parameters
    ///
    /// * `value` - A reference to the item to add to the collection
    ///
    /// # Performance
    ///
    /// This operation is typically amortized O(1) but may occasionally be O(n)
    /// if reallocation is required. Use [`reserve_items()`](Collector::reserve_items)
    /// or [`reserve_data()`](Collector::reserve_data) to minimize reallocations.
    fn push(&mut self, value: &Self::Item);

    /// Sets the length of the collection to the specified value.
    ///
    /// This method modifies the collection's length, either by truncating it if
    /// `new_len` is smaller than the current length, or by extending it with
    /// default values if `new_len` is larger.
    ///
    /// # Parameters
    ///
    /// * `new_len` - The desired new length of the collection
    fn set_len(&mut self, new_len: usize);

    /// Clears the collector, removing all values.
    ///
    /// This method usually retains the allocated capacity of the collector.
    fn clear(&mut self);
}
