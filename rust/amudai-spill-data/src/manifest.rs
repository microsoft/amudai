//! Manifest system for describing memory image layouts and metadata.
//!
//! The manifest module provides the core metadata system used by the spill data library
//! to describe the structure and layout of serialized data. Manifests enable efficient
//! reconstruction of data structures without parsing the entire serialized content.
//!
//! # Overview
//!
//! A [`Manifest`] serves as a dehydrated blueprint that describes how serialized data
//! should be interpreted and reconstructed. It acts as the bridge between the
//! [`Dump`](crate::Dump) and [`Load`](crate::Load) traits, providing all necessary
//! metadata for efficient data recovery from storage.
//!
//! # Core Concepts
//!
//! ## Manifest Structure
//!
//! A [`Manifest`] acts as a hierarchical blueprint for understanding serialized data layout:
//!
//! - **Properties**: Key-value metadata pairs storing primitive values like counts, sizes,
//!   type information, and custom application-specific metadata
//! - **Named children**: Maps of named sub-manifests describing structured data components
//!   like fields in a struct or named sections in a composite data format
//! - **List children**: Ordered sequences of sub-manifests for array-like structures
//!   where order matters and components are similar in structure
//!
//! ## Memory Image Metadata
//!
//! Manifests describe "memory images" - data layouts that closely resemble in-memory
//! representations with minimal encoding overhead.
//!
//! ## Storage Alignment
//!
//! All manifest operations respect page-aligned boundaries ([`PAGE_SIZE`]) to optimize
//! I/O performance and enable direct (unbuffered) storage access patterns.
//!
//! # Usage Patterns
//!
//! ## Creating Manifests During Serialization
//!
//! Manifests are typically created during [`Dump::dump()`](crate::Dump::dump) operations
//! to record the structure of serialized data:
//!
//! ```rust,no_run
//! use amudai_spill_data::manifest::Manifest;
//!
//! # fn example_dump() -> std::io::Result<Manifest> {
//! let mut manifest = Manifest::default();
//!
//! // Store metadata about the serialized data
//! manifest.put("len", 1000u64);           // Number of items
//! manifest.put("item_size", 8u32);        // Size per item
//! manifest.put("total_bytes", 8000u64);   // Total data size
//!
//! // Add child manifests for nested structures
//! // For a BytesCollector, this might include:
//! // manifest.set_child("offsets", offsets_manifest);
//! // manifest.set_child("data", data_manifest);
//!
//! Ok(manifest)
//! # }
//! ```
//!
//! ## Loading from Manifests
//!
//! Manifests guide [`Load::load()`](crate::Load::load) operations to reconstruct data
//! by providing all necessary layout and type information:
//!
//! ```rust,no_run
//! use amudai_spill_data::manifest::Manifest;
//!
//! # fn load_example(manifest: &Manifest) -> std::io::Result<()> {
//! // Read basic metadata
//! let len: u64 = manifest.get("len")?;
//! let item_size: u32 = manifest.get("item_size")?;
//! let total_bytes: u64 = manifest.get("total_bytes")?;
//!
//! // Access child manifests for composite structures
//! let offsets_child = manifest.child("offsets")?;
//! let data_child = manifest.child("data")?;
//!
//! // Use child ranges for sub-region access
//! let offsets_range = &offsets_child.range;
//! let data_range = &data_child.range;
//! # Ok(())
//! # }
//! ```
//!
//! ## Sealed Storage Files
//!
//! The [`Dump::dump_and_seal()`](crate::Dump::dump_and_seal) pattern creates
//! self-describing files with manifest footers, enabling complete reconstruction
//! without external metadata:
//!
//! ```text
//! ┌─────────────┬──────────┬─────────────────────────────────┐
//! │ Data Region │ Manifest │ Footer: [size] [backoff]        │
//! └─────────────┴──────────┴─────────────────────────────────┘
//! ```
//!
//! These can be loaded using [`Manifest::load_from_footer()`] without requiring
//! separate manifest files:
//!
//! ```rust,no_run
//! # use std::sync::Arc;
//! # use amudai_io::ReadAt;
//! use amudai_spill_data::manifest::Manifest;
//!
//! # fn load_sealed_example(reader: Arc<dyn ReadAt>) -> std::io::Result<()> {
//! // Load manifest and get access to data region
//! let (manifest, data_region) = Manifest::load_from_footer(reader)?;
//!
//! // Reconstruct the original data using the manifest
//! // let my_data = MyDataType::load(data_region, &manifest)?;
//! # Ok(())
//! # }
//! ```

use std::{
    io::{Error, ErrorKind},
    ops::Range,
};

use amudai_bytes::align::{align_up_u64, is_aligned_u64};
use amudai_io::{ReadAt, SlicedFile, WriteAt, verify};
use bincode::{Decode, Encode};

use crate::{Dump, Load};

/// Metadata structure describing the layout and properties of serialized data.
///
/// A `Manifest` serves as a blueprint for understanding how raw storage bytes should be
/// interpreted as structured data. It contains metadata about data layouts, type information,
/// and hierarchical descriptions of nested data structures.
///
/// # Structure
///
/// - **Properties**: Key-value pairs storing primitive metadata (sizes, counts, type info)
/// - **Named children**: Map of named sub-manifests for structured data components
/// - **List children**: Ordered array of sub-manifests for sequential data structures
///
/// # Design Philosophy
///
/// Manifests are designed to enable reconstruction of complex data structures from raw
/// storage with minimal overhead:
///
/// - **Lightweight**: Stores only essential metadata, not the actual data
/// - **Hierarchical**: Nested manifests describe complex data structure trees
/// - **Type-agnostic**: Can describe any data layout through generic property system
/// - **Storage-efficient**: Uses compact bincode serialization
///
/// # Thread Safety
///
/// `Manifest` is `Clone` and can be safely shared across threads. The underlying data
/// is immutable once created during serialization.
#[derive(Debug, Default, Clone, Encode, Decode)]
pub struct Manifest {
    /// Key-value metadata properties storing primitive values as byte arrays.
    pub properties: ahash::HashMap<String, Vec<u8>>,

    /// Named child manifests describing structured data components.
    ///
    /// Used for data structures with named fields or components. Each child
    /// manifest describes a sub-region of the serialized data with its own
    /// layout and metadata.
    pub child_map: ahash::HashMap<String, NestedManifest>,

    /// Ordered child manifests for array-like data structures.
    ///
    /// Used when data contains sequences of similar structures that need
    /// to be processed in order. Each element describes a sub-region
    /// with its own manifest.
    pub child_list: Vec<NestedManifest>,
}

/// A manifest paired with its storage range relative to the storage range of the
/// parent manifest, describing a nested data structure.
///
/// `NestedManifest` combines a [`Manifest`] with the byte range where the described
/// data is located in storage, enabling hierarchical decomposition of complex data
/// structures while maintaining precise location information.
///
/// **NOTE: The byte range is always relative to the parent manifest range**.
///
/// # Usage
///
/// Nested manifests are created during serialization when dumping composite data
/// structures. They allow the loading process to:
///
/// - Understand the overall data layout through the manifest
/// - Access the specific storage region containing the data
/// - Recursively process nested components
///
/// # Example Structure
///
/// For a serialized `BytesCollector`, the manifest hierarchy might look like:
///
/// ```text
/// Root Manifest {
///   properties: { "len": 3 }
///   children: {
///     "offsets": NestedManifest { range: 0..24, manifest: {...} }
///     "data": NestedManifest { range: 24..1000, manifest: {...} }
///   }
/// }
/// ```
#[derive(Debug, Default, Clone, Encode, Decode)]
pub struct NestedManifest {
    /// Byte range in storage where this manifest's data is located.
    ///
    /// The range is relative to the parent storage region and indicates
    /// exactly where to find the data described by the associated manifest.
    pub range: Range<u64>,

    /// Manifest describing the structure and metadata of the data in this range.
    pub manifest: Manifest,
}

/// Page size constant used for storage alignment (4096 bytes).
///
/// All `dump()` operations align data to page boundaries to optimize I/O
/// performance and enable direct (unbuffered) storage access.
pub const PAGE_SIZE: u64 = 4096;

impl Manifest {
    /// Deserializes a manifest from a byte slice.
    ///
    /// Parses a manifest that was previously serialized using [`to_vec()`](Self::to_vec).
    /// Uses the standard bincode configuration with fixed-length integer encoding
    /// for deterministic serialization.
    ///
    /// # Parameters
    ///
    /// * `slice` - Byte slice containing the serialized manifest data
    ///
    /// # Returns
    ///
    /// Returns the deserialized `Manifest` on success, or an I/O error if the data
    /// is corrupted or incompatible.
    pub fn from_slice(slice: &[u8]) -> std::io::Result<Manifest> {
        bincode::decode_from_slice(slice, Self::binc_config())
            .map_err(|e| Error::other(e))
            .map(|(manifest, _)| manifest)
    }

    /// Loads a manifest from a sealed file footer.
    ///
    /// Reads a self-describing file that was created using [`Dump::dump_and_seal()`].
    /// Such files have a manifest footer containing:
    ///
    /// ```text
    /// [Data] [Manifest] [manifest_size: u64] [manifest_backoff: u64]
    /// ```
    ///
    /// The footer enables loading the manifest without external metadata, making
    /// the file completely self-describing.
    ///
    /// # Parameters
    ///
    /// * `reader` - Storage reader that implements `ReadAt + Clone`
    ///
    /// # Returns
    ///
    /// Returns a tuple of:
    /// - The loaded `Manifest` describing the data layout
    /// - A `SlicedFile` providing access to the data region (excluding manifest)
    ///
    pub fn load_from_footer<R>(reader: R) -> std::io::Result<(Manifest, SlicedFile<R>)>
    where
        R: ReadAt + Clone,
    {
        let reader_size = reader.size()?;
        verify!(reader_size >= 16);
        verify!(is_aligned_u64(reader_size, PAGE_SIZE));

        let footer = reader.read_at(reader_size - 16..reader_size)?;

        let manifest_size = u64::from_le_bytes(footer[..8].try_into().unwrap());
        let manifest_backoff = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        verify!(manifest_size <= reader_size);
        verify!(manifest_backoff <= reader_size);
        verify!(is_aligned_u64(manifest_backoff, PAGE_SIZE));

        let manifest_pos = reader_size - manifest_backoff;

        let manifest = Manifest::load(
            SlicedFile::new(reader.clone(), manifest_pos..reader_size),
            &Default::default(),
        )?;
        Ok((manifest, SlicedFile::new(reader, 0..manifest_pos)))
    }

    /// Wraps this manifest as a nested manifest with the given storage range.
    ///
    /// Creates a [`NestedManifest`] that combines this manifest with location information.
    /// This is typically used during serialization to record where in storage the
    /// manifest's described data is located.
    ///
    /// # Parameters
    ///
    /// * `range` - Byte range in parent's storage where this manifest's data is stored
    ///
    /// # Returns
    ///
    /// A `NestedManifest` containing this manifest and the specified range.
    pub fn into_nested(self, range: Range<u64>) -> NestedManifest {
        NestedManifest {
            range,
            manifest: self,
        }
    }

    /// Retrieves a property value as a byte slice.
    ///
    /// Properties are stored as raw byte arrays to support arbitrary data types.
    /// Use [`get()`](Self::get) for typed access to common primitive types.
    ///
    /// # Parameters
    ///
    /// * `name` - The property name to look up
    ///
    /// # Returns
    ///
    /// Returns a reference to the property's byte data, or a `NotFound` error
    /// if the property doesn't exist.
    pub fn get_bytes(&self, name: &str) -> std::io::Result<&[u8]> {
        self.properties
            .get(name)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, name))
            .map(|v| v.as_slice())
    }

    /// Retrieves a typed property value.
    ///
    /// Deserializes a property from its stored byte representation to the specified type.
    /// The type must implement `bytemuck::Pod` for safe byte-level interpretation.
    ///
    /// # Type Requirements
    ///
    /// - `T: Default + Copy + bytemuck::Pod` - Must be a Plain Old Data type
    /// - The stored byte data must be exactly `size_of::<T>()` bytes
    ///
    /// # Parameters
    ///
    /// * `name` - The property name to retrieve
    ///
    /// # Returns
    ///
    /// Returns the typed value, or an error if:
    /// - The property doesn't exist (`NotFound`)
    /// - The stored data size doesn't match the type size
    pub fn get<T>(&self, name: &str) -> std::io::Result<T>
    where
        T: Default + Copy + bytemuck::Pod,
    {
        let bytes = self.get_bytes(name)?;
        verify!(bytes.len() == std::mem::size_of::<T>());
        let mut value = T::default();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(bytes);
        Ok(value)
    }

    /// Stores a property value as raw bytes.
    ///
    /// Inserts or updates a property with arbitrary byte data. This is the most
    /// flexible property storage method, supporting any data that can be represented
    /// as bytes.
    ///
    /// # Parameters
    ///
    /// * `name` - Property name (will be converted to `String`)
    /// * `bytes` - Byte data to store (will be converted to `Vec<u8>`)
    pub fn put_bytes(&mut self, name: impl Into<String>, bytes: impl Into<Vec<u8>>) {
        self.properties.insert(name.into(), bytes.into());
    }

    /// Stores a typed property value.
    ///
    /// Serializes a POD (Plain Old Data) value as bytes and stores it as a property.
    /// This is the recommended way to store primitive types like integers, floats,
    /// and simple structs.
    ///
    /// # Type Requirements
    ///
    /// - `T: Copy + bytemuck::Pod` - Must be a copyable POD type
    ///
    /// # Parameters
    ///
    /// * `name` - Property name (will be converted to `String`)
    /// * `value` - The value to store
    pub fn put<T>(&mut self, name: impl Into<String>, value: T)
    where
        T: Copy + bytemuck::Pod,
    {
        self.put_bytes(name, bytemuck::bytes_of(&value));
    }

    /// Retrieves a named child manifest.
    ///
    /// Named children represent structured components of the serialized data,
    /// such as fields in a struct or named sections in a file format.
    ///
    /// # Parameters
    ///
    /// * `name` - The child name to look up
    ///
    /// # Returns
    ///
    /// Returns a reference to the [`NestedManifest`] containing both the child's
    /// manifest and its storage range, or a `NotFound` error if the child doesn't exist.
    pub fn child(&self, name: &str) -> std::io::Result<&NestedManifest> {
        self.child_map
            .get(name)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, name))
    }

    /// Stores a named child manifest.
    ///
    /// Associates a nested manifest with a name, typically used during serialization
    /// to record the structure of composite data types.
    ///
    /// # Parameters
    ///
    /// * `name` - Name for this child component (will be converted to `String`)
    /// * `child` - The nested manifest containing the child's manifest and storage range
    pub fn set_child(&mut self, name: impl Into<String>, child: NestedManifest) {
        self.child_map.insert(name.into(), child);
    }

    /// Retrieves a child manifest by index from the ordered list.
    ///
    /// List children represent sequential components like array elements or
    /// ordered data chunks.
    ///
    /// # Parameters
    ///
    /// * `index` - Zero-based index into the child list
    ///
    /// # Returns
    ///
    /// Returns a reference to the [`NestedManifest`] at the specified index,
    /// or a `NotFound` error if the index is out of bounds.
    pub fn child_at(&self, index: usize) -> std::io::Result<&NestedManifest> {
        self.child_list
            .get(index)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, index.to_string()))
    }

    /// Computes the exact encoded size of this manifest when serialized.
    ///
    /// This method provides the precise byte count needed to serialize this manifest
    /// using the standard bincode encoding. It's more accurate than [`estimate_encoded_size()`](Self::estimate_encoded_size)
    /// but slower due to the full encoding simulation.
    ///
    /// # Returns
    ///
    /// The exact number of bytes needed to serialize this manifest.
    ///
    /// # Performance
    ///
    /// This method performs a complete encoding simulation and should be used when
    /// exact sizes are required. For performance-critical scenarios where approximate
    /// sizes are sufficient, use [`estimate_encoded_size()`](Self::estimate_encoded_size).
    pub fn compute_encoded_size(&self) -> u64 {
        Tracker::compute_encoded_size(self)
    }

    /// Estimates the encoded size of this manifest when serialized.
    ///
    /// Provides a fast approximation of the serialized size without performing full
    /// encoding simulation. The estimate is typically close to the actual size but
    /// may not be perfectly accurate due to bincode's variable-length encoding.
    ///
    /// # Returns
    ///
    /// An estimated number of bytes needed to serialize this manifest.
    ///
    /// # Algorithm
    ///
    /// The estimation includes:
    /// - Base overhead for the manifest structure (~24 bytes)
    /// - Property storage: key length + value length + encoding overhead per property
    /// - Child manifest storage: name length + estimated child size + overhead per child
    /// - Recursive estimation for nested child manifests
    ///
    /// # Performance
    ///
    /// This method is significantly faster than [`compute_encoded_size()`](Self::compute_encoded_size)
    /// and suitable for performance-critical scenarios where exact sizes aren't required.
    pub fn estimate_encoded_size(&self) -> u64 {
        24u64
            + self
                .properties
                .iter()
                .map(|(n, v)| (n.len() + v.len() + 16) as u64)
                .sum::<u64>()
            + self
                .child_map
                .iter()
                .map(|(n, v)| n.len() as u64 + v.estimate_encoded_size() + 8)
                .sum::<u64>()
            + self
                .child_list
                .iter()
                .map(|v| v.estimate_encoded_size())
                .sum::<u64>()
    }

    /// Serializes this manifest to a byte vector.
    ///
    /// Converts the manifest to its binary representation using bincode with fixed-length
    /// integer encoding for deterministic serialization. This is the inverse of
    /// [`from_slice()`](Self::from_slice).
    ///
    /// # Returns
    ///
    /// A byte vector containing the serialized manifest data.
    pub fn to_vec(&self) -> std::io::Result<Vec<u8>> {
        bincode::encode_to_vec(self, Self::binc_config()).map_err(|e| Error::other(e))
    }

    /// Saves this manifest as a footer at the end of a storage writer.
    ///
    /// This is a utility method used internally by [`Dump::dump_and_seal()`](crate::Dump::dump_and_seal)
    /// to append manifest metadata to the end of a data file. The manifest is written
    /// at a page-aligned position starting from the given offset.
    ///
    /// # Parameters
    ///
    /// * `writer` - Storage writer to append the manifest to
    /// * `pos` - Starting position where the manifest should be written (will be aligned to page boundary)
    ///
    /// # Returns
    ///
    /// The byte range where the manifest was actually written after page alignment.
    ///
    /// # Storage Layout
    ///
    /// The method creates the footer layout expected by [`load_from_footer()`](Self::load_from_footer):
    ///
    /// ```text
    /// [pos (aligned)] [Manifest Data] [manifest_size: u64] [manifest_backoff: u64]
    /// ```
    pub fn save_as_footer<W>(&self, writer: W, pos: u64) -> std::io::Result<Range<u64>>
    where
        W: WriteAt + Clone,
    {
        let pos = align_up_u64(pos, PAGE_SIZE);
        let dump_size = self.compute_size();
        self.dump(SlicedFile::new(writer, pos..pos + dump_size))?;
        Ok(pos..pos + dump_size)
    }

    /// Returns the bincode configuration used for manifest serialization.
    fn binc_config() -> impl bincode::config::Config {
        bincode::config::standard().with_fixed_int_encoding()
    }
}

impl Dump for Manifest {
    /// Computes the storage size needed for this manifest.
    ///
    /// Returns the page-aligned size required to store this manifest, including
    /// the serialized data plus a 16-byte footer containing size metadata.
    ///
    /// # Returns
    ///
    /// The number of bytes needed, aligned to [`PAGE_SIZE`] boundaries.
    fn compute_size(&self) -> u64 {
        align_up_u64(self.estimate_encoded_size() + 16, PAGE_SIZE)
    }

    /// Serializes this manifest to storage with size metadata.
    ///
    /// This implementation writes the manifest data followed by a 16-byte footer
    /// containing the manifest size and total allocated size. This format enables
    /// loading via [`Load::load()`](crate::Load::load) and supports the sealed
    /// file format used by [`dump_and_seal()`](crate::Dump::dump_and_seal).
    ///
    /// # Storage Format
    ///
    /// ```text
    /// [Manifest Data] [padding_bytes] [manifest_size: u64] [total_size: u64]
    /// ```
    ///
    /// # Parameters
    ///
    /// * `writer` - Storage region to write the manifest to
    ///
    /// # Returns
    ///
    /// An empty manifest (manifests are self-describing and don't need metadata).
    fn dump<W>(&self, writer: amudai_io::SlicedFile<W>) -> std::io::Result<Manifest>
    where
        W: WriteAt + Clone,
    {
        let mut buf = self.to_vec()?;

        let manifest_size = buf.len() as u64;
        let min_dump_size = align_up_u64(manifest_size + 16, PAGE_SIZE);
        verify!(min_dump_size <= writer.slice_size());
        verify!(is_aligned_u64(writer.slice_size(), PAGE_SIZE));

        let buf_len = writer.slice_size() as usize;
        buf.resize(buf_len, 0);

        buf[buf_len - 16..buf_len - 8].copy_from_slice(&manifest_size.to_le_bytes());
        buf[buf_len - 8..buf_len].copy_from_slice(&writer.slice_size().to_le_bytes());

        writer.write_at(0, &buf)?;
        Ok(Default::default())
    }
}

impl Load for Manifest {
    /// Loads a manifest from storage that was written by [`Dump::dump()`](crate::Dump::dump).
    ///
    /// This implementation reads a manifest from storage by parsing the footer
    /// metadata to locate the actual manifest data within the storage region.
    /// It expects the storage format created by the [`Dump`](crate::Dump) implementation.
    ///
    /// # Parameters
    ///
    /// * `reader` - Storage region containing the serialized manifest
    /// * `_manifest` - Ignored (manifests are self-describing)
    ///
    /// # Returns
    ///
    /// The deserialized manifest.
    ///
    /// # Storage Format
    ///
    /// Expects the format written by [`Dump::dump()`](crate::Dump::dump):
    /// ```text
    /// [Manifest Data] [padding_bytes] [manifest_size: u64] [total_size: u64]
    /// ```
    fn load<R>(reader: amudai_io::SlicedFile<R>, _: &Manifest) -> std::io::Result<Manifest>
    where
        R: amudai_io::ReadAt + Clone,
    {
        let bytes = reader.read_all()?;
        verify!(bytes.len() >= 16);

        let size_bytes = &bytes[bytes.len() - 16..bytes.len() - 8];
        let manifest_size = u64::from_le_bytes(size_bytes.as_ref().try_into().unwrap()) as usize;
        verify!(manifest_size <= bytes.len());

        Manifest::from_slice(&bytes[..manifest_size])
    }
}

impl NestedManifest {
    /// Computes the exact encoded size of this nested manifest when serialized.
    ///
    /// This method provides the precise byte count needed to serialize this nested manifest,
    /// including both the contained manifest and the range information.
    ///
    /// # Returns
    ///
    /// The exact number of bytes needed to serialize this nested manifest.
    ///
    /// # Performance
    ///
    /// This method performs complete encoding simulation and should be used when
    /// exact sizes are required. For faster approximations, use [`estimate_encoded_size()`](Self::estimate_encoded_size).
    pub fn compute_encoded_size(&self) -> u64 {
        Tracker::compute_encoded_size(self)
    }

    /// Estimates the encoded size of this nested manifest when serialized.
    ///
    /// Provides a fast approximation by adding the estimated size of the contained
    /// manifest plus overhead for the range information (~16 bytes).
    ///
    /// # Returns
    ///
    /// An estimated number of bytes needed to serialize this nested manifest.
    ///
    /// # Algorithm
    ///
    /// The estimation includes:
    /// - The estimated size of the contained manifest
    /// - Fixed overhead for the range information (start and end positions)
    pub fn estimate_encoded_size(&self) -> u64 {
        self.manifest.estimate_encoded_size() + 16
    }
}

/// Internal utility for computing exact encoded sizes.
///
/// `Tracker` is a minimal `Write` implementation that counts bytes without
/// actually storing them. It's used to determine the precise encoded size
/// of manifests by performing a dry-run serialization.
#[derive(Default)]
struct Tracker(u64);

impl Tracker {
    /// Returns the total number of bytes tracked so far.
    fn size(&self) -> u64 {
        self.0
    }

    /// Computes the exact encoded size of a value by performing a dry-run encoding.
    fn compute_encoded_size<T>(value: &T) -> u64
    where
        T: bincode::Encode,
    {
        let mut t = Self::default();
        t.encode(value);
        t.size()
    }

    /// Encodes a value into this tracker, updating the byte count.
    fn encode<T>(&mut self, value: &T)
    where
        T: bincode::Encode,
    {
        bincode::encode_into_std_write(value, self, Manifest::binc_config())
            .expect("encode_into_std_write");
    }
}

impl std::io::Write for Tracker {
    /// Increments the byte counter by the length of the buffer.
    ///
    /// This implementation doesn't actually store the data, only tracks
    /// the number of bytes that would be written.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }

    /// No-op flush implementation.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use amudai_io::SharedIoBuffer;

    use crate::manifest::Manifest;

    #[test]
    fn test_manifest_size() {
        let mut m = Manifest::default();
        m.properties.insert("abcdefgh".into(), vec![0u8; 8]);
        m.properties.insert("12345678".into(), vec![1u8; 8]);
        assert_eq!(m.estimate_encoded_size(), m.compute_encoded_size());
        for i in 0..20 {
            m.child_map.insert(i.to_string(), Default::default());
        }
        assert_eq!(m.estimate_encoded_size(), m.compute_encoded_size());

        let m = m.into_nested(10..100);
        assert_eq!(m.estimate_encoded_size(), m.compute_encoded_size());

        let m = gen_manifest(5, 7);
        assert_eq!(m.compute_encoded_size(), m.estimate_encoded_size());
    }

    #[test]
    fn test_manifest_save_load() {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);
        assert_eq!(file.size().unwrap(), 0);

        file.write_at(0, &vec![10u8; 1024 * 4 * 5]).unwrap();

        let pos = file.size().unwrap();

        let manifest = gen_manifest(4, 7);
        assert_eq!(
            manifest.estimate_encoded_size(),
            manifest.compute_encoded_size()
        );

        manifest.save_as_footer(file.clone(), pos).unwrap();
        let (manifest2, data) = Manifest::load_from_footer(file.clone()).unwrap();
        assert_eq!(
            manifest.compute_encoded_size(),
            manifest2.compute_encoded_size()
        );
        assert_eq!(data.slice_size(), pos);
        assert!(manifest2.get_bytes("prop_0").is_ok());
        assert!(manifest2.child("child_0").is_ok());
        assert!(manifest2.child_at(0).is_ok());
    }

    fn gen_manifest(depth: usize, breadth: usize) -> Manifest {
        let mut manifest = Manifest::default();

        // Generate properties (around breadth count, +/- 2)
        let prop_count = breadth.saturating_sub(2) + (depth % 5); // Vary between breadth-2 to breadth+2
        for i in 0..prop_count {
            let key = format!("prop_{}", i);
            let value = vec![i as u8; 8 + (i % 16)]; // Varying sizes
            manifest.put_bytes(&key, value);
        }

        if depth > 0 {
            // Generate named children (child_map) - around breadth/2 count
            let named_child_count = (breadth / 2).max(1);
            for i in 0..named_child_count {
                let child_name = format!("child_{}", i);
                let child_manifest = gen_manifest(depth - 1, breadth);
                let range_start = i as u64 * 1000;
                let range_end = range_start + 800;
                let nested = child_manifest.into_nested(range_start..range_end);
                manifest.child_map.insert(child_name, nested);
            }

            // Generate list children (child_list) - around breadth/2 count
            let list_child_count = (breadth / 2).max(1);
            for i in 0..list_child_count {
                let child_manifest = gen_manifest(depth - 1, breadth);
                let range_start = (named_child_count + i) as u64 * 1000;
                let range_end = range_start + 600;
                let nested = child_manifest.into_nested(range_start..range_end);
                manifest.child_list.push(nested);
            }
        }

        manifest
    }
}
