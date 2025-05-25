//! A reader for an artifact, providing methods to access its content.

use std::{ops::Range, sync::Arc};

use amudai_bytes::Bytes;
use amudai_common::{error::ErrorKind, verify_data, Result};
use amudai_io::{ReadAt, StorageProfile};
use amudai_objectstore::url::ObjectUrl;

/// A reader for an artifact, providing methods to access its content.
///
/// This struct wraps an implementation of the `ReadAt` trait representing the
/// stored shard artifact. It provides methods for reading specific sections
/// of the artifact, verifying headers and footers, and reading primitive types
/// and protobuf messages.
#[derive(Clone)]
pub struct ArtifactReader {
    inner: Arc<dyn ReadAt>,
    url: Arc<ObjectUrl>,
}

impl ArtifactReader {
    /// Creates a new `ArtifactReader` from a `ReadAt` implementation.
    pub fn new(inner: Arc<dyn ReadAt>, url: Arc<ObjectUrl>) -> ArtifactReader {
        ArtifactReader { inner, url }
    }

    /// Consumes the `ArtifactReader` and returns the inner `Arc<dyn ReadAt>`.
    pub fn into_inner(self) -> Arc<dyn ReadAt> {
        self.inner
    }

    /// Returns a reference to the inner `Arc<dyn ReadAt>`.
    pub fn inner(&self) -> &Arc<dyn ReadAt> {
        &self.inner
    }

    /// Returns the artifact URL.
    pub fn url(&self) -> &Arc<ObjectUrl> {
        &self.url
    }

    /// Retrieves the size of the artifact.
    ///
    /// This method delegates to the `size` method of the underlying `ReadAt`.
    pub fn size(&self) -> Result<u64> {
        Ok(self.inner.size()?)
    }

    /// Reads a range of bytes from the artifact.
    ///
    /// This method delegates to the `read_at` method of the underlying `ReadAt`.
    ///
    /// # Arguments
    ///
    /// * `range` - The range of bytes to read, convertible to `Range<u64>`.
    pub fn read_at(&self, range: impl Into<Range<u64>>) -> Result<Bytes> {
        let bytes = self.inner.read_at(range.into())?;
        Ok(bytes)
    }

    /// Verifies the artifact header.
    ///
    /// This method reads the first 8 bytes of the artifact and checks if they match
    /// the expected Amudai format header magic number and version.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Reading the header fails.
    /// * The header size is not 8 bytes.
    /// * The magic number does not match `defs::AMUDAI_MAGIC`.
    /// * The major version in the header is greater than `defs::AMUDAI_VERSION_MAJOR`.
    pub fn verify_header(&self) -> Result<()> {
        let header = self.read_at(0..8)?;
        Self::verify_header_or_footer(&header)
    }

    /// Verifies the artifact footer.
    ///
    /// This method reads the last 8 bytes of the artifact and checks if they match
    /// the expected Amudai format footer magic number and version.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Reading the footer fails.
    /// * The artifact size is less than twice the header size (minimum size for header
    ///   and footer).
    /// * The footer size is not 8 bytes.
    /// * The magic number does not match `defs::AMUDAI_MAGIC`.
    /// * The major version in the footer is greater than `defs::AMUDAI_VERSION_MAJOR`.
    pub fn verify_footer(&self) -> Result<()> {
        let size = self.size()?;
        verify_data!(
            "minimal artifact size",
            size >= amudai_format::defs::AMUDAI_HEADER.len() as u64 * 2
        );
        let footer = self.read_at(size - 8..size)?;
        Self::verify_header_or_footer(&footer)
    }

    /// Reads a `u32` value from the artifact at the given position.
    ///
    /// Reads 4 bytes starting from `pos` and interprets them as a little-endian `u32`.
    pub fn read_u32(&self, pos: u64) -> Result<u32> {
        let bytes = self.inner.read_at(pos..pos + 4)?;
        verify_data!("read_u32: len", bytes.len() == 4);
        Ok(u32::from_le_bytes(
            bytes.as_ref().try_into().expect("u32 bytes"),
        ))
    }

    /// Reads a `u64` value from the artifact at the given position.
    ///
    /// Reads 8 bytes starting from `pos` and interprets them as a little-endian `u64`.
    pub fn read_u64(&self, pos: u64) -> Result<u64> {
        let bytes = self.inner.read_at(pos..pos + 8)?;
        verify_data!("read_u64: len", bytes.len() == 8);
        Ok(u64::from_le_bytes(
            bytes.as_ref().try_into().expect("u64 bytes"),
        ))
    }

    /// Reads and decodes a protobuf message from the artifact.
    ///
    /// Reads bytes from the specified `range`, validates the checksum, and then
    /// decodes them into a protobuf message of type `M`.
    ///
    /// # Arguments
    ///
    /// * `range` - The range of bytes containing the protobuf message, convertible
    ///   to `Range<u64>`. This range should cover a 4-byte length prefix, the encoded
    ///   message itself, and a 4-byte checksum.
    ///
    ///
    /// # Type Parameters
    ///
    /// * `M` - The protobuf message type to decode into. Must implement `prost::Message`
    ///   and `Default`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Reading bytes from the specified range fails.
    /// * Checksum validation fails.
    /// * Protobuf decoding fails.
    pub fn read_message<M>(&self, range: impl Into<Range<u64>>) -> Result<M>
    where
        M: prost::Message + Default,
    {
        let bytes = self.read_at(range)?;
        let message_bytes = amudai_format::checksum::validate_message(&bytes)?;
        let message = M::decode(message_bytes).map_err(|e| ErrorKind::InvalidFormat {
            element: "protobuf message".to_string(),
            message: e.to_string(),
        })?;
        Ok(message)
    }

    /// Returns the storage profile of the underlying reader.
    pub fn storage_profile(&self) -> StorageProfile {
        self.inner.storage_profile()
    }
}

impl ArtifactReader {
    fn verify_header_or_footer(header: &[u8]) -> Result<()> {
        verify_data!("header/footer size", header.len() == 8);
        let magic = &header[..4];
        verify_data!(
            "Amudai format magic",
            magic == amudai_format::defs::AMUDAI_MAGIC
        );
        let version_major = header[7];
        if version_major > amudai_format::defs::AMUDAI_VERSION_MAJOR {
            return Err(ErrorKind::InvalidFormat {
                element: "header version".to_string(),
                message: format!(
                    "Amudai shard major version {version_major} > {} (max supported)",
                    amudai_format::defs::AMUDAI_VERSION_MAJOR
                ),
            }
            .into());
        }
        Ok(())
    }
}
