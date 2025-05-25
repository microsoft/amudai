//! A writer designed for appending data to an artifact, such as a file, blob, or stream.
//! This writer is utilized in the process of creating and sealing data shards.

use std::ops::Range;

use amudai_format::defs::{common::DataRef, AMUDAI_FOOTER, AMUDAI_HEADER};
use amudai_io::{ReadAt, SealingWrite, StorageProfile};

/// A writer designed for appending data to an artifact, such as a file, blob,
/// or stream.
///
/// Optionally, the written data can be encapsulated in the Amudai file format,
/// which includes a well-defined header and footer.
///
/// This serves as a convenient wrapper for the `SealingWrite` abstraction found in
/// `amudai::io`. It allows for incremental writing of data to a destination,
/// tracking of the current write position, and retrieval of a URL representing
/// the artifact being written. The writing process is completed by invoking the
/// `seal()` method.
pub struct ArtifactWriter {
    inner: Box<dyn SealingWrite>,
    pos: u64,
    url: String,
    has_frame: bool,
}

impl ArtifactWriter {
    /// The default size of a single write buffer used when copying data from
    /// a large stream in multiple smaller chunks.
    pub const DEFAULT_IO_BLOCK_SIZE: usize = 4 * 1024 * 1024;

    /// Creates a new `ArtifactWriter` that writes to the provided `SealingWrite`
    /// and frames the data with the Amudai header and footer.
    ///
    /// This is the most common method for creating an `ArtifactWriter`.
    ///
    /// # Arguments
    ///
    /// * `writer`: The underlying `SealingWrite` to write to.
    /// * `url`: The URL representing the artifact being written.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the new `ArtifactWriter` on success, or an
    /// `std::io::Error` on failure.
    pub fn framed(
        mut writer: Box<dyn SealingWrite>,
        url: impl Into<String>,
    ) -> std::io::Result<ArtifactWriter> {
        writer.write_all(&AMUDAI_HEADER)?;
        Ok(ArtifactWriter {
            inner: writer,
            pos: AMUDAI_HEADER.len() as u64,
            url: url.into(),
            has_frame: true,
        })
    }

    /// Creates a new `ArtifactWriter` that writes to the provided `SealingWrite`
    /// **without** framing the data with the Amudai header and footer.
    ///
    /// # Arguments
    ///
    /// * `writer`: The underlying `SealingWrite` to write to.
    /// * `url`: The URL representing the artifact being written.
    pub fn bare(writer: Box<dyn SealingWrite>, url: impl Into<String>) -> ArtifactWriter {
        ArtifactWriter {
            inner: writer,
            pos: 0,
            url: url.into(),
            has_frame: false,
        }
    }

    /// Returns the URL of the artifact being written.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the current position of the writer.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Writes (appends) the entire buffer to the underlying
    /// storage artifact.
    pub fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf)?;
        self.pos += buf.len() as u64;
        Ok(())
    }

    /// Writes the encoded Protobuf message into the storage artifact according to the
    /// shard specification. The format includes the encoded message length, the encoded
    /// message itself, and the message checksum.
    ///
    /// # Returns
    ///
    /// On success, returns a `DataRef` representing the location and size of the written
    /// message enclosure within the storage artifact. The `DataRef` contains the URL of
    /// the storage artifact and the byte range where the message is stored.
    pub fn write_message<M>(&mut self, message: &M) -> std::io::Result<DataRef>
    where
        M: prost::Message,
    {
        let buf = Self::prepare_message_buffer(message);
        let start = self.position();
        self.write_all(&buf)?;
        let end = self.position();
        Ok(DataRef::new(self.url.clone(), start..end))
    }

    /// Copies data from a `ReadAtBlocking` source to the underlying storage artifact.
    ///
    /// This method reads data from the source in chunks and writes it to the
    /// artifact, updating the internal write position.
    ///
    /// # Arguments
    ///
    /// * `source`: The `ReadAtBlocking` source to read from.
    /// * `range`: The byte range within the source to copy.
    ///
    /// # Returns
    ///
    /// On success, returns a `DataRef` representing the location and size of the
    /// copied data within the storage artifact. The `DataRef` contains the URL of
    /// the storage artifact and the byte range where the data is stored.
    pub fn write_from_slice(
        &mut self,
        source: &dyn ReadAt,
        range: Range<u64>,
    ) -> std::io::Result<DataRef> {
        let block_size = self
            .storage_profile()
            .clamp_io_size(Self::DEFAULT_IO_BLOCK_SIZE) as u64;
        let target_start = self.position();
        let mut source_pos = range.start;
        let end_pos = range.end;
        while source_pos < end_pos {
            let next_pos = std::cmp::min(source_pos + block_size, end_pos);
            let buffer = source.read_at(source_pos..next_pos)?;
            if buffer.is_empty() {
                break;
            }
            self.write_all(&buffer)?;
            source_pos += buffer.len() as u64;
        }
        let target_end = self.position();
        Ok(DataRef::new(self.url.clone(), target_start..target_end))
    }

    /// Aligns the underlying storage artifact writer to the specified byte alignment.
    ///
    /// This method writes padding bytes to the underlying writer until the current
    /// position (as tracked by the `ArtifactWriter`) is a multiple of the given
    /// `alignment`.
    ///
    /// # Arguments
    ///
    /// * `alignment`: The byte alignment to achieve. Must be a power of 2.
    ///
    /// # Returns
    ///
    /// The number of padding bytes written.
    pub fn align(&mut self, alignment: usize) -> std::io::Result<usize> {
        let padding = amudai_io::io_extensions::get_padding(self.pos, alignment);
        self.write_all(&padding)?;
        Ok(padding.len())
    }

    /// Seals the writer, ensuring that all data is flushed and committed.
    ///
    /// # Returns
    ///
    /// On success, returns `Ok(DataRef)`, which references the entire artifact.
    pub fn seal(mut self) -> std::io::Result<DataRef> {
        if self.has_frame {
            self.write_all(&AMUDAI_FOOTER)?;
        }

        let end = self.pos;
        self.inner.seal()?;
        Ok(DataRef::new(self.url.clone(), 0..end))
    }

    /// Retrieves the storage profile of the underlying implementation.
    pub fn storage_profile(&self) -> StorageProfile {
        self.inner.storage_profile()
    }
}

impl ArtifactWriter {
    fn prepare_message_buffer<M>(message: &M) -> Vec<u8>
    where
        M: prost::Message,
    {
        let message_len = message.encoded_len();
        assert!(message_len < u32::MAX as usize);
        let buf_len = message_len + std::mem::size_of::<u32>() * 2;
        let mut buf = Vec::<u8>::with_capacity(buf_len);

        // Reserve space for the length (u32).
        buf.resize(std::mem::size_of::<u32>(), 0);
        // Encode the Protobuf message
        message
            .encode(&mut buf)
            .expect("encode message to vec must succeed");
        // Re-comute the actual message length and the checksum.
        let message_len = buf.len() - std::mem::size_of::<u32>();
        let checksum = amudai_format::checksum::compute(&buf[4..]);
        // Put the actual message length into the header.
        buf[0..4].copy_from_slice(&(message_len as u32).to_le_bytes());
        // Append the checksum
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use amudai_format::defs::shard::FieldDescriptor;

    use super::*;

    #[test]
    fn test_framed_artifact_writer() {
        let writer = Vec::<u8>::new();
        let url = "test_url".to_string();
        let mut artifact_writer = ArtifactWriter::framed(Box::new(writer), url.clone()).unwrap();

        assert_eq!(artifact_writer.url(), url);
        assert_eq!(artifact_writer.position(), AMUDAI_HEADER.len() as u64);
        artifact_writer.write_all(b"abcde").unwrap();
        let dref = artifact_writer.seal().unwrap();
        assert_eq!(dref.url, "test_url");
        assert_eq!(dref.range.unwrap().end, 21);
    }

    #[test]
    fn test_framed_artifact_writer_with_protobuf() {
        let writer = Vec::<u8>::new();
        let url = "test_url".to_string();
        let mut artifact_writer = ArtifactWriter::framed(Box::new(writer), url.clone()).unwrap();

        let mref = artifact_writer
            .write_message(&FieldDescriptor {
                position_count: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(mref.range.unwrap().end - mref.range.unwrap().start > 4);

        let dref = artifact_writer.seal().unwrap();
        assert_eq!(dref.url, "test_url");
        assert_eq!(dref.range.unwrap().end, mref.range.unwrap().end + 8);
    }
}
