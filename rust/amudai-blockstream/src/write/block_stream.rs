//! BlockStream format encoder.

use std::sync::Arc;

use amudai_common::{Result, error::Error, verify_arg};
use amudai_io::{IoStream, ReadAt, io_extensions::AlignWrite};

use super::block_map_encoder::BlockMapEncoder;

/// `BlockStreamEncoder` is responsible for writing a sequence of encoded blocks,
/// each representing a specific aspect (e.g., Data, Presence, Offsets) of a value
/// sequence within a stripe field.
///
/// Background: the value sequence for a stripe field is divided into blocks, with each
/// block containing a range of logical values. These blocks can vary in size, typically
/// containing a few hundred to a few thousand values. Each block is encoded independently
/// using either a general-purpose or a specialized encoding algorithm. The resulting
/// encoded block buffer, along with the logical size of the block, is then passed to the
/// `BlockStreamEncoder`.
///
/// During the creation of the block stream, `BlockStreamEncoder` manages the bookkeeping of logical
/// block sizes to construct a `BlockMap`.
pub struct BlockStreamEncoder {
    writer: Box<dyn IoStream>,
    block_map: BlockMapEncoder,
}

impl BlockStreamEncoder {
    pub fn new(writer: Box<dyn IoStream>) -> BlockStreamEncoder {
        assert_eq!(writer.current_size(), 0);
        BlockStreamEncoder {
            writer,
            block_map: BlockMapEncoder::new(),
        }
    }

    pub fn push_block(&mut self, data: &[u8], logical_size: usize) -> Result<()> {
        verify_arg!(logical_size, logical_size < u32::MAX as usize);
        verify_arg!(logical_size, logical_size != 0);
        verify_arg!(data.len(), data.len() < u32::MAX as usize);
        verify_arg!(data.len(), !data.is_empty());
        verify_arg!(
            "block count",
            self.block_map.block_count() < u32::MAX as u64
        );

        self.writer
            .write_all(data)
            .map_err(|e| Error::io("push_block", e))?;

        self.block_map
            .add_block(logical_size as u32, data.len() as u32);
        Ok(())
    }

    pub fn finish(mut self) -> Result<PreparedBlockStream> {
        let block_count = self.block_map.block_count();
        self.writer.align(16)?;
        let data_size = self.writer.current_size();

        let block_map = self.block_map.finish();
        self.writer.write_all(&block_map)?;
        let block_map_size = self.writer.current_size() - data_size;

        let block_stream = self
            .writer
            .into_read_at()
            .map_err(|e| Error::io("finish.block_stream", e))?;
        Ok(PreparedBlockStream {
            block_count,
            data_size,
            block_map_size,
            block_stream,
        })
    }
}

#[must_use]
pub struct PreparedBlockStream {
    pub block_count: u64,
    pub data_size: u64,
    pub block_map_size: u64,
    pub block_stream: Arc<dyn ReadAt>,
}

impl PreparedBlockStream {
    /// Size of the data part of the block stream.
    /// The range `0..data_size` is the data part (sequence of encoded blocks).
    pub fn data_size(&self) -> u64 {
        self.data_size
    }

    /// Size of the block map part of the block stream.
    /// The range `data_size..data_size + block_map_size` is the block map.
    pub fn block_map_size(&self) -> u64 {
        self.block_map_size
    }

    /// Total size of the block stream.
    /// The range `0..total_size` in `self.block_stream` is the entire buffer.
    pub fn total_size(&self) -> u64 {
        self.data_size + self.block_map_size
    }
}

impl std::fmt::Debug for PreparedBlockStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedBlockStream")
            .field("block_count", &self.block_count)
            .field("data_size", &self.data_size)
            .field("block_map_size", &self.block_map_size)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use amudai_io::IoStream;
    use amudai_io_impl::temp_file_store;

    use super::BlockStreamEncoder;

    #[test]
    fn test_empty_block_stream_encoding() {
        let writer = new_temp_writable();
        let encoder = BlockStreamEncoder::new(writer);
        let res = encoder.finish().unwrap();
        assert_eq!(res.block_count, 0);
        assert_eq!(res.total_size(), 56);
    }

    #[test]
    fn test_single_block_stream_encoding() {
        let writer = new_temp_writable();
        let mut encoder = BlockStreamEncoder::new(writer);
        encoder.push_block(b"abcdefg", 111).unwrap();
        let res = encoder.finish().unwrap();
        assert_eq!(res.block_count, 1);
        assert!(res.total_size() < 200);

        let buf = res.block_stream.read_at(0..res.total_size()).unwrap();
        assert_eq!(&buf[0..7], b"abcdefg");
    }

    #[test]
    fn test_many_block_stream_encoding() {
        let writer = new_temp_writable();
        let mut encoder = BlockStreamEncoder::new(writer);
        let mut data_size = 0;
        for i in 0..100 {
            let len = i % 5 + 1;
            encoder.push_block(&b"abcdefg"[..len], i % 7 + 1).unwrap();
            data_size += len;
        }
        let res = encoder.finish().unwrap();
        assert_eq!(res.block_count, 100);
        assert!(res.block_map_size < 400);
        assert_eq!(res.data_size, (data_size.div_ceil(16) * 16) as u64);
        let buf = res.block_stream.read_at(0..res.total_size()).unwrap();
        assert_eq!(&buf[0..10], b"aababcabcd");
    }

    fn new_temp_writable() -> Box<dyn IoStream> {
        let temp_store = temp_file_store::create_in_memory(100000000).unwrap();
        temp_store.allocate_stream(None).unwrap()
    }
}
