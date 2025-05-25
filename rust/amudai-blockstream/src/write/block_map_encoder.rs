/// Encoder for the `BlockMap` - a data structure designed to efficiently navigate
/// the sequence of encoded blocks.
///
/// Conceptually, a block map comprises two lists of numbers:
///  - logical sizes: the number of values in each block.
///  - block sizes: the compressed size in bytes for each block.
///
/// We define `logical_offsets` as the prefix sum of logical sizes and `block_offsets`
/// as the prefix sum of block sizes. For a block with ordinal `i`, its logical range
/// is determined by `logical_offsets[i]..logical_offsets[i + 1]`, and its location
/// within the file is specified by `block_offsets[i]..block_offsets[i + 1]`.
///
/// The primary function of `BlockMap` is to translate a logical value position into
/// the corresponding block ordinal that contains the value, and to retrieve the
/// block's logical range and file offsets.
///
/// The encoded `BlockMap` is structured similarly to a B-tree and consists of the following components:
/// - **Segments**: An array of `BlockMapSegment` objects, each containing a slice of packed block locators.
/// - **Segment Locators**: List of segment logical offsets and segment entry offsets.
/// - **Footer**: Contains the total block count, segment count, and value count.
pub struct BlockMapEncoder {
    /// Size of a segment, expressed in blocks.
    segment_size: usize,
    /// Logical sizes of the blocks in the current segment.
    logical_sizes: Vec<u32>,
    /// Block sizes of the blocks in the current segment.
    block_sizes: Vec<u32>,
    /// Logical pos of the first block of the current segment.
    segment_first_logical_pos: u64,
    /// Storage offset of the first block of the current segment.
    segment_first_block_offset: u64,
    /// Total number of logical values.
    value_count: u64,
    /// Total number of blocks.
    block_count: u64,
    /// Segment logical offsets
    segment_logical_offsets: Vec<u64>,
    /// Segment entry offsets, relative to the block map start.
    segment_entry_offsets: Vec<u64>,
    /// In-progress `BlockMap` encoding buffer.
    encoded: Vec<u8>,
}

impl BlockMapEncoder {
    pub const DEFAULT_SEGMENT_BLOCKS: usize = 1024;

    pub fn new() -> BlockMapEncoder {
        BlockMapEncoder::with_parameters(Self::DEFAULT_SEGMENT_BLOCKS)
    }

    pub fn with_parameters(segment_size: usize) -> BlockMapEncoder {
        assert_ne!(segment_size, 0);
        BlockMapEncoder {
            segment_size,
            logical_sizes: Vec::with_capacity(segment_size),
            block_sizes: Vec::with_capacity(segment_size),
            segment_first_logical_pos: 0,
            segment_first_block_offset: 0,
            value_count: 0,
            block_count: 0,
            segment_logical_offsets: vec![0],
            segment_entry_offsets: vec![0],
            encoded: Vec::new(),
        }
    }

    /// Current number of block entries added to the encoder.
    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Adds the logical and storage size for the next block.
    pub fn add_block(&mut self, logical_size: u32, block_size: u32) {
        self.logical_sizes.push(logical_size);
        self.block_sizes.push(block_size);
        self.value_count += logical_size as u64;
        self.block_count += 1;
        if self.block_sizes.len() == self.segment_size {
            self.flush_segment();
        }
    }

    /// Completes the encoding and returns an encoded `BlockMap`.
    pub fn finish(mut self) -> Vec<u8> {
        self.flush_segment();

        assert_eq!(
            self.segment_logical_offsets.len(),
            self.segment_entry_offsets.len()
        );

        assert_eq!(self.encoded.len() % 8, 0);

        self.encoded
            .extend_from_slice(bytemuck::cast_slice::<_, u8>(&self.segment_logical_offsets));
        self.encoded
            .extend_from_slice(bytemuck::cast_slice::<_, u8>(&self.segment_entry_offsets));

        self.encoded
            .extend_from_slice(&self.value_count.to_le_bytes());
        self.encoded
            .extend_from_slice(&self.block_count.to_le_bytes());
        let segment_count = self.segment_entry_offsets.len() as u64 - 1;
        self.encoded.extend_from_slice(&segment_count.to_le_bytes());
        self.encoded
            .extend_from_slice(&(self.segment_size as u64).to_le_bytes());
        // Total size of the stored blocks.
        self.encoded
            .extend_from_slice(&self.segment_first_block_offset.to_le_bytes());

        assert_eq!(self.encoded.len() % 8, 0);

        self.encoded
    }
}

impl BlockMapEncoder {
    fn flush_segment(&mut self) {
        if self.block_sizes.is_empty() {
            return;
        }

        assert_eq!(self.encoded.len() % 8, 0);

        Self::encode_segment(
            &mut self.encoded,
            self.segment_first_logical_pos,
            self.segment_first_block_offset,
            &self.logical_sizes,
            &self.block_sizes,
        );

        assert_eq!(self.encoded.len() % 8, 0);

        self.segment_first_logical_pos += self.logical_sizes.iter().map(|&s| s as u64).sum::<u64>();
        self.segment_first_block_offset += self.block_sizes.iter().map(|&s| s as u64).sum::<u64>();

        self.segment_entry_offsets.push(self.encoded.len() as u64);
        self.segment_logical_offsets
            .push(self.segment_first_logical_pos);

        self.logical_sizes.clear();
        self.block_sizes.clear();
    }

    fn encode_segment(
        encoded: &mut Vec<u8>,
        first_logical_pos: u64,
        first_block_offset: u64,
        logical_sizes: &[u32],
        block_sizes: &[u32],
    ) {
        assert_eq!(logical_sizes.len(), block_sizes.len());
        if block_sizes.is_empty() {
            return;
        }
        encoded.extend_from_slice(&first_logical_pos.to_le_bytes());
        encoded.extend_from_slice(&first_block_offset.to_le_bytes());

        let min_logical_size = logical_sizes.iter().min().copied().unwrap();
        let max_logical_size = logical_sizes.iter().max().copied().unwrap();
        let logical_size_bits = Self::get_packed_width(min_logical_size, max_logical_size);
        let min_block_size = block_sizes.iter().min().copied().unwrap();
        let max_block_size = block_sizes.iter().max().copied().unwrap();
        let block_size_bits = Self::get_packed_width(min_block_size, max_block_size);

        encoded.extend_from_slice(&min_logical_size.to_le_bytes());
        encoded.extend_from_slice(&min_block_size.to_le_bytes());
        encoded.push(logical_size_bits);
        encoded.push(block_size_bits);
        encoded.extend_from_slice(&(logical_sizes.len() as u16).to_le_bytes());

        Self::pack(logical_sizes, min_logical_size, logical_size_bits, encoded);
        Self::align(encoded, 4);
        assert_eq!(encoded.len() % 4, 0);
        Self::pack(block_sizes, min_block_size, block_size_bits, encoded);
        Self::align(encoded, 8);
    }

    fn get_packed_width(min: u32, max: u32) -> u8 {
        let d = max - min;
        if d <= u8::MAX as u32 {
            8
        } else if d <= u16::MAX as u32 {
            16
        } else {
            32
        }
    }

    fn pack(values: &[u32], min: u32, bits: u8, buf: &mut Vec<u8>) {
        match bits {
            8 => {
                buf.extend(values.iter().map(|&v| (v - min) as u8));
            }
            16 => {
                buf.extend(
                    values
                        .iter()
                        .flat_map(|&v| ((v - min) as u16).to_le_bytes()),
                );
            }
            32 => {
                buf.extend(values.iter().flat_map(|&v| (v - min).to_le_bytes()));
            }
            _ => panic!("pack: unexpected width {bits}"),
        }
    }

    fn align(buf: &mut Vec<u8>, alignment: usize) {
        assert!(alignment.is_power_of_two());
        let new_len = (buf.len() + alignment - 1) & !(alignment - 1);
        buf.resize(new_len, 0);
    }
}

impl Default for BlockMapEncoder {
    fn default() -> Self {
        Self::new()
    }
}
