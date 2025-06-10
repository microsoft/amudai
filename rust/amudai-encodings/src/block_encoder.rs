use crate::buffers_pool::BufferPoolRef;
use std::ops::Range;

/// Essential parameters for block encoding that influence the data
/// contained within the encoded block.
///
/// These parameters are supplied to the block encoder and are accessible
/// to the block decoder during the decoding process.
#[derive(Debug, Default, Clone)]
pub struct BlockEncodingParameters {
    /// Indicates whether block checksums are enabled.
    ///
    /// **Note**: The block encoder must comply with this setting.
    pub checksum: BlockChecksum,

    /// Indicates whether value presence (validity) information should be
    /// encoded as part of the block.
    ///
    /// **Note**: The block encoder must comply with this setting.
    pub presence: PresenceEncoding,
}

/// Represents the policy for block encoding as specified by the caller.
#[derive(Debug, Default, Clone)]
pub struct BlockEncodingPolicy {
    /// Essential parameters for block encoding.
    pub parameters: BlockEncodingParameters,

    /// The desired balance between speed and compression when encoding blocks.
    pub profile: BlockEncodingProfile,

    /// Optional constraints on the size of the resulting blocks.
    /// If provided, the encoder should consider these constraints and attempt to adhere
    /// to them on a best-effort basis (e.g., when determining its own preferred block size).
    /// However, the caller does not expect strict adherence to these constraints.
    /// If `None`, the encoder has the freedom to choose its own block size preferences.
    pub size_constraints: Option<BlockSizeConstraints>,
    //
    // Additional configuration options will be introduced as we progress...
    //
}

/// Indicates whether checksums are computed and suffixed to each block.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BlockChecksum {
    Disabled,
    #[default]
    Enabled,
}

impl BlockChecksum {
    pub fn is_enabled(&self) -> bool {
        matches!(self, BlockChecksum::Enabled)
    }
}

/// Indicates whether presence bits (non-null flags) should be encoded
/// as part of each block.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PresenceEncoding {
    Disabled,
    #[default]
    Enabled,
}

impl PresenceEncoding {
    pub fn is_enabled(&self) -> bool {
        matches!(self, PresenceEncoding::Enabled)
    }
}

/// Represents the desired trade-off for the selected block compression method.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BlockEncodingProfile {
    /// Plain encoding: a "memcpy"-style operation without any compression.
    Plain,
    /// Minimal compression: reduces CPU usage during encoding and introduces minimal
    /// overhead during decoding (compared to "memcpy"), at the cost of larger encoded size.
    /// Generally avoids cascading encodings, FSST, and general-purpose compressors.
    /// For strings, may utilize RLE (Run-Length Encoding).
    /// For numerics, may employ RLE or simple bit-packing.
    MinimalCompression,
    /// Balanced approach: aims for an efficient encoding cascade, potentially using all
    /// available methods for numeric encodings, and may employ LZ4 and FSST for text.
    /// Comparable to the current V3.
    #[default]
    Balanced,
    /// Focuses on achieving a high compression ratio, sacrificing ingestion speed and,
    /// to some extent, future decoding performance.
    HighCompression,
    /// Prioritizes the highest possible compression, regardless of other factors.
    /// This may involve:
    ///  - Requiring the caller to provide sufficiently large source blocks.
    ///  - Using Zstd for both text and numerics, or even as the final step in the cascade.
    MinimalSize,
}

/// This structure represents "soft" constraints on the preferred block sizes, in terms of
/// both logical value count and raw data size.
#[derive(Debug, Clone)]
pub struct BlockSizeConstraints {
    /// Preferred minimum and maximum size of the block, in terms of logical value count.
    pub value_count: Range<usize>,
    /// Preferred minimum and maximum *raw* data size of the block, in bytes.
    /// For fixed-size types, this is expected to be proportional to the `value_count`.
    pub data_size: Range<usize>,
}

/// Represents a generalized encoded block as a "byte slice," enabling the block encoder
/// to return buffers that are either owned, borrowed, or pooled.
pub enum EncodedBlock<'a> {
    Owned(Vec<u8>),
    Borrowed(&'a [u8]),
    Pooled(BufferPoolRef<'a>),
}

impl AsRef<[u8]> for EncodedBlock<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            EncodedBlock::Owned(vec) => vec.as_ref(),
            EncodedBlock::Borrowed(buf) => buf,
            EncodedBlock::Pooled(buf) => buf.as_ref(),
        }
    }
}

/// The primary `BlockEncoder` trait.
///
/// The `BlockEncoder` is designed to be "semi-stateful": generally, a single instance is used
/// throughout the stripe-level sequence of values for a specific field. However, it is possible
/// to recreate the block encoder at any point and start fresh, although this is an uncommon scenario.
/// Consequently, implementations may utilize internal scratch buffers, caches, and memoization of
/// decisions, among other techniques.
///
/// Source block data is provided to the block encoder in the form of an Arrow array. This array
/// contains the values (either fixed-size primitives or byte buffers) along with an optional `nulls`
/// bitmap, also known as a validity or presence bitmap. Depending on the `PresenceEncoding` setting
/// specified by the encoding parameters, the block encoder should either disregard this validity bitmap
/// or ensure that it is accurately represented in the encoded block.
///
/// The caller guarantees that the Arrow array will conform to one of the following data types and
/// shapes, based on the formal Amudai type of the field:
///
/// - `Int{N}Array` if the Amudai's `BasicType` is a signed `i8`, `i16`, `i32`, or `i64`
///   (where `N` is 8, 16, 32 or 64, respectively).
/// - `Int64Array` if the Amudai's `BasicType` is `DateTime` or the `KustoTimeSpan` extension type.
/// - `UInt{N}Array` if the Amudai's `BasicType` is an unsigned `u8`, `u16`, `u32`, or `u64`.
/// - `LargeBinaryArray` if the Amudai's `BasicType` is `Binary`, `String` or `KustoDynamic`
///   extension type.
/// - `FixedSizeBinaryArray` if the Amudai's `BasicType` is `FixedSizeBinary`, `Guid`, or the
///   `KustoDecimal` extension type.
///
/// # Main Requirement for Block Encoder
///
/// Regardless of the scheme or technique used by the encoder, the corresponding **block decoder**
/// must be capable of reconstructing the original values using three essential pieces of information:
/// the encoded block data, the `BasicTypeDescriptor` of Amudai's data type, and the
/// `BlockEncodingParameters` employed during encoding.
///
/// # Stripe Field Encoding Flow
///
/// This outlines the sequence of operations from the perspective of the block encoder:
/// ```ignore
/// let mut encoder = BlockEncoder::new(policy, basic_type);
/// loop {
///     // Parent field encoder: accumulate data in the staging buffer
///     // ...
///     let sample_size = encoder.sample_size();
///     // Parent field encoder: sample data from the staging buffer
///     // ...
///     let block_size = encoder.analyze_sample(sample);
///     while { // while there is enough data in the staging buffer
///         // Parent field encoder: dequeue source block by block_size
///         encoder.encode(block);
///     }
/// }
/// ```
///
/// # Notes
///
/// Although this trait is structured to facilitate its use through dynamic dispatch
/// (e.g., as `Box<dyn BlockEncoder>`), in most instances, the higher-level field encoder will
/// employ a more specific implementation of the `BlockEncoder` directly
/// (e.g., `PrimitiveBlockEncoder` or `BinaryBlockEncoder`).
pub trait BlockEncoder: Send + Sync + 'static {
    /// Returns the preferred sample size for this block encoder.
    ///
    /// The caller will attempt to provide a sample that conforms to this size in the `analyze_sample`
    /// call. If `sample_size()` returns `None`, it indicates that this encoder does not require
    /// sampling, and `analyze_sample` will not be invoked.
    fn sample_size(&self) -> Option<BlockSizeConstraints>;

    /// Analyzes a data sample to determine the most appropriate encoding strategy.
    ///
    /// The block encoder uses the provided sample to decide on an encoding strategy and store it
    /// internally. It will be applied in subsequent `encode` calls until the next invocation of
    /// `analyze_sample`.
    ///
    /// # Returns
    ///
    /// A description of the preferred raw block size for the encoder. The caller will make every
    /// effort to adhere to this constraint and will call `encode` with raw data that conforms to it.
    fn analyze_sample(
        &mut self,
        sample: &dyn arrow_array::Array,
    ) -> amudai_common::Result<BlockSizeConstraints>;

    /// Encodes a sequence of input values into a single byte block and returns it as a byte slice.
    ///
    /// # Arguments
    ///
    /// - `input` - A sequence of values, potentially accompanied by a validity (presence) bitmap,
    ///   that should be encoded in its entirety as a single block.
    ///
    /// # Returns
    ///
    /// A byte slice containing the encoded block, including the header, payload, and possibly
    /// a checksum suffix.
    fn encode(&mut self, input: &dyn arrow_array::Array) -> amudai_common::Result<EncodedBlock>;
}

/// Creation of the block encoder, separate from the main `BlockEncoder` trait
/// as it is not dyn-compatible.
pub trait BlockEncoderInit: BlockEncoder {
    /// Creates an instance of the `BlockEncoder`.
    ///
    /// # Arguments
    ///
    /// - `policy` - A block encoding policy.
    /// - `basic_type` - A formal Amudai type of the encoded values.
    fn new(
        policy: BlockEncodingPolicy,
        basic_type: amudai_format::schema::BasicTypeDescriptor,
    ) -> Self;
}
